"""
Bronze — ingestao das vendas diarias da ShopBrasil.

Le o incoming do dia (pode vir em Parquet ou CSV, dependendo da origem -- o
parceiro logistico historicamente manda CSV, o feed principal do e-commerce
manda Parquet) e grava tudo em um schema unico na camada Bronze, com metadados
de rastreabilidade. Nao faz limpeza aqui, isso e trabalho da Silver.

Uso:
    spark-submit ingestao.py --data-ref 2023-12-01
    spark-submit ingestao.py --data-ref 2023-12-08 --input-path /data/raw
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit


def configurar_logging(log_level: str) -> logging.Logger:
    logger = logging.getLogger("ingestao")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "[%(asctime)s] %(levelname)s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(handler)
    return logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bronze - ingestao de vendas ShopBrasil")
    parser.add_argument("--data-ref", required=True, help="Data de referencia (YYYY-MM-DD)")
    parser.add_argument("--input-path", default="data/raw", help="Diretorio base dos dados de entrada")
    parser.add_argument("--output-path", default="data/datalake", help="Diretorio base do datalake")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    try:
        datetime.strptime(args.data_ref, "%Y-%m-%d")
    except ValueError:
        parser.error(f"--data-ref deve estar no formato YYYY-MM-DD, recebido '{args.data_ref}'")

    return args


def criar_spark_session(data_ref: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(f"ShopBrasil-Bronze-{data_ref}")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


COLUNAS_ESPERADAS = [
    "order_id", "customer_id", "product_id", "quantity", "unit_price",
    "total_amount", "order_date", "payment_method", "shipping_city",
    "shipping_state", "status", "partner_source",
]


def ler_incoming(spark: SparkSession, input_path: str, data_ref: str, logger: logging.Logger):
    pasta_dia = Path(input_path) / "incoming" / data_ref
    caminho_parquet = pasta_dia / "vendas.parquet"
    caminho_csv = pasta_dia / "vendas.csv"

    if caminho_parquet.exists():
        logger.info(f"[BRONZE] Formato detectado: parquet ({caminho_parquet})")
        df = spark.read.parquet(str(caminho_parquet))
        origem = "vendas_parquet_v1"
    elif caminho_csv.exists():
        logger.info(f"[BRONZE] Formato detectado: csv ({caminho_csv})")
        df = spark.read.option("header", True).option("inferSchema", True).csv(str(caminho_csv))
        origem = "vendas_csv_parceiro"
    else:
        raise FileNotFoundError(f"Nenhum arquivo de vendas encontrado em {pasta_dia} (esperado .parquet ou .csv)")

    for coluna in COLUNAS_ESPERADAS:
        if coluna not in df.columns:
            df = df.withColumn(coluna, lit(None))
    df = df.select(*COLUNAS_ESPERADAS)

    df = (
        df.withColumn("quantity", col("quantity").cast("int"))
          .withColumn("unit_price", col("unit_price").cast("double"))
          .withColumn("total_amount", col("total_amount").cast("double"))
          .withColumn("order_date", col("order_date").cast("timestamp"))
    )

    return df, origem


def etapa_bronze(spark: SparkSession, input_path: str, output_path: str, data_ref: str, logger: logging.Logger) -> int:
    logger.info(f"[BRONZE] Iniciando ingestao para data_ref={data_ref}")

    df_raw, origem = ler_incoming(spark, input_path, data_ref, logger)
    contagem = df_raw.count()
    logger.info(f"[BRONZE] Registros lidos: {contagem:,}")

    df_bronze = (
        df_raw
        .withColumn("_data_ref", lit(data_ref))
        .withColumn("_ingestion_ts", current_timestamp())
        .withColumn("_source", lit(origem))
    )

    caminho_bronze = f"{output_path}/bronze/vendas"
    df_bronze.coalesce(1).write.mode("overwrite").partitionBy("_data_ref").parquet(caminho_bronze)

    logger.info(f"[BRONZE] Escrita concluida em {caminho_bronze}")
    logger.info(f"[BRONZE] OK -- {contagem:,} registros ingeridos (origem: {origem})")
    return contagem


def main():
    args = parse_args()
    logger = configurar_logging(args.log_level)
    spark = None

    try:
        spark = criar_spark_session(args.data_ref)
        logger.info(f"SparkSession criada -- versao {spark.version}")
        etapa_bronze(spark, args.input_path, args.output_path, args.data_ref, logger)
        logger.info("[BRONZE] Pipeline concluido com sucesso")
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erro inesperado na ingestao: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
