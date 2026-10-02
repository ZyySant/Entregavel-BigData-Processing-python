"""
Gold — metricas de negocio para o dashboard executivo.

Le a Silver do dia, enriquece com produtos, e gera duas tabelas:
  - faturamento_por_estado: faturamento, pedidos e ticket medio por UF/categoria
  - faturamento_mensal: mesma coisa na granularidade mes/categoria

Uso:
    spark-submit agregacao.py --data-ref 2023-12-01
"""

import argparse
import logging
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, date_format, lit
from pyspark.sql.functions import sum as spark_sum


def configurar_logging(log_level: str) -> logging.Logger:
    logger = logging.getLogger("agregacao")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "[%(asctime)s] %(levelname)s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(handler)
    return logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gold - agregacoes de negocio ShopBrasil")
    parser.add_argument("--data-ref", required=True)
    parser.add_argument("--input-path", default="data/raw", help="Onde estao produtos/clientes")
    parser.add_argument("--output-path", default="data/datalake")
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
        .appName(f"ShopBrasil-Gold-{data_ref}")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def etapa_gold(spark: SparkSession, input_path: str, output_path: str, data_ref: str, logger: logging.Logger) -> int:
    logger.info(f"[GOLD] Iniciando agregacoes para data_ref={data_ref}")

    caminho_silver = f"{output_path}/silver/vendas/data_ref={data_ref}"
    df_silver = spark.read.parquet(caminho_silver)

    df_produtos = spark.read.parquet(f"{input_path}/produtos.parquet")

    df_enriquecido = df_silver.join(
        df_produtos.select("product_id", "category", "subcategory"), on="product_id", how="left"
    )

    df_por_estado = df_enriquecido.groupBy("shipping_state", "category").agg(
        spark_sum("total_amount").alias("faturamento_total"),
        count("order_id").alias("total_pedidos"),
        avg("total_amount").alias("ticket_medio"),
    ).withColumn("data_ref", lit(data_ref))

    caminho_estado = f"{output_path}/gold/faturamento_por_estado"
    df_por_estado.coalesce(1).write.mode("overwrite").partitionBy("data_ref").parquet(caminho_estado)
    contagem_estado = df_por_estado.count()
    logger.info(f"[GOLD] faturamento_por_estado: {contagem_estado:,} linhas em {caminho_estado}")

    df_mensal = (
        df_enriquecido
        .withColumn("mes_referencia", date_format(col("order_date"), "yyyy-MM"))
        .groupBy("mes_referencia", "category")
        .agg(
            spark_sum("total_amount").alias("faturamento_total"),
            count("order_id").alias("total_pedidos"),
            avg("total_amount").alias("ticket_medio"),
        )
        .withColumn("data_ref", lit(data_ref))
    )

    caminho_mensal = f"{output_path}/gold/faturamento_mensal"
    df_mensal.coalesce(1).write.mode("overwrite").partitionBy("data_ref").parquet(caminho_mensal)
    contagem_mensal = df_mensal.count()
    logger.info(f"[GOLD] faturamento_mensal: {contagem_mensal:,} linhas em {caminho_mensal}")

    total_gold = contagem_estado + contagem_mensal
    logger.info(f"[GOLD] OK -- {total_gold:,} linhas geradas nas duas tabelas")
    return total_gold


def main():
    args = parse_args()
    logger = configurar_logging(args.log_level)
    spark = None

    try:
        spark = criar_spark_session(args.data_ref)
        logger.info(f"SparkSession criada -- versao {spark.version}")
        etapa_gold(spark, args.input_path, args.output_path, args.data_ref, logger)
        logger.info("[GOLD] Pipeline concluido com sucesso")
    except Exception as e:
        logger.error(f"Erro inesperado na agregacao: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
