"""
Silver — limpeza, deduplicacao e quality gate das vendas.

Le a partição do dia na Bronze, roda o DataQualityFramework (quality/checks.py)
e separa em validos/quarentena. So os validos seguem para a Silver; a
quarentena fica gravada a parte para o time de dados investigar depois.

Uso:
    spark-submit transformacao.py --data-ref 2023-12-01
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from quality.checks import DataQualityFramework


def configurar_logging(log_level: str) -> logging.Logger:
    logger = logging.getLogger("transformacao")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "[%(asctime)s] %(levelname)s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(handler)
    return logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Silver - limpeza e quality gate de vendas ShopBrasil")
    parser.add_argument("--data-ref", required=True)
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
        .appName(f"ShopBrasil-Silver-{data_ref}")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


REGRAS_QUARENTENA = {
    "campos_obrigatorios_nulos": (
        "order_id IS NOT NULL AND customer_id IS NOT NULL AND product_id IS NOT NULL "
        "AND total_amount IS NOT NULL AND order_date IS NOT NULL"
    ),
    "quantidade_invalida": "quantity > 0",
    "valor_negativo": "total_amount >= 0",
    "status_desconhecido": "status IN ('pending', 'shipped', 'delivered', 'cancelled')",
    "data_futura": "order_date <= current_timestamp()",
    "estado_nulo": "shipping_state IS NOT NULL",
}


def etapa_silver(spark: SparkSession, output_path: str, data_ref: str, logger: logging.Logger) -> int:
    logger.info(f"[SILVER] Iniciando limpeza para data_ref={data_ref}")

    caminho_bronze = f"{output_path}/bronze/vendas/_data_ref={data_ref}"
    df_bronze = spark.read.parquet(caminho_bronze)
    contagem_entrada = df_bronze.count()
    logger.info(f"[SILVER] Registros da Bronze: {contagem_entrada:,}")

    df_dedup = df_bronze.dropDuplicates(["order_id"])
    duplicatas_removidas = contagem_entrada - df_dedup.count()
    logger.info(f"[SILVER] Duplicatas exatas removidas: {duplicatas_removidas:,}")

    dq = DataQualityFramework(spark)
    dq.run_all_checks(df_dedup, {
        "completeness": {
            "columns": ["order_id", "customer_id", "product_id", "total_amount", "order_date"],
            "threshold": 0.90,
            "severity": "critical",
        },
        "uniqueness": {"key_columns": ["order_id"], "severity": "critical"},
        "validity": {
            "rules": {
                "quantidade_positiva": "quantity > 0",
                "valor_nao_negativo": "total_amount >= 0",
                "status_valido": "status IN ('pending', 'shipped', 'delivered', 'cancelled')",
                "data_nao_futura": "order_date <= current_timestamp()",
                "estado_preenchido": "shipping_state IS NOT NULL",
            },
            "threshold": 0.90,
            "severity": "warning",
        },
    })

    relatorio = dq.generate_report()
    logger.info(
        f"[SILVER] Quality report: {relatorio['checks_passed']}/{relatorio['checks_total']} checks OK, "
        f"score={relatorio['overall_score']:.1%}, gate={'PASSED' if relatorio['gate_passed'] else 'FAILED'}"
    )

    pasta_reports = Path(output_path) / "quality_reports"
    pasta_reports.mkdir(parents=True, exist_ok=True)
    dq.salvar_relatorio(str(pasta_reports / f"{data_ref}.json"))

    df_validos, df_quarentena = dq.quarantine(df_dedup, REGRAS_QUARENTENA)

    contagem_validos = df_validos.count()
    contagem_quarentena = df_quarentena.count()
    logger.info(f"[SILVER] Validos: {contagem_validos:,} | Quarentena: {contagem_quarentena:,}")

    df_silver = df_validos.withColumn("data_ref", lit(data_ref))
    caminho_silver = f"{output_path}/silver/vendas"
    df_silver.coalesce(1).write.mode("overwrite").partitionBy("data_ref").parquet(caminho_silver)
    logger.info(f"[SILVER] Escrita concluida em {caminho_silver}")

    if contagem_quarentena > 0:
        df_quarentena_out = df_quarentena.withColumn("data_ref", lit(data_ref))
        caminho_quarentena = f"{output_path}/quarentena/vendas"
        df_quarentena_out.coalesce(1).write.mode("overwrite").partitionBy("data_ref").parquet(caminho_quarentena)
        logger.info(f"[SILVER] {contagem_quarentena:,} registros enviados para quarentena em {caminho_quarentena}")

    logger.info(f"[SILVER] OK -- {contagem_validos:,} registros validos")
    return contagem_validos


def main():
    args = parse_args()
    logger = configurar_logging(args.log_level)
    spark = None

    try:
        spark = criar_spark_session(args.data_ref)
        logger.info(f"SparkSession criada -- versao {spark.version}")
        etapa_silver(spark, args.output_path, args.data_ref, logger)
        logger.info("[SILVER] Pipeline concluido com sucesso")
    except Exception as e:
        logger.error(f"Erro inesperado na transformacao: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
