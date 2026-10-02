"""
DAG: pipeline de vendas ShopBrasil (DataFlow Analytics)

Fluxo: sensor -> bronze -> silver -> gold -> quality gate -> notificacao

O quality gate roda DEPOIS do Gold e le o relatorio que a Silver gravou em
data/datalake/quality_reports/<data_ref>.json, mais uma checagem direta na
Gold (sem negativos, sem estado nulo, volume minimo). Se qualquer coisa
falhar, a task explode e o on_failure_callback avisa a equipe.

Schedule: diario as 06:00 UTC
"""

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor

VOLUME_MINIMO_GOLD = 1


def alerta_falha(context):
    task = context["task_instance"]
    print(f"[ALERTA] Task '{task.task_id}' falhou na DAG '{context['dag'].dag_id}'")
    print(f"  Data de execucao: {context['ds']}")
    print(f"  Erro: {context.get('exception', 'N/A')}")


default_args = {
    "owner": "dataflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alerta_falha,
}


def quality_gate(**context):
    """Le o relatorio da Silver + confere a Gold antes de liberar pra notificacao."""
    data_ref = context["ds"]
    falhas = []

    caminho_relatorio = f"/opt/airflow/data/datalake/quality_reports/{data_ref}.json"
    try:
        with open(caminho_relatorio, encoding="utf-8") as f:
            relatorio = json.load(f)
    except FileNotFoundError:
        raise Exception(f"Relatorio de qualidade nao encontrado em {caminho_relatorio}")

    print(f"[QUALITY] Relatorio da Silver: {relatorio['checks_passed']}/{relatorio['checks_total']} checks OK "
          f"(score={relatorio['overall_score']:.1%})")

    if not relatorio["gate_passed"]:
        falhas.append("quality gate da Silver falhou (algum check critical nao passou)")

    import glob
    import pandas as pd

    caminho_gold = f"/opt/airflow/data/datalake/gold/faturamento_por_estado/data_ref={data_ref}"
    arquivos_gold = glob.glob(f"{caminho_gold}/*.parquet")
    if not arquivos_gold:
        falhas.append(f"nenhum arquivo Gold encontrado em {caminho_gold}")
    else:
        df_gold = pd.concat([pd.read_parquet(a) for a in arquivos_gold], ignore_index=True)

        if len(df_gold) < VOLUME_MINIMO_GOLD:
            falhas.append(f"volume da Gold abaixo do minimo: {len(df_gold)} < {VOLUME_MINIMO_GOLD}")

        negativos = df_gold[df_gold["faturamento_total"] < 0]
        if len(negativos) > 0:
            falhas.append(f"{len(negativos)} linhas com faturamento_total negativo na Gold")

        estados_nulos = df_gold["shipping_state"].isnull().sum()
        if estados_nulos > 0:
            falhas.append(f"{estados_nulos} linhas com shipping_state nulo na Gold")

    if falhas:
        raise Exception(f"Quality gate FALHOU para {data_ref}: " + "; ".join(falhas))

    print(f"[QUALITY] Quality gate OK para {data_ref}")
    return {"status": "PASSED", "data_ref": data_ref}


def notificar_sucesso(**context):
    data_ref = context["ds"]
    print("=" * 50)
    print("PIPELINE SHOPBRASIL CONCLUIDO COM SUCESSO")
    print("=" * 50)
    print(f"  Data: {data_ref}")
    print("  Fluxo: incoming -> bronze -> silver -> gold")
    print("  Quality gate: PASSED")
    print("=" * 50)


with DAG(
    dag_id="shopbrasil_pipeline_vendas",
    default_args=default_args,
    description="Pipeline E2E ShopBrasil: sensor -> bronze -> silver -> gold -> quality gate -> notificacao",
    schedule="0 6 * * *",
    start_date=datetime(2023, 12, 1),
    catchup=False,
    tags=["dataflow", "shopbrasil", "producao"],
) as dag:

    sensor_arquivo = FileSensor(
        task_id="aguardar_incoming",
        filepath="/opt/airflow/data/raw/incoming/{{ ds }}",
        poke_interval=15,
        timeout=60,
        mode="poke",
        soft_fail=True,
    )

    job_bronze = SparkSubmitOperator(
        task_id="bronze_ingestao",
        application="/opt/spark-jobs/ingestao.py",
        conn_id="spark_default",
        application_args=["--data-ref", "{{ ds }}", "--input-path", "/opt/airflow/data/raw",
                           "--output-path", "/opt/airflow/data/datalake"],
        name="shopbrasil_bronze_{{ ds_nodash }}",
        verbose=True,
    )

    job_silver = SparkSubmitOperator(
        task_id="silver_transformacao",
        application="/opt/spark-jobs/transformacao.py",
        conn_id="spark_default",
        application_args=["--data-ref", "{{ ds }}", "--output-path", "/opt/airflow/data/datalake"],
        name="shopbrasil_silver_{{ ds_nodash }}",
        verbose=True,
    )

    job_gold = SparkSubmitOperator(
        task_id="gold_agregacao",
        application="/opt/spark-jobs/agregacao.py",
        conn_id="spark_default",
        application_args=["--data-ref", "{{ ds }}", "--input-path", "/opt/airflow/data/raw",
                           "--output-path", "/opt/airflow/data/datalake"],
        name="shopbrasil_gold_{{ ds_nodash }}",
        verbose=True,
    )

    task_quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=quality_gate,
    )

    task_notificar = PythonOperator(
        task_id="notificar_sucesso",
        python_callable=notificar_sucesso,
    )

    task_log = BashOperator(
        task_id="log_execucao",
        bash_command='echo "[{{ ds }}] Pipeline ShopBrasil concluido as $(date +%H:%M:%S). Proxima execucao: {{ next_ds }}"',
    )

    sensor_arquivo >> job_bronze >> job_silver >> job_gold >> task_quality_gate >> task_notificar >> task_log
