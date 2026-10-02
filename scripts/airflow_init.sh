#!/bin/bash
# Roda uma vez no container airflow-init: banco, usuario admin e a conexao
# que o SparkSubmitOperator usa pra falar com o cluster.
set -e

airflow db init

airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User --role Admin \
  --email admin@shopbrasil.local

airflow connections delete spark_default 2>/dev/null || true
airflow connections add spark_default \
  --conn-type spark \
  --conn-host spark://spark-master \
  --conn-port 7077

echo "Airflow inicializado -- usuario admin/admin, conexao spark_default configurada."
