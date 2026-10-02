#!/bin/bash
# Zera o ambiente: derruba os containers, apaga o historico do Airflow e os
# dados gerados pelo pipeline (bronze/silver/gold/quarentena), e sobe tudo
# de novo do zero. Os dados de ENTRADA (data/raw/) nao sao tocados.
#
# Uso:
#   ./scripts/resetar_ambiente.sh
set -e

cd "$(dirname "$0")/.."

echo "=== Derrubando containers e volumes ==="
docker compose down -v

echo "=== Limpando dados gerados pelo pipeline ==="
rm -rf data/datalake

echo "=== Subindo ambiente limpo ==="
docker compose up -d

echo
echo "=== Pronto ==="
echo "Airflow: http://localhost:8081 (admin/admin)"
echo "Spark:   http://localhost:8080"
echo
echo "Espere os servicos ficarem 'healthy' (docker compose ps) antes de"
echo "ativar a DAG. Pra demo, rode ./scripts/preparar_demo.sh em seguida."
