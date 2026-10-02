#!/bin/bash
# Roda isso UMA VEZ, no dia da apresentacao, antes de abrir a UI do Airflow.
#
# O botao padrao "Trigger DAG" sempre usa a data de hoje -- esse script copia
# o dia com dado sujo (2023-12-08) pra uma pasta com a data atual, assim o
# sensor acha arquivo de verdade e a demo fica 100% clicavel, sem precisar
# digitar comando na hora.
set -e

# UTC, nao hora local -- o Airflow roda no fuso UTC por padrao, e a pasta
# precisa bater com a data que ele vai usar no Trigger DAG
HOJE=$(date -u +%Y-%m-%d)
ORIGEM="data/raw/incoming/2023-12-08/vendas.csv"
DESTINO="data/raw/incoming/$HOJE"

if [ ! -f "$ORIGEM" ]; then
  echo "Erro: rode este script a partir da raiz do projeto (onde fica data/raw/)."
  exit 1
fi

mkdir -p "$DESTINO"
cp "$ORIGEM" "$DESTINO/vendas.csv"

echo "Pronto -- data/raw/incoming/$HOJE/ criado com o dado sujo do dia 08/12."
echo "Agora é só ativar a DAG e clicar no Trigger DAG padrao na UI do Airflow."
