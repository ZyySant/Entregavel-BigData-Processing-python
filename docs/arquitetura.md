# Arquitetura — Pipeline ShopBrasil

## Visão geral

```
                    ┌───────────────────────┐
                    │   data/raw/incoming/   │
                    │  (Parquet 01..07 dez,  │
                    │   CSV 08 dez)          │
                    └───────────┬────────────┘
                                │
                    FileSensor (aguarda o dia)
                                │
                                ▼
                    ┌───────────────────────┐
                    │  ingestao.py (Bronze)  │  detecta formato, unifica
                    │                        │  schema, adiciona metadados
                    └───────────┬────────────┘  (_source, _ingestion_ts, _data_ref)
                                │
                                ▼
                    ┌───────────────────────┐
                    │ transformacao.py       │  dedup + DataQualityFramework
                    │ (Silver)               │  (quality/checks.py)
                    └─────┬─────────────┬────┘
                          │             │
                   válidos│             │quarentena
                          ▼             ▼
              data/datalake/silver   data/datalake/quarentena
                          │
                          ▼
                    ┌───────────────────────┐
                    │  agregacao.py (Gold)   │  join c/ produtos.parquet
                    │                        │
                    └─────┬─────────────┬────┘
                          │             │
           faturamento_por_estado   faturamento_mensal
                          │             │
                          ▼             ▼
                    ┌───────────────────────┐
                    │      quality_gate       │  lê o relatório da Silver +
                    │   (task Airflow)         │  confere a Gold direto
                    └───────────┬────────────┘
                                │
                                ▼
                       notificar_sucesso
```

## Por que 8 dias de incoming, e por que um deles é CSV

Os 7 dias de dezembro/2023 vêm do dataset de produção da aula 7
(`datasets/aula_07/producao/incoming/`) — é o feed principal, já em Parquet,
já limpo.

O dia `2023-12-08` é o CSV de `datasets/aula_06/dados_sujos/vendas_problemas.csv`
— reaproveitado aqui como se fosse o dia em que o parceiro logístico mandou o
extrato com problema (mesma situação descrita na narrativa da aula 6: campos
nulos, valores negativos, duplicatas). Isso resolve dois requisitos do projeto
final ao mesmo tempo:

1. **Ingestão multi-formato de verdade** — o Bronze precisa ler Parquet e CSV
   e normalizar os dois pro mesmo schema antes de seguir.
2. **Quarentena com conteúdo real** — se a gente só usasse os dados limpos da
   aula 7, os checks de qualidade passariam 100% sempre e a quarentena nunca
   teria nada dentro, o que não prova nada na demo. Com o dia 08 no meio,
   dá pra mostrar a quarentena pegando registro sujo de verdade.

Pra demo, vale a pena rodar o dia 08 na frente dos outros — é onde a
quarentena e o quality gate realmente têm o que fazer.

## Decisões de design

| Decisão | Por quê |
|---|---|
| 3 scripts separados (`ingestao.py`, `transformacao.py`, `agregacao.py`) em vez de um script único | Mais fácil de testar cada camada isolada, e cada um vira uma task própria na DAG — se a Silver falhar, não precisa reprocessar a Bronze |
| `DataQualityFramework` em `quality/checks.py`, sem lib pronta | Exigência do projeto final (proibido Great Expectations/Soda) |
| Quality gate roda em duas partes: dentro da Silver (gera o relatório) e como task separada no Airflow (lê o relatório + confere a Gold) | A Silver sabe os detalhes de cada check; o Airflow só precisa saber se pode ou não liberar a notificação — não faz sentido subir Spark de novo só pra isso |
| Imagem customizada do Airflow (`docker/airflow.Dockerfile`) | A imagem oficial `apache/airflow` não tem Java nem `spark-submit`, então o `SparkSubmitOperator` quebra. Isso é um problema conhecido (está até no troubleshooting da aula 7), só que sem solução lá — resolvemos instalando JRE + pyspark num diretório fixo |
| `partitionOverwriteMode=dynamic` em todas as escritas | Idempotência — reprocessar um dia não duplica nem afeta os outros dias |
| `coalesce(1)` antes de cada escrita | No bind mount do Docker Desktop (Windows), várias tasks tentando criar o mesmo diretório temporário em paralelo dá erro de `Mkdirs failed`. Com o volume de dados daqui, 1 arquivo por partição não pesa em performance |
| `spark.hadoop.fs.permissions.umask-mode=000` em toda SparkSession | O `SparkSubmitOperator` roda o driver *dentro* do container do Airflow (deploy-mode client, uid do usuário `airflow`), mas os executors rodam no cluster Spark (uid do usuário `spark`). Sem essa config, quem cria um diretório primeiro tranca o outro uid pra fora dele — o erro aparece como "Mkdirs failed", mas é permissão, não race condition |
| `FileSensor` com timeout curto (60s) | O Airflow recria uma dag_run "de hoje" toda vez que a DAG é despausada (comportamento padrão do `catchup=False` com `schedule` em cron) — como não tem dado de verdade pra hoje, o sensor ia esperar 5 min à toa antes. Com 60s isso passa rápido e não atrapalha o resto |

## Fluxo de dados por camada

- **Bronze** (`data/datalake/bronze/vendas/`): raw + metadados de
  rastreabilidade, particionado por `_data_ref`. Sem transformação de negócio.
- **Silver** (`data/datalake/silver/vendas/`): deduplicado por `order_id`,
  só os registros que passaram nas regras de quarentena. Particionado por
  `data_ref`.
- **Quarentena** (`data/datalake/quarentena/vendas/`): registros que falharam
  em pelo menos uma regra, com a coluna `quarantine_reasons` explicando o
  motivo.
- **Gold** (`data/datalake/gold/`): duas tabelas —
  `faturamento_por_estado` (visão geográfica, estado + categoria) e
  `faturamento_mensal` (visão temporal, mês + categoria — a granularidade
  fica pronta pra quando o pipeline acumular vários meses de execução real).

## Rodando fora do Docker (dev/debug rápido)

```bash
pip install -r requirements.txt

python spark_jobs/ingestao.py --data-ref 2023-12-01 --input-path data/raw --output-path data/datalake
python spark_jobs/transformacao.py --data-ref 2023-12-01 --output-path data/datalake
python spark_jobs/agregacao.py --data-ref 2023-12-01 --input-path data/raw --output-path data/datalake
```
