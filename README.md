# ShopBrasil — Pipeline de Vendas

Projeto final da disciplina Big Data Processing (MBA Engenharia de Dados,
Mackenzie). Pipeline de dados end-to-end pra ShopBrasil (cliente fictício da
DataFlow Analytics, a empresa usada como narrativa do curso): ingestão
multi-formato, arquitetura Medallion (Bronze/Silver/Gold), qualidade de dados
customizada com quarentena, e orquestração via Airflow — tudo containerizado.

## Integrantes

| Nome | RA |
|---|---|
| | |
| | |
| | |

## Como rodar

### Pré-requisitos

- [Docker Desktop](https://docs.docker.com/get-docker/) instalado (Windows/Mac) ou Docker Engine + plugin Compose (Linux) — já vem com Docker Compose v2
- Git
- 8GB RAM livres, 4 cores, internet na primeira subida (baixa as imagens do Spark e do Airflow)
- Portas `7077`, `8080` e `8081` livres na máquina

### Passo a passo

```bash
git clone https://github.com/ZyySant/Entregavel-BigData-Processing.git
cd Entregavel-BigData-Processing
docker compose up -d --build
```

A primeira subida demora uns 5–10 minutos (builda a imagem do Airflow com
Java + PySpark, e baixa a imagem oficial do Spark). Acompanhe com
`docker compose ps` até `spark-master` e `airflow-webserver` aparecerem como
`healthy`.

- Spark Master UI: http://localhost:8080
- Airflow UI: http://localhost:8081 (usuário `admin`, senha `admin`)

Antes de ativar a DAG, rode o script que prepara os dados do dia:

```bash
./scripts/preparar_demo.sh
```

O botão padrão de "Trigger DAG" na UI do Airflow sempre usa a data de hoje —
esse script copia o dia com dado problemático (2023-12-08) pra uma pasta com
a data atual, então o clique padrão já acha arquivo de verdade e mostra o
quality gate e a quarentena funcionando, sem precisar digitar nenhum comando
na hora. Depois é só ativar a DAG `shopbrasil_pipeline_vendas` (toggle à
esquerda) e clicar no play.

Se quiser rodar uma das datas originais (2023-12-01 a 2023-12-08) em vez da
data de hoje, use o comando abaixo (a UI não deixa escolher a data no
disparo padrão):

```bash
docker exec shopbrasil-airflow-scheduler airflow dags trigger shopbrasil_pipeline_vendas --exec-date 2023-12-08T06:00:00+00:00
```

Resultado final em `data/datalake/gold/`.

### Plano B (se o Airflow travar na hora da demo)

```bash
./scripts/rodar_pipeline_local.sh 2023-12-01 2023-12-08
```

Roda o pipeline inteiro via `spark-submit` direto, sem depender da UI do
Airflow — só precisa do cluster Spark de pé.

## Arquitetura

Ingestão lê o `incoming/` do dia (Parquet ou CSV, dependendo da origem) →
Bronze (raw + metadados) → Silver (dedup + `DataQualityFramework`, separa
válidos de quarentena) → Gold (duas tabelas: faturamento por estado e
faturamento mensal) → quality gate → notificação.

Diagrama detalhado e as decisões de design (por que 3 scripts separados, por
que a imagem customizada do Airflow, etc.) estão em
[`docs/arquitetura.md`](docs/arquitetura.md).

## Qualidade de dados

`quality/checks.py` implementa um framework próprio (sem Great Expectations
nem Soda, conforme pedido na especificação) com:

- **Completude** — campos obrigatórios (`order_id`, `customer_id`,
  `product_id`, `total_amount`, `order_date`)
- **Unicidade** — `order_id` não pode se repetir
- **Validade de domínio** — quantidade positiva, valor não-negativo, status
  dentro do conjunto esperado

Quem falha em alguma regra vai pra `data/datalake/quarentena/vendas/` com o
motivo registrado em `quarantine_reasons`, em vez de simplesmente ser
descartado.

## Estrutura do repositório

```
.
├── docker-compose.yml
├── docker/airflow.Dockerfile
├── dags/pipeline.py
├── spark_jobs/
│   ├── ingestao.py        # Bronze
│   ├── transformacao.py   # Silver + quality gate
│   └── agregacao.py       # Gold
├── quality/checks.py      # DataQualityFramework
├── scripts/
│   ├── airflow_init.sh
│   ├── rodar_pipeline_local.sh   # plano B
│   └── preparar_demo.sh          # copia o dia sujo pra data de hoje, antes da demo
├── data/raw/               # dados de entrada (incoming + dimensões)
└── docs/arquitetura.md
```

## Troubleshooting rápido

| Sintoma | Causa provável | Solução |
|---|---|---|
| `docker compose up` falha com "port is already allocated" | Porta 7077, 8080 ou 8081 já está em uso por outro programa | Feche o que estiver usando a porta, ou pare outros containers com `docker ps` + `docker stop <id>` |
| `SparkSubmitOperator` falha com "spark-submit: command not found" | Imagem do Airflow não foi rebuildada | `docker compose build airflow-init airflow-webserver airflow-scheduler` |
| Clicou em "Trigger DAG" (botão padrão) e todas as tasks ficaram `skipped` | O botão padrão usa a data de hoje, e não existe pasta `incoming/<hoje>` com dado | Rode `./scripts/preparar_demo.sh` antes de disparar, ou use o comando com `--exec-date` pra uma data de 2023-12 |
| DAG não aparece na UI | Scheduler ainda não fez o parse, ou erro de import | `docker compose logs airflow-scheduler` / `docker exec shopbrasil-airflow-scheduler airflow dags list-import-errors` |
| `FileNotFoundError` no job Bronze | Data errada ou pasta `incoming/<data>` não existe | Confira `data/raw/incoming/` — só existe de 2023-12-01 a 2023-12-08 |
| Quality gate falha sempre | Relatório da Silver não foi gerado | Rode a Silver antes (`silver_transformacao` precisa ter sucesso antes do `quality_gate`) |
| `Mkdirs failed to create file:...` num dos jobs Spark | Permissão — o driver (dentro do container do Airflow) e os executors (no cluster Spark) rodam com uids diferentes, e um tranca o outro fora do diretório que criou primeiro | Já corrigido via `spark.hadoop.fs.permissions.umask-mode=000` nas SparkSessions. Se acontecer mesmo assim, apague `data/datalake/` e rode de novo |
| Uma execução aparece sozinha pra "hoje" assim que a DAG é ativada, e fica presa no sensor por um tempo | Comportamento padrão do Airflow: `catchup=False` + `schedule` cron dispara a última janela automaticamente ao despausar | Normal, não quebra nada — o sensor falha suave (`soft_fail`) em ~1 min e as tasks seguintes são puladas. Ignore essa execução e foque na que você disparou manualmente |
