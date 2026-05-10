# flight-price-big-data

Ambiente de Big Data para exploracao e modelagem preditiva de precos de passagens aereas com HDFS, Spark e Jupyter.

## Requisitos

- Docker Desktop em execucao
- Permissao para acessar o daemon do Docker no Windows
- Pelo menos 8 GB de RAM livres para os containers

## Subindo o ambiente

Execute na raiz do projeto:

```powershell
docker compose up -d --build
```

Servicos esperados:

- HDFS NameNode: `http://localhost:9870`
- Spark Master: `http://localhost:8080`
- Spark Worker 1: `http://localhost:8081`
- Spark Worker 2: `http://localhost:8082`
- Jupyter Lab: `http://localhost:8888/lab?token=bigdata2026`

Para verificar o estado:

```powershell
docker compose ps
```

## Abrindo o notebook

Use o Jupyter do container, nao o Python local.

1. Abra `http://localhost:8888/lab?token=bigdata2026`
2. Navegue ate `/work`
3. Abra o notebook `flight_price_sql_mllib_starter.ipynb`
4. Execute as celulas no kernel `Python 3 (ipykernel)`

Se voce tentou executar o notebook antes de subir o CSV no HDFS, faca:

```powershell
Kernel > Restart Kernel
```

e rode as celulas novamente desde o topo. Caso contrario, o Spark pode ficar preso em um caminho antigo do dataset.

## Preparando o dataset

Fluxo recomendado depois do pull:

1. manter o CSV original em `data/itineraries.csv`
2. converter para Parquet diretamente no HDFS
3. usar o notebook lendo Parquet por padrao

Conversao recomendada:

```powershell
make convert-local
```

Isso evita o upload manual do CSV para o HDFS e grava:

- `hdfs://namenode:9000/data/itineraries.parquet`

Se quiser validar a saida:

```powershell
make verify
```

## Carregando o CSV no HDFS

Esse passo continua disponivel, mas agora e secundario.

Com os containers saudaveis:

```powershell
docker compose exec namenode /scripts/upload-to-hdfs.sh
```

O notebook tenta ler nesta ordem:

- `hdfs://namenode:9000/data/itineraries.parquet`
- `hdfs://namenode:9000/data/itineraries.csv`
- fallback: `/data/itineraries.csv`

## Problemas comuns

### `ModuleNotFoundError: No module named 'pyspark'`

Isso acontece quando o notebook esta rodando no Python local da maquina ou quando a imagem do Jupyter nao foi rebuildada.

Corrija assim:

```powershell
docker compose up -d --build
```

Depois abra o notebook pelo Jupyter do container.

### Spark lento mesmo com mais RAM no Docker

Este projeto fica pesado na secao de modelagem porque o gargalo nao e so memoria:

- ha encoding de categoricas de alta cardinalidade, como `route`
- varios modelos sao treinados na mesma sessao
- a MLP proxy consome bastante CPU

O compose atual ja sobe 2 workers Spark e o notebook pede executores maiores.
Se voce alterou recursos do Docker Desktop ou do WSL, recrie os containers:

```powershell
docker compose down
docker compose up -d --build
```

No Spark UI, o esperado e ver:

- 2 workers
- 2 executors no app do notebook
- memoria de executor perto de `3g`, nao `1024 MB`

### `docker compose` para ou Jupyter nao sobe

Confira os logs:

```powershell
docker compose logs --tail 100
```

Se precisar recriar tudo:

```powershell
docker compose down
docker compose up -d --build
```
