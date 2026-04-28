# Proposta de Projeto Final — Previsão de Preços de Passagens Aéreas com Big Data

## 1. Identificação

| Campo | Informação |
|-------|-----------|
| **Disciplina** | Big Data |
| **Projeto** | Previsão de Preços de Passagens Aéreas |
| **Entrega Canvas** | 10/05/2026 (slides + notebooks) |
| **Apresentação** | 11/05, 12/05, 25/05 ou 26/05/2026 |

### Integrantes

| # | Nome | RA |
|---|------|-----|
| 1 | Kaique Medeiros Govani | 210170 |
| 2 | Felipe Augusto Almeida Mariano | 210045 |
| 3 | _[Nome do Integrante 3]_ | _[RA]_ |
| 4 | _[Nome do Integrante 4]_ | _[RA]_ |

---

## 2. Introdução

O setor de aviação comercial é caracterizado por uma precificação altamente dinâmica, onde tarifas variam em função de múltiplos fatores como antecedência da compra, sazonalidade, aeroporto de origem/destino, número de escalas e classe tarifária. Compreender e prever esses preços é um problema relevante tanto para consumidores quanto para a indústria.

Este projeto propõe a construção de um pipeline completo de Big Data para análise exploratória e modelagem preditiva de preços de passagens aéreas. Utilizaremos um dataset massivo (31 GB, 82 milhões de registros) coletado da plataforma Expedia, aplicando ferramentas do ecossistema Apache (HDFS, Spark) em um ambiente containerizado com Docker.

### Objetivos

- Construir um ambiente distribuído de Big Data simulando um cluster com Docker.
- Realizar análise exploratória de dados (EDA) em escala, identificando padrões de precificação.
- Treinar e avaliar modelos de regressão (Árvore de Decisão, Regressão Linear e Redes Neurais) para prever o valor total da tarifa (`totalFare`).
- Comparar o desempenho dos modelos utilizando métricas padronizadas.

---

## 3. Dataset Escolhido

**Nome:** Flight Prices  
**Fonte:** [Kaggle — dilwong/flightprices](https://www.kaggle.com/datasets/dilwong/flightprices)  
**Origem dos dados:** Expedia (plataforma de reservas de viagens)  
**Período:** Abril a Outubro de 2022  
**Cobertura:** 16 principais aeroportos dos Estados Unidos  

### Dimensões

| Métrica | Valor |
|---------|-------|
| Linhas | ~82 milhões |
| Colunas | 27 |
| Tamanho | ~31 GB (CSV) |
| Formato | CSV monolítico |

O dataset **excede significativamente** o requisito mínimo de 1 GB, justificando plenamente o uso de ferramentas de Big Data para seu processamento.

### Variável Alvo

- **`totalFare`** (float64) — preço total da passagem incluindo taxas. Tarefa principal: **regressão**.

### Descrição das Colunas

| # | Coluna | Tipo | Descrição |
|---|--------|------|-----------|
| 1 | legId | string | Identificador único do trecho |
| 2 | searchDate | string | Data em que a busca foi realizada |
| 3 | flightDate | string | Data do voo |
| 4 | startingAirport | string | Aeroporto de origem (código IATA) |
| 5 | destinationAirport | string | Aeroporto de destino (código IATA) |
| 6 | fareBasisCode | string | Código da base tarifária |
| 7 | travelDuration | string | Duração total da viagem (formato textual) |
| 8 | elapsedDays | int64 | Dias entre a busca e o voo |
| 9 | isBasicEconomy | bool | Indica se é tarifa econômica básica |
| 10 | isRefundable | bool | Indica se a passagem é reembolsável |
| 11 | isNonStop | bool | Indica se o voo é direto (sem escalas) |
| 12 | baseFare | float64 | Tarifa base sem taxas |
| 13 | **totalFare** | **float64** | **Preço total (VARIÁVEL ALVO)** |
| 14 | seatsRemaining | int64 | Assentos restantes |
| 15 | totalTravelDistance | float64 | Distância total percorrida (milhas) |
| 16 | segmentsDepartureTimeEpochSeconds | string | Horários de partida dos segmentos (pipe-delimited) |
| 17 | segmentsDepartureTimeRaw | string | Horários de partida em formato legível |
| 18 | segmentsArrivalTimeEpochSeconds | string | Horários de chegada dos segmentos (pipe-delimited) |
| 19 | segmentsArrivalTimeRaw | string | Horários de chegada em formato legível |
| 20 | segmentsArrivalAirportCode | string | Códigos dos aeroportos de chegada por segmento |
| 21 | segmentsDepartureAirportCode | string | Códigos dos aeroportos de partida por segmento |
| 22 | segmentsAirlineName | string | Companhias aéreas por segmento |
| 23 | segmentsAirlineCode | string | Códigos das companhias por segmento |
| 24 | segmentsEquipmentDescription | string | Descrição das aeronaves (muitos valores ausentes) |
| 25 | segmentsDurationInSeconds | string | Duração de cada segmento em segundos |
| 26 | segmentsDistance | string | Distância de cada segmento (contém valores 'None') |
| 27 | segmentsCabinCode | string | Classe da cabine por segmento |

### Problemas Conhecidos

- `segmentsEquipmentDescription`: alta proporção de valores ausentes.
- `segmentsDistance`: contém strings `'None'` em vez de valores numéricos.
- `totalTravelDistance`: valores `NaN` em registros com dados de segmento incompletos.
- Datas (`searchDate`, `flightDate`): armazenadas como strings, necessitam parsing.
- Colunas de segmento: valores delimitados por pipe (`||`) para voos com múltiplas escalas.

### Justificativa da Escolha

- Volume massivo adequado para Big Data (31 GB, 82M linhas).
- Problema de regressão bem definido com variável alvo clara.
- Riqueza de features para engenharia de atributos.
- Relevância prática e aplicabilidade no mundo real.
- Dados reais de mercado (Expedia), não sintéticos.

---

## 4. Parte 1 — Ambiente de Big Data (1 ponto)

### Arquitetura Proposta

O ambiente será orquestrado via **Docker Compose**, simulando um cluster distribuído com os seguintes componentes:

| Componente | Container(s) | Função |
|------------|-------------|--------|
| HDFS NameNode | 1 | Gerenciamento de metadados do sistema de arquivos distribuído |
| HDFS DataNode | 2+ | Armazenamento distribuído dos blocos de dados |
| Spark Master | 1 | Coordenação do cluster de processamento |
| Spark Workers | 2+ | Execução distribuída das tarefas de processamento |
| Jupyter Notebook | 1 | Interface interativa para análise e modelagem |

### Fluxo de Dados

1. O dataset CSV será carregado no HDFS via comandos `hdfs dfs -put`.
2. O Spark acessará os dados diretamente do HDFS para processamento distribuído.
3. O Jupyter Notebook se conectará ao Spark Master via PySpark para análise interativa.

### Rede

Todos os containers compartilharão uma rede Docker interna, permitindo comunicação direta entre os serviços. Volumes persistentes serão configurados para o HDFS e para os notebooks.

---

## 5. Parte 2 — Análise de Dados (5 pontos)

### 5.1 Pré-processamento

Dado o volume do dataset e os problemas identificados, as seguintes etapas de pré-processamento serão realizadas com PySpark:

**Tratamento de datas:**
- Converter `searchDate` e `flightDate` de string para tipo date.
- Calcular `elapsedDays` como diferença entre `flightDate` e `searchDate` (validação).

**Tratamento de colunas de segmento (pipe-delimited):**
- Separar valores delimitados por `||` em arrays.
- Extrair contagem de segmentos (número de escalas).
- Calcular duração total a partir de `segmentsDurationInSeconds`.

**Valores ausentes e inconsistentes:**
- Remover ou imputar `NaN` em `totalTravelDistance`.
- Substituir strings `'None'` em `segmentsDistance` por valores nulos.
- Avaliar descarte da coluna `segmentsEquipmentDescription` (alta taxa de ausência).

**Conversões de tipo:**
- Converter `travelDuration` de formato textual para minutos (numérico).
- Converter campos booleanos para inteiros (0/1).

### 5.2 Engenharia de Atributos

- **Dias até o voo:** diferença entre data do voo e data da busca.
- **Número de segmentos:** contagem de escalas derivada das colunas pipe-delimited.
- **Duração total em minutos:** parsing da coluna `travelDuration`.
- **Hora de partida:** extração da hora do dia a partir dos timestamps de segmento.
- **Dia da semana do voo:** derivado de `flightDate`.
- **Mês do voo:** sazonalidade mensal.
- **Rota:** combinação origem-destino como feature categórica.
- **Distância por segmento:** média da distância dividida pelo número de segmentos.

### 5.3 Análise Exploratória (EDA)

- Distribuição da variável alvo (`totalFare`) — histograma e estatísticas descritivas.
- Correlação entre features numéricas e `totalFare`.
- Análise de preço por rota (aeroportos mais caros/baratos).
- Variação de preço por antecedência de compra (`elapsedDays`).
- Impacto de escalas vs. voos diretos no preço.
- Sazonalidade: variação de preços ao longo dos meses.
- Distribuição de classes tarifárias e seu efeito no preço.

---

## 6. Parte 3 — Modelos Preditivos (4 pontos)

### Estratégia Geral

- Divisão dos dados em treino (80%) e teste (20%) com amostragem estratificada.
- Normalização/padronização de features numéricas quando necessário.
- Encoding de variáveis categóricas (one-hot ou label encoding conforme o modelo).

### 6.1 Regressão Linear

**Justificativa:** Modelo baseline que estabelece uma referência de desempenho. Permite interpretar a contribuição linear de cada feature no preço final. Adequado para identificar relações lineares entre distância, antecedência e preço.

**Abordagem:** Treinamento com PySpark MLlib (`LinearRegression`). Regularização (Lasso/Ridge) para lidar com multicolinearidade entre features correlacionadas.

### 6.2 Árvore de Decisão

**Justificativa:** Captura relações não-lineares e interações entre features sem necessidade de transformações. Altamente interpretável — permite visualizar regras de decisão (ex: "se voo direto E antecedência > 30 dias, preço tende a ser menor"). Robusto a outliers.

**Abordagem:** Treinamento com PySpark MLlib (`DecisionTreeRegressor`). Tuning de hiperparâmetros: profundidade máxima, número mínimo de amostras por folha. Possível extensão para Random Forest se o tempo permitir.

### 6.3 Redes Neurais

**Justificativa:** Capacidade de modelar relações complexas e não-lineares de alta dimensionalidade. Potencial para capturar padrões sutis que modelos mais simples não detectam, especialmente em um dataset com 82 milhões de amostras.

**Abordagem:** Rede neural feedforward (MLP) com camadas densas. Treinamento com TensorFlow/Keras ou PyTorch em amostra representativa dos dados. Arquitetura: camadas ocultas com ativação ReLU, dropout para regularização, otimizador Adam.

### 6.4 Métricas de Avaliação

| Métrica | Fórmula | Interpretação |
|---------|---------|---------------|
| RMSE | √(Σ(ŷ-y)²/n) | Erro médio na mesma unidade do preço (USD) |
| MAE | Σ\|ŷ-y\|/n | Erro absoluto médio — menos sensível a outliers |
| R² | 1 - SS_res/SS_tot | Proporção da variância explicada pelo modelo |

Os três modelos serão comparados lado a lado utilizando as mesmas partições de dados para garantir uma avaliação justa.

---

## 7. Cronograma

| Período | Atividade | Responsável |
|---------|-----------|-------------|
| 28/04 – 30/04 | Configuração do ambiente Docker (HDFS + Spark + Jupyter) | Equipe |
| 01/05 – 03/05 | Ingestão dos dados no HDFS e pré-processamento inicial | Equipe |
| 03/05 – 05/05 | Análise exploratória (EDA) e engenharia de atributos | Equipe |
| 05/05 – 07/05 | Treinamento e avaliação dos modelos preditivos | Equipe |
| 07/05 – 09/05 | Comparação de resultados e ajustes finais | Equipe |
| 08/05 – 09/05 | Criação dos slides da apresentação (PowerPoint) | Equipe |
| 09/05 – 10/05 | Documentação final e revisão dos slides | Equipe |
| **10/05** | **Entrega via Canvas (notebooks + slides)** | **Equipe** |
| 11–12/05 ou 25–26/05 | Apresentação final com slides | Equipe |

---

## 8. Ferramentas e Tecnologias

| Categoria | Ferramenta | Uso no Projeto |
|-----------|-----------|----------------|
| Containerização | Docker / Docker Compose | Orquestração do ambiente de cluster |
| Armazenamento | Apache HDFS | Sistema de arquivos distribuído para o dataset |
| Processamento | Apache Spark / PySpark | Processamento distribuído e MLlib |
| Interface | Jupyter Notebook | Análise interativa e visualizações |
| Linguagem | Python 3.x | Linguagem principal do projeto |
| ML Clássico | scikit-learn / Spark MLlib | Regressão Linear e Árvore de Decisão |
| Deep Learning | TensorFlow/Keras ou PyTorch | Redes Neurais |
| Visualização | Matplotlib / Seaborn | Gráficos e visualizações da EDA |
| Versionamento | Git / GitHub | Controle de versão do código |
| Apresentação | Microsoft PowerPoint | Criação dos slides para apresentação final |

---

## 9. Referências

1. Wong, D. (2022). *Flight Prices Dataset*. Kaggle. Disponível em: https://www.kaggle.com/datasets/dilwong/flightprices
2. Apache Spark Documentation. Disponível em: https://spark.apache.org/docs/latest/
3. Apache Hadoop HDFS Documentation. Disponível em: https://hadoop.apache.org/docs/current/
4. Docker Documentation. Disponível em: https://docs.docker.com/
5. Géron, A. (2022). *Hands-On Machine Learning with Scikit-Learn, Keras, and TensorFlow*. 3rd Edition. O'Reilly Media.
6. Zaharia, M. et al. (2016). *Apache Spark: A Unified Engine for Big Data Processing*. Communications of the ACM, 59(11), 56-65.
