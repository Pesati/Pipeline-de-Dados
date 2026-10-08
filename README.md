# 🚀 Arquitetura Data Lakehouse em Tempo Real | Streaming & Batch

Este projeto implementa um pipeline de dados ponta a ponta utilizando a **Arquitetura Medallion** (Bronze, Silver, Gold). O sistema realiza a ingestão de eventos transacionais simulados em tempo real, processa os dados garantindo a conformidade com a LGPD e disponibiliza métricas agregadas para o consumo de Business Intelligence.

## 🏗️ Arquitetura e Fluxo de Dados

O pipeline foi desenhado para simular um ambiente corporativo de alta disponibilidade, separando responsabilidades de mensageria, processamento e armazenamento em nuvem:

1. **Origem (Mensageria):** Scripts em Python geram eventos transacionais JSON e os publicam em tópicos do **Apache Kafka** (rodando via Docker).
2. **Camada Bronze (Raw/Streaming):** O **Apache Spark (PySpark)** consome as mensagens do Kafka em micro-batches contínuos e armazena o dado bruto e imutável (JSON/TXT) no **Amazon S3**.
3. **Camada Silver (Refined/Streaming):** Um segundo job Spark monitora a camada Bronze, aplica tipagem rigorosa, anonimiza dados sensíveis via hash SHA-256 (Compliance LGPD) e converte os arquivos para o formato colunar **Parquet** com compressão Snappy.
4. **Camada Gold (Aggregated/Batch):** Um job batch processa a base histórica da Silver, executa agregações de negócio e consolida os indicadores financeiros finais no S3, prontos para consumo analítico estruturado.

## 🛠️ Tecnologias Utilizadas

* **Linguagem:** Python 3
* **Mensageria:** Apache Kafka e Zookeeper (Containers Docker)
* **Processamento:** Apache Spark (PySpark Structured Streaming e Batch)
* **Storage/Nuvem:** AWS S3 (com configuração de autenticação via IAM)
* **Qualidade e Testes:** `behave` (Behavior-Driven Development - BDD)

## ⚙️ Decisões de Engenharia e Otimizações

* **Resolução do *Small Files Problem*:** Implementação de gatilhos de tempo (`trigger(processingTime)`) nos jobs de streaming para acumular micro-batches eficientes, reduzindo chamadas de API (PUT requests) e custos de armazenamento no S3.
* **Governança e LGPD:** Mascaramento do identificador original do cliente (`id_cliente`) utilizando criptografia unidirecional (SHA-256) antes que o dado alcance a camada analítica (Silver).
* **Tolerância a Falhas:** Utilização de `checkpointLocation` no S3 em todos os fluxos de streaming, garantindo processamento *exactly-once* e permitindo a retomada segura após interrupções de hardware ou rede.
* **Storage Otimizado:** Transição de arquivos textuais na entrada (Bronze) para o formato binário colunar Parquet (Silver/Gold), reduzindo drasticamente o I/O e o custo de consultas (queries) via motores serverless.

## 🧪 Automação de Qualidade (BDD)

A segurança do dado é validada continuamente através de testes guiados por comportamento (BDD) utilizando a biblioteca `behave`. Os testes simulam a camada Silver e garantem que a coluna de CPF nunca vaze para o Data Lake e que o hash criptográfico seja gerado corretamente com 64 caracteres.

```bash
# Executando a validação de LGPD
behave