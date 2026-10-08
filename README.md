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
```

## ⚙️ Como Configurar e Executar o Projeto

### Pré-requisitos
* **Python 3.10+** instalado.
* **Docker Desktop** instalado e rodando.
* Conta na **AWS** com um bucket S3 criado e credenciais programáticas (Access Key e Secret Key).
* *(Apenas para Windows)*: Binários do Hadoop (winutils) configurados localmente na pasta `hadoop/`.

### 1. Configuração do Ambiente
Clone o repositório e crie um ambiente virtual:
```bash
git clone https://github.com/SEU_USUARIO/NOME_DO_REPOSITORIO.git
cd NOME_DO_REPOSITORIO
python -m venv .venv
```

Ative o ambiente virtual:
* **Windows:** `.venv\Scripts\activate`
* **Linux/Mac:** `source .venv/bin/activate`

Instale as dependências do projeto:
```bash
pip install -r requirements.txt
```

### 2. Configuração de Credenciais
Crie um arquivo chamado `.env` na raiz do projeto e insira suas credenciais da AWS (este arquivo é ignorado pelo Git por segurança):
```env
AWS_ACCESS_KEY=sua_access_key_aqui
AWS_SECRET_KEY=sua_secret_key_aqui
```

### 3. Subindo a Infraestrutura (Kafka)
Inicie os containers do Zookeeper e Kafka em segundo plano:
```bash
docker-compose up -d
```

### 4. Executando o Pipeline de Dados
Abra terminais separados (com o `.venv` ativado em todos) para simular o ambiente de streaming e batch:

**Terminal 1 (Gerador de Transações):**
```bash
python src/gerador.py
```

**Terminal 2 (Camada Bronze - Ingestão Streaming):**
```bash
python src/1_bronze.py
```

**Terminal 3 (Camada Silver - Tratamento e LGPD):**
```bash
python src/2_silver.py
```

**Terminal 4 (Camada Gold - Agregação Batch):**
*(Execute este job apenas quando já houver dados na Silver)*
```bash
python src/3_gold.py
```

### 5. Validando a Qualidade (Testes BDD)
Para garantir que a regra de mascaramento de CPF da LGPD está funcionando e os dados sensíveis não estão vazando para o Data Lakehouse, execute a suíte de testes automatizados:
```bash
behave
```