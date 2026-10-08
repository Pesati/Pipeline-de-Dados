import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count
from dotenv import load_dotenv

# Carrega as chaves secretas do arquivo .env
load_dotenv()

# Configuração do Hadoop
pasta_hadoop = os.path.join(os.getcwd(), 'hadoop')
os.environ['HADOOP_HOME'] = pasta_hadoop
os.environ['PATH'] = os.path.join(pasta_hadoop, 'bin') + os.pathsep + os.environ.get('PATH', '')

NOME_BUCKET = "datalake-bancario-pesati"
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

versao_spark = pyspark.__version__
pacotes = (
    "org.apache.hadoop:hadoop-aws:3.5.0,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.766"
)

spark = SparkSession.builder \
    .appName("PipelineGold_AgregacaoBatch") \
    .config("spark.jars.packages", pacotes) \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.sa-east-1.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.endpoint.region", "sa-east-1") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Lendo a base histórica refinada da Camada Silver...")
# Leitura em Batch (sem readStream)
df_silver = spark.read.parquet(f"s3a://{NOME_BUCKET}/silver/transacoes")

print("Processando cálculos e indicadores de negócio...")
# Agregação: Total financeiro e volume de transações por Bandeira e Status
df_gold = df_silver.groupBy("bandeira", "status") \
    .agg(
        count("id_transacao").alias("volume_transacoes"),
        sum("valor").alias("receita_total")
    )

print(f"Substituindo dados consolidados no S3: s3a://{NOME_BUCKET}/gold/indicadores_cartoes/")

# Gravação em Batch sobrescrevendo os dados antigos (overwrite)
df_gold.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(f"s3a://{NOME_BUCKET}/gold/indicadores_cartoes")

print("Camada Gold gerada e atualizada com sucesso no S3!")