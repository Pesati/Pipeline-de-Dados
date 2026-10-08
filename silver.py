import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import from_json, col, sha2, current_timestamp
from dotenv import load_dotenv

# Carrega as chaves secretas do arquivo .env
load_dotenv()

# Configuração do Hadoop para o Windows
pasta_hadoop = os.path.join(os.getcwd(), 'hadoop')
os.environ['HADOOP_HOME'] = pasta_hadoop
os.environ['PATH'] = os.path.join(pasta_hadoop, 'bin') + os.pathsep + os.environ.get('PATH', '')

NOME_BUCKET = "datalake-bancario-pesati"
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

versao_spark = pyspark.__version__

pacotes = (
    f"org.apache.spark:spark-sql-kafka-0-10_2.13:{versao_spark},"
    "org.apache.hadoop:hadoop-aws:3.5.0,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.766"
)

spark = SparkSession.builder \
    .appName("PipelineSilver_S3ToS3") \
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

print("Lendo dados brutos da Camada Bronze no S3...")

# 1. Lê a pasta inteira da Camada Bronze de forma contínua (Streaming)
df_bronze = spark.readStream \
    .format("text") \
    .load(f"s3a://{NOME_BUCKET}/bronze/transacoes")

# 2. Define o esquema rigoroso
esquema = StructType([
    StructField("id_transacao", StringType(), True),
    StructField("data_hora", StringType(), True),
    StructField("id_cliente", StringType(), True),
    StructField("valor", DoubleType(), True),
    StructField("bandeira", StringType(), True),
    StructField("status", StringType(), True)
])

# 3. Transforma o texto em colunas estruturadas
df_estruturado = df_bronze.withColumn("dados", from_json(col("value"), esquema)).select("dados.*")

# 4. Regras de Negócio e LGPD da Camada Silver
df_silver = df_estruturado \
    .withColumn("id_cliente_anonimizado", sha2(col("id_cliente"), 256)) \
    .drop("id_cliente") \
    .withColumn("data_processamento", current_timestamp())

print(f"Gravando dados refinados em Parquet no S3: s3a://{NOME_BUCKET}/silver/transacoes/")

# 5. Salva em formato Parquet na nova pasta
query_silver = df_silver.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", f"s3a://{NOME_BUCKET}/silver/transacoes") \
    .option("checkpointLocation", "./checkpoints/silver") \
    .trigger(processingTime="30 seconds") \
    .start()

query_silver.awaitTermination()