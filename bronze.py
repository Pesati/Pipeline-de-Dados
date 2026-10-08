import os
import pyspark
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Carrega as chaves secretas do arquivo .env
load_dotenv()

# Configuração do Hadoop para o Windows
pasta_hadoop = os.path.join(os.getcwd(), 'hadoop')
os.environ['HADOOP_HOME'] = pasta_hadoop
os.environ['PATH'] = os.path.join(pasta_hadoop, 'bin') + os.pathsep + os.environ.get('PATH', '')

# INFORMAÇÕES DA SUA AWS (Substitua pelos seus dados)
NOME_BUCKET = "datalake-bancario-pesati"
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

versao_spark = pyspark.__version__

# Pacotes do Kafka + Conectores do Amazon S3 (versões compatíveis com seu ambiente)
pacotes = (
    f"org.apache.spark:spark-sql-kafka-0-10_2.13:{versao_spark},"
    "org.apache.hadoop:hadoop-aws:3.5.0,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.766"
)

print("Iniciando Spark para Ingestão da Camada Bronze...")

# Inicializando o Spark com os drivers da AWS
spark = SparkSession.builder \
    .appName("PipelineBronze_KafkaToS3") \
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

print("Conectando no Kafka...")

# 1. Lendo da Fonte (Kafka)
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transacoes_bancarias") \
    .option("startingOffsets", "latest") \
    .load()

# 2. Transformação Zero: Apenas pegamos o JSON em formato binário e convertemos para texto
df_bronze = df_kafka.selectExpr("CAST(value AS STRING) as json_bruto")

print(f"Gravando dados puros no S3: s3a://{NOME_BUCKET}/bronze/transacoes/")

# 3. Gravando o dado bruto (Camada Bronze) no Amazon S3
query_bronze = df_bronze.writeStream \
    .outputMode("append") \
    .format("text") \
    .option("path", f"s3a://{NOME_BUCKET}/bronze/transacoes") \
    .option("checkpointLocation", "./checkpoints/bronze") \
    .trigger(processingTime="15 seconds") \
    .start()

query_bronze.awaitTermination()