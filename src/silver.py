from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import from_json, col, sha2, current_timestamp
from spark_setup import obter_spark_session

spark = obter_spark_session("PipelineSilver_KafkaToS3")
NOME_BUCKET = "datalake-bancario-pesati"

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