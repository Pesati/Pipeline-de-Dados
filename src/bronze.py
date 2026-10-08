from spark_setup import obter_spark_session

spark = obter_spark_session("PipelineBronze_KafkaToS3")
NOME_BUCKET = "datalake-bancario-pesati"

print("Lendo dados do Kafka em Streaming...")

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