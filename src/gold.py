from pyspark.sql.functions import col, sum, count
from spark_setup import obter_spark_session

spark = obter_spark_session("PipelineBronze_KafkaToS3")
NOME_BUCKET = "datalake-bancario-pesati"

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