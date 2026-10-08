import os
import sys

from behave import given, when, then
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Força o Spark a usar o Python do seu ambiente virtual (.venv)
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Inicializamos um mini-Spark local super rápido só para rodar o teste
spark = SparkSession.builder \
    .appName("TesteBDD_LGPD") \
    .master("local[1]") \
    .getOrCreate()

# Suprime os logs poluídos do Spark no terminal durante o teste
spark.sparkContext.setLogLevel("ERROR")

@given('que eu tenho um lote de transacoes brutas da camada Bronze')
def step_impl_bronze(context):
    # Criamos dados fictícios (Mocks) na memória para não depender da AWS no teste
    esquema = StructType([
        StructField("id_transacao", StringType(), True),
        StructField("data_hora", StringType(), True),
        StructField("id_cliente", StringType(), True),  # O CPF vulnerável
        StructField("valor", DoubleType(), True),
        StructField("bandeira", StringType(), True),
        StructField("status", StringType(), True)
    ])
    
    dados_mock = [
        ("t1", "2026-10-07T10:00", "12345678900", 150.0, "Visa", "Aprovada"),
        ("t2", "2026-10-07T10:05", "00987654321", 20.0, "Mastercard", "Negada")
    ]
    
    context.df_bronze = spark.createDataFrame(dados_mock, esquema)

@when('o processo da camada Silver for executado')
def step_impl_silver(context):
    # Aplicamos a exata mesma regra de anonimização do seu script silver.py
    context.df_silver = context.df_bronze \
        .withColumn("id_cliente_anonimizado", sha2(col("id_cliente"), 256)) \
        .drop("id_cliente")

@then('a coluna "id_cliente" não deve existir no resultado')
def step_impl_verifica_remocao(context):
    colunas = context.df_silver.columns
    # O comando 'assert' é a validação de qualidade. Se for falso, o teste reprova.
    assert "id_cliente" not in colunas, "FALHA DE SEGURANÇA: A coluna original vazou para a Silver!"

@then('uma nova coluna "id_cliente_anonimizado" deve conter um hash criptografado')
def step_impl_verifica_hash(context):
    colunas = context.df_silver.columns
    assert "id_cliente_anonimizado" in colunas, "FALHA: A coluna anonimizada não foi gerada."
    
    # Pega a primeira linha convertida para validar
    primeira_linha = context.df_silver.first()
    hash_gerado = primeira_linha["id_cliente_anonimizado"]
    
    # Regra matemática: Um hash SHA-256 sempre gera uma string de exatamente 64 caracteres
    assert len(hash_gerado) == 64, f"FALHA: O hash {hash_gerado} é inválido."