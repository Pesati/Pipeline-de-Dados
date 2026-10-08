import os
import sys
import pyspark
from pyspark.sql import SparkSession
from dotenv import load_dotenv

def obter_spark_session(nome_app):
    # Carrega as chaves secretas
    load_dotenv()

    # Configuração do Hadoop para Windows
    pasta_hadoop = os.path.join(os.getcwd(), 'hadoop')
    os.environ['HADOOP_HOME'] = pasta_hadoop
    os.environ['PATH'] = os.path.join(pasta_hadoop, 'bin') + os.pathsep + os.environ.get('PATH', '')
    
    # Força o Spark a usar o Python do ambiente virtual
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

    pacotes = (
        f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__},"
        "org.apache.hadoop:hadoop-aws:3.5.0,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.766"
    )

    spark = SparkSession.builder \
        .appName(nome_app) \
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
    
    return spark