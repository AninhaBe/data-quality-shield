from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente
load_dotenv()

# Recuperar chave da storage
azure_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
if not azure_key:
    raise ValueError("A variável AZURE_STORAGE_ACCOUNT_KEY não foi carregada do .env!")

# Caminho para o arquivo Parquet na nuvem
parquet_path = "wasbs://bronze@datashieldstorage2025.blob.core.windows.net/nyc_volunteers.parquet"

# Iniciar sessão Spark
spark = SparkSession.builder \
    .appName("Inspect Parquet") \
    .config("spark.jars", ",".join([
        "jars/hadoop-azure-3.2.0.jar",
        "jars/azure-storage-8.6.6.jar",
        "jars/jetty-util-9.4.35.v20201120.jar",
        "jars/jetty-util-ajax-9.4.35.v20201120.jar",
        "jars/jetty-client-9.4.35.v20201120.jar"
    ])) \
    .config(f"fs.azure.account.key.datashieldstorage2025.blob.core.windows.net", azure_key) \
    .getOrCreate()

# Ler e exibir schema do Parquet
df = spark.read.parquet(parquet_path)
df.printSchema()
df.show(5)

spark.stop()
