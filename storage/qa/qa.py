import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from qa_utils import salvar_alerta_em_logs

load_dotenv()
account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
container = os.getenv("SILVER_CONTAINER")

spark = SparkSession.builder \
    .appName("Validação QA") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.1,com.microsoft.azure:azure-storage:8.6.6") \
    .getOrCreate()

spark.conf.set(f"fs.azure.account.key.{account_name}.blob.core.windows.net", account_key)

input_path = f"wasbs://{container}@{account_name}.blob.core.windows.net/nyc_volunteers_silver.parquet"
df = spark.read.parquet(input_path)

nome_tabela = "nyc_volunteers_silver"

# Validação de duplicados
duplicados = df.groupBy("Unique Squirrel ID").agg(count("*").alias("count")).filter(col("count") > 1)

if duplicados.count() > 0:
    print("[ALERTA] Há registros duplicados por Unique Squirrel ID")
    duplicados.show()
    ids = ", ".join([row["Unique Squirrel ID"] for row in duplicados.collect()])
    salvar_alerta_em_logs(duplicados, "Duplicidade por Unique Squirrel ID", "duplicados", ids, input_path, nome_tabela)
else:
    print("[OK] Nenhum duplicado encontrado")
