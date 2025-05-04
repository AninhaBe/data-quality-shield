from datetime import datetime
from pyspark.sql import SparkSession, DataFrame

def salvar_alerta_em_logs(df_alerta: DataFrame, nome_validacao: str, tipo_alerta: str, valor_observado: str, input_path: str, nome_tabela: str, gravidade: str = "alta"):
    spark = SparkSession.builder.getOrCreate()
    data_hoje = datetime.now().strftime('%Y-%m-%d')
    timestamp_execucao = datetime.now().isoformat(timespec="seconds")

    alerta_df = spark.createDataFrame([{
        "nome_validacao": nome_validacao,
        "tipo_alerta": tipo_alerta,
        "valor_observado": valor_observado,
        "timestamp_execucao": timestamp_execucao,
        "caminho_input": input_path,
        "gravidade": gravidade,
        "nome_tabela": nome_tabela
    }])

    output_path = f"wasbs://logs@datashieldstorage2025.blob.core.windows.net/qa_alerts/{data_hoje}/{nome_tabela}.parquet"
    alerta_df.write.mode("append").parquet(output_path)
