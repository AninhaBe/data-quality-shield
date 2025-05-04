from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataQualityShield") \
    .getOrCreate()
