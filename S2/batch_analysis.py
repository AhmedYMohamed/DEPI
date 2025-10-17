from pyspark.sql import SparkSession
from pyspark.sql.functions import col,mean,mode,stddev,abs


spark = SparkSession.builder.appName("processdata").getOrCreate()

df = spark.read.parquet("hdfs://localhost:9000/sensor_data/")


print(df.printSchema())