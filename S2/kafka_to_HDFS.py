from pyspark.sql import SparkSession
from pyspark.sql.functions import col,from_json
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, LongType


spark = SparkSession.builder\
        .appName('kafkatohdfs')\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()


df = spark.readStream.format("kafka")\
                        .option("kafka.bootstrap.servers","localhost:9092")\
                        .option("subscribe","S2")\
                        .load()\
                        .selectExpr("CAST(value as string) as json")


schema = StructType([
    StructField("timestamp",LongType()),
    StructField("sensor_id",StringType()),
    StructField("value",IntegerType())
])


data = df.select(from_json(col("json"),schema).alias("data")).select("data.*")

print(data.printSchema())



clean_data = data.filter(
    col("timestamp").isNotNull() &
    col("sensor_id").isNotNull()&
    col("value").isNotNull()
)


query = data.writeStream.format("parquet")\
        .option("path","hdfs://localhost:9000/sensor_data")\
        .option("checkpointlocation","hdfs://localhost:9000/temp")\
        .trigger(processingTime ="30 seconds")\
        .start()


query.awaitTermination()