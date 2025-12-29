from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("crypto-realtime")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 1️⃣ Read from Kafka
df_raw = (
    spark.readStream    
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "trades.raw")
    .option("startingOffsets", "latest")
    .load()
)