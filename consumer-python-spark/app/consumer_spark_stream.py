import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Load env vars
def env(key, default=None):
    return os.environ.get(key, default)

# Spark Session
spark = (
    SparkSession.builder
    .appName(env("SPARK_APP_NAME", "KafkaToDeltaTrades"))
    .config("spark.ui.port", env("SPARK_UI_PORT", "4050"))
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.memory", env("SPARK_DRIVER_MEMORY", "4g"))
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=100")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Paths & config
output_path = env("DELTA_OUTPUT_PATH")
checkpoint_path = env("CHECKPOINT_PATH")
partition_field = env("DELTA_PARTITION_FIELD", "hour_id")

# Kafka source
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", env("KAFKA_BOOTSTRAP_SERVERS"))
    .option("subscribe", env("KAFKA_TOPIC"))
    .option("startingOffsets", env("KAFKA_STARTING_OFFSETS"))
    .option("maxOffsetsPerTrigger", env("MAX_OFFSETS_PER_TRIGGER"))  # ← LIMITS ROWS PER BATCH
    .load()
)

# Parse JSON
value_schema = StructType([
    StructField("exchange", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("trade_id", LongType(), True),
    StructField("side", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("size", DoubleType(), True),
    StructField("exchange_time", StringType(), True),
    StructField("ingest_time", StringType(), True),
])

df_parsed = df.select(
    col("offset").cast("long"),
    from_json(col("value").cast("string"), value_schema).alias("data")
).filter(col("data").isNotNull())

# Flatten + add computed columns
df_final = df_parsed.select(
    "offset",
    "data.symbol",
    "data.trade_id",
    "data.side",
    "data.price",
    "data.size",
    "data.exchange_time",
    "data.ingest_time"
).withColumn(
    "exchange_time_ts", from_utc_timestamp(to_timestamp("exchange_time"), "Asia/Bangkok")
).withColumn(
    "hour_id", date_format(col("exchange_time_ts"), "yyyyMMddHH")
)

# Write to Delta
query = (
    df_final.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(processingTime='5 seconds')  # fixed interval
    .option("checkpointLocation", checkpoint_path)
    .start(output_path)
)

print("✅ Streaming started → writing to Delta")
query.awaitTermination()