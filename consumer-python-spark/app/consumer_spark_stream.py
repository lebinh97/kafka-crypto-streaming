import os
import threading
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ============================================================================
# CONFIGURATION & INITIALIZATION
# ============================================================================

# Load env vars
def env(key, default=None):
    return os.environ.get(key, default)

# Spark Session Configuration - Iceberg
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName(env("SPARK_APP_NAME", "KafkaToIcebergTrades"))
    .config("spark.ui.port", env("SPARK_UI_PORT", "4050"))

    # ✅ BOTH extensions in ONE line (order does not matter)
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,"
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    
    # ✅ Delta catalog (keeps Delta working)
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    
    # ✅ Iceberg catalog
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "hadoop")
    .config("spark.sql.catalog.iceberg.warehouse", env("ICEBERG_WAREHOUSE_PATH", "/opt/consumer-output-iceberg"))
    
    # Memory config
    .config("spark.driver.memory", env("SPARK_DRIVER_MEMORY", "6g"))
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=100")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Paths & Configuration
iceberg_catalog = env("ICEBERG_CATALOG_NAME", "iceberg")
iceberg_namespace = env("ICEBERG_NAMESPACE", "crypto")
iceberg_table = env("ICEBERG_TABLE_NAME", "trades")
iceberg_table_identifier = f"{iceberg_catalog}.{iceberg_namespace}.{iceberg_table}"
checkpoint_path = env("CHECKPOINT_PATH", "/opt/consumer-output-iceberg/checkpoints/crypto-trades")
partition_field = env("ICEBERG_PARTITION_FIELD", "hour_id")

# ============================================================================
# MAIN STREAMING PIPELINE: Kafka → Parse JSON → Enrich → Iceberg
# ============================================================================

# 1. Read from Kafka Topic (trades.raw)
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", env("KAFKA_BOOTSTRAP_SERVERS"))
    .option("subscribe", env("KAFKA_TOPIC"))
    .option("startingOffsets", env("KAFKA_STARTING_OFFSETS"))
    .load()
)

# 2. Parse JSON Payload
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
    col("key").cast("string").alias("key"),
    col("offset").cast("long"),
    from_json(col("value").cast("string"), value_schema).alias("data")
).filter(col("data").isNotNull())

# 3. Flatten & Enrich with Computed Columns
df_final = df_parsed.select(
    "key",
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

# ============================================================================
# BACKGROUND THREAD: Iceberg File Rewrite (Excludes Current Hour Partition)
# Purpose: Compact small files produced by streaming writes
# Strategy: Only compact partitions older than current hour to avoid conflicts
# ============================================================================

def rewrite_iceberg_files():
    """Compact small Iceberg data files, excluding current hour partition"""
    while True:
        try:
            time.sleep(600)  # Every 30 minutes
            
            # Get current hour_id (same format as partition key) - UTC+7
            current_hour_id = datetime.now(timezone(timedelta(hours=7))).strftime("%Y%m%d%H")
            
            print(f"🔧 [ICEBERG THREAD] Running rewrite_data_files (excluding hour_id >= {current_hour_id})...")
            spark.sql(f"""
                CALL iceberg.system.rewrite_data_files(
                    table => '{iceberg_namespace}.{iceberg_table}',
                    strategy => 'binpack',
                    where => 'hour_id < "{current_hour_id}"',
                    options => map(
                        'target-file-size-bytes', '{env("ICEBERG_TARGET_FILE_SIZE", "134217728")}',
                        'min-input-files', '{env("ICEBERG_MIN_INPUT_FILES", "2")}'
                    )
                )
            """)
            print("✅ [ICEBERG THREAD] rewrite_data_files completed")
        except Exception as e:
            print(f"⚠️  [ICEBERG THREAD] rewrite_data_files failed: {e}")

# Start background threads (daemon mode = auto-stop when main thread exits)
optimize_thread = threading.Thread(target=rewrite_iceberg_files, daemon=True)
optimize_thread.start()

# ============================================================================
# TABLE INITIALIZATION: Create Iceberg table if not exists
# ============================================================================

def init_iceberg_table():
    """Create Iceberg namespace and table if they don't exist"""
    table_exists = spark.catalog.tableExists(iceberg_table_identifier)
    
    if not table_exists:
        # Create namespace if not exists
        print(f"🔨 Creating Iceberg namespace {iceberg_namespace} if not exists...")
        spark.sql(f"""
            CREATE NAMESPACE IF NOT EXISTS {iceberg_catalog}.{iceberg_namespace}
        """)
        print(f"✅ Namespace {iceberg_catalog}.{iceberg_namespace} ready")
        
        # Create table
        print(f"🔨 Creating Iceberg table {iceberg_table_identifier} with hour_id partition...")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {iceberg_table_identifier} (
                key STRING,
                offset BIGINT,
                symbol STRING,
                trade_id BIGINT,
                side STRING,
                price DOUBLE,
                size DOUBLE,
                exchange_time STRING,
                ingest_time STRING,
                exchange_time_ts TIMESTAMP,
                hour_id STRING
            )
            USING iceberg
            PARTITIONED BY (hour_id)
        """)
        print(f"✅ Table {iceberg_table_identifier} created successfully")
    else:
        print(f"✅ Table {iceberg_table_identifier} already exists")

# Initialize table before streaming starts
init_iceberg_table()

# ============================================================================
# MAIN STREAMING QUERY: foreachBatch Mode to Iceberg
# ============================================================================

def write_batch_to_iceberg(batch_df, batch_id):
    """Write each micro-batch to Iceberg and log completion timestamp"""
    row_count = batch_df.count()
    
    if row_count > 0:
        batch_df.write.format("iceberg").mode("append").save(iceberg_table_identifier)
    
    now = datetime.now(timezone(timedelta(hours=7))).strftime("%Y-%m-%d %H:%M:%S")
    print(f"📦 [BATCH {batch_id}] Completed at {now} | Rows: {row_count}")

query = (
    df_final.writeStream
    .foreachBatch(write_batch_to_iceberg)
    .outputMode("append")
    .trigger(processingTime=f'{env("PROCESSING_TRIGGER_INTERVAL", "30")} seconds')
    .option("checkpointLocation", checkpoint_path)
    .start()
)

# Log startup info
print("=" * 80)
print("✅ [MAIN] Streaming pipeline started")
print("=" * 80)
print(f"📍 Kafka Bootstrap: {env('KAFKA_BOOTSTRAP_SERVERS')}")
print(f"📍 Topic: {env('KAFKA_TOPIC')}")
print(f"📁 Iceberg Table: {iceberg_table_identifier}")
print(f"📂 Iceberg Warehouse: {env('ICEBERG_WAREHOUSE_PATH', '/opt/consumer-output-iceberg')}")
print(f"⏱️  Streaming Trigger: {env('PROCESSING_TRIGGER_INTERVAL', '30')} seconds")
print(f"🔧 Iceberg rewrite thread: Every 30 minutes")
print("=" * 80)

# Block main thread until streaming job completes
query.awaitTermination()


