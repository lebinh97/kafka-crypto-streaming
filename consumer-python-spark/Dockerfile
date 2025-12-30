FROM apache/spark:3.5.1

WORKDIR /opt/app

# Download required JARs into Spark's classpath
# Why this is needed:
# - apache/spark image is "bare" (no Kafka / Delta support by default)
# - Spark loads all JARs under /opt/spark/jars automatically
# - Avoids --packages at runtime (faster startup, reproducible builds)
RUN mkdir -p /opt/spark/jars && \
    \
    # Kafka source for Structured Streaming (Spark ↔ Kafka integration)
    curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    \
    # Kafka token provider (required by spark-sql-kafka)
    curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    \
    # Kafka client library (version aligned with Spark 3.5.x)
    curl -L -o /opt/spark/jars/kafka-clients-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    \
    # Object pool used internally by Kafka consumer
    curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    \
    # Delta Lake Spark integration (ACID, MERGE, schema evolution)
    curl -L -o /opt/spark/jars/delta-spark_2.12-3.2.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
    \
    # Delta Lake storage layer (transaction log, file actions)
    curl -L -o /opt/spark/jars/delta-storage-3.2.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar


# Copy Spark job
# COPY consumer_spark_stream.py .

# IMPORTANT:
# Use shell form so env vars from .env are expanded
CMD /opt/spark/bin/spark-submit \
    --master local[*] \
    --driver-memory ${SPARK_DRIVER_MEMORY:-4g} \
    consumer_spark_stream.py
