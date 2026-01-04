FROM apache/spark:3.5.1

USER root

RUN apt-get update && apt-get install -y curl python3-pip \
    && pip3 install --no-cache-dir jupyterlab pyspark delta-spark pyiceberg

# Download required JARs into Spark's classpath (/opt/spark/jars)
RUN mkdir -p /opt/spark/jars && \
    # --------------------------------------------------
    # Kafka integration for Spark Structured Streaming/Batch
    # --------------------------------------------------
    # Main connector for reading/writing Kafka topics in Spark SQL
    curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    # Token provider for Kafka authentication (required by spark-sql-kafka)
    curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    # Core Kafka client library (producing/consuming messages)
    curl -L -o /opt/spark/jars/kafka-clients-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    # Connection pooling utility used by Kafka clients
    curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    # --------------------------------------------------
    # Delta Lake 3.2.0 (for Spark 3.5.1 + Scala 2.12)
    # --------------------------------------------------
    # Main Delta Lake engine: SQL extensions, DataFrame APIs, transaction log handling
    curl -L -o /opt/spark/jars/delta-spark_2.12-3.2.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
    # Delta Lake storage layer: abstracts file system operations (required for DeltaCatalog and ACID)
    curl -L -o /opt/spark/jars/delta-storage-3.2.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar && \
    # --------------------------------------------------
    # Apache Iceberg 1.4.2 (for Spark 3.5.1 + Scala 2.12)
    # --------------------------------------------------
    # All-in-one runtime JAR with Iceberg Spark integration (ACID, schema evolution, time travel)
    curl -L -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar

WORKDIR /opt/workspace
EXPOSE 8888 4040 4041 4042

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]