FROM apache/spark:3.5.1

USER root

# --------------------------------------------------
# System + Python dependencies
# --------------------------------------------------
RUN apt-get update && apt-get install -y \
    curl \
    python3-pip \
 && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
    jupyterlab \
    pyspark \
    delta-spark \
    pyiceberg \
    google-auth \
    google-cloud-storage

# --------------------------------------------------
# Spark JARs
# --------------------------------------------------
RUN mkdir -p /opt/spark/jars && \
    # ==============================
    # Kafka (Spark 3.5.1 / Scala 2.12)
    # ==============================
    curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o /opt/spark/jars/kafka-clients-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \

    # ==============================
    # Delta Lake 3.2.0
    # ==============================
    curl -L -o /opt/spark/jars/delta-spark_2.12-3.2.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
    curl -L -o /opt/spark/jars/delta-storage-3.2.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar && \

    # ==============================
    # Apache Iceberg 1.4.2
    # ==============================
    curl -L -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar && \

    # ---------- GCS Connector ----------
    curl -L -o /opt/spark/jars/gcs-connector-hadoop3-2.2.17.jar \
      https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.17/gcs-connector-hadoop3-2.2.17.jar && \

    # ---------- BigQuery Connector ----------
    curl -L -o /opt/spark/jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar \
      https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.36.1/spark-bigquery-with-dependencies_2.12-0.36.1.jar

# --------------------------------------------------
# Workspace
# --------------------------------------------------
WORKDIR /opt/workspace

EXPOSE 8888 4040 4041 4042

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
