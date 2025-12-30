# Kafka Crypto Streaming

## Objective
Stream live crypto trades from Coinbase into Kafka, process with Spark Structured Streaming,
and store results in Delta Lake for analysis using Jupyter lab (Running spark).

**Minimal demo pipeline:**  
**Coinbase → Kafka → Spark → Delta Lake**

---

## Quick Start (Docker)

### 1. Start services
```bash
docker-compose up -d
```

### 2. ⚠️ REQUIRED: Create Kafka topic (DO NOT SKIP)
You must create the Kafka topic manually before running the producer or Spark streaming job. If the topic does not exist, the services will not run.

Create topic from host (recommended):
```bash
# Creates topic with partitions = number of PRODUCT_IDS in producer-crypto-price/.env
./kafka_create_topic.sh

# If the topic exists and you need to increase partitions to match PRODUCT_IDS:
./kafka_alter_partitions.sh
```

Or inside the Kafka container:
```bash
docker exec -it kafka bash -c "kafka-topics --bootstrap-server localhost:9092 \
 --create --topic trades.raw --partitions <NUM_PARTITIONS> --replication-factor 1"
```

If the topic is not created, the pipeline will fail. After creating the topic, restarting containers is safe and expected.

**Important Kafka notes**
- The number of Kafka partitions must match the number of `PRODUCT_IDS` in `producer-crypto-price/.env`.
- Each `product_id` is mapped to one Kafka partition.
- Partition mismatch will result in producer errors such as:
```text
KafkaError{code=_UNKNOWN_PARTITION}
```

---

## Run Locally (Without Docker)

### Producer
```bash
python producer-crypto-price/producer.py
```
Configuration: see `producer-crypto-price/.env` — it streams live trade data from Coinbase into Kafka.

### Spark Structured Streaming consumer
```bash
python consumer-python-spark/consumer_spark_stream.py
```
Required environment variables:
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC`
- `DELTA_OUTPUT_PATH`
- `CHECKPOINT_PATH`

### Quick Kafka consumer (debug)
```bash
python consumer.py
```
Connects to: `localhost:29092`

---

## JupyterLab (On-demand analysis)
JupyterLab is available at: http://localhost:8888
The kernel is pre-configured with Apache Spark, Spark SQL, and Delta Lake for interactive analysis.

---

## Useful paths
- Notebooks: `consumer-jupyter-spark/notebooks/`
- Delta Lake output: `consumer-output-delta-lake/delta-trades-table/`

---

## Networking notes
- Use `kafka:9092` when accessing Kafka from containers.
- Use `localhost:29092` when accessing Kafka from host.
- On Windows, run Kafka scripts using WSL / Git Bash or directly inside the Kafka container.

