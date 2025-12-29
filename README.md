# Kafka Crypto Streaming

**Objective:** Stream live crypto trades from Coinbase into Kafka, process with Spark Structured Streaming, and store results in Delta Lake for analysis.

Minimal demo: Coinbase → Kafka → Spark → Delta.

Quick start (Docker):

1. Start services: `docker-compose up -d`
2. (Optional) Create topic: `./kafka_create_topic.sh` or inside container:
   `docker exec -it kafka bash -c "kafka-topics --bootstrap-server localhost:9092 --create --topic trades.raw --partitions 5 --replication-factor 1"`

Run locally:
- Producer: `producer-crypto-price/producer.py` (see `producer-crypto-price/.env`)
- Spark stream: `consumer-python-spark/consumer_spark_stream.py` (set `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `DELTA_OUTPUT_PATH`, `CHECKPOINT_PATH`)
- Quick consumer: `consumer.py` (connects to `localhost:29092`)

Useful paths:
- Notebooks: `consumer-jupyter-spark/notebooks/`
- Delta output: `consumer-output-delta-lake/delta-trades-table/`

Tips:
- Use `kafka:9092` from containers, `localhost:29092` from host.
- For Windows, run topic scripts in WSL/Git Bash or inside the `kafka` container.

License: add `LICENSE` if sharing publicly.

