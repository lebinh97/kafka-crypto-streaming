kafka-topics --bootstrap-server localhost:9092 --create \
  --topic trades.raw \
  --partitions 5 \
  --replication-factor 1