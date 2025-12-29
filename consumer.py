import time
from confluent_kafka import Consumer

c = Consumer({
    "bootstrap.servers": "localhost:29092",   # HOST access
    "group.id": f"test-consumer-{int(time.time())}",
    "auto.offset.reset": "earliest"
})

c.subscribe(["trades.raw"])

while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"❌ Consumer error: {msg.error()}")
        continue
    print(f"✅ Received message: {msg.value().decode('utf-8')}")