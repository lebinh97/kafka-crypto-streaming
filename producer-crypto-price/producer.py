import asyncio
import json
import websockets
from datetime import datetime, timezone
from confluent_kafka import Producer
import os

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCT_IDS = os.getenv("PRODUCT_IDS").split(",")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
LINGER_MS = os.getenv("LINGER_MS")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

# Kafka config
producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "linger.ms": LINGER_MS,
    "acks": "all"
})

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}: {msg.value()}", end='\r')

# Tạo map product_id -> partition index
product_partition_map = {pid: idx for idx, pid in enumerate(PRODUCT_IDS)}

async def stream_trades():
    async with websockets.connect(COINBASE_WS_URL) as websocket:
        # Subscribe to multiple coins at once
        subscribe_message = {
            "type": "subscribe",
            "product_ids": PRODUCT_IDS,
            "channels": ["matches"]
        }

        await websocket.send(json.dumps(subscribe_message))
        print(f"✅ Subscribed to trades: {', '.join(PRODUCT_IDS)}")

        while True:
            msg = await websocket.recv()
            data = json.loads(msg)

            if data.get("type") != "match":
                continue

            raw_trade = {
                "exchange": "coinbase",
                "symbol": data["product_id"],  # dynamic per trade
                "trade_id": data["trade_id"],
                "side": data["side"],
                "price": float(data["price"]),
                "size": float(data["size"]),
                "exchange_time": data["time"],
                "ingest_time": datetime.now(timezone.utc).isoformat()
            }

            # Chọn partition dựa trên map
            partition = product_partition_map.get(data["product_id"], 0)

            producer.produce(
                topic=KAFKA_TOPIC,
                key=data["product_id"],  # key = product_id
                value=json.dumps(raw_trade),
                partition=partition,     # force partition
                on_delivery=delivery_report
            )

            producer.poll(0)

if __name__ == "__main__":
    try:
        asyncio.run(stream_trades())
    except KeyboardInterrupt:
        print("Stopping...")    
    finally:
        producer.flush()
