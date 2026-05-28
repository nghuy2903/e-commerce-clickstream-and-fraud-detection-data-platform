"""
Minimal clickstream generator entrypoint.
"""
import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'user_events'


EVENT_TYPES = ["search", "view", "add_to_cart", "checkout"]
BOT_IPS = ["192.168.1.99", "10.0.0.88"]

def create_producer() -> KafkaProducer:
    
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def generate_event(is_bot: bool) -> dict:
    timestamp = datetime.utcnow().isoformat()
    
    if is_bot:
        
        return {
            "event_id": str(uuid.uuid4()),
            "user_id": f"bot_{random.randint(1, 5)}",
            "ip_address": random.choice(BOT_IPS),
            "event_type": random.choice(["search", "view"]),
            "timestamp": timestamp,
            "is_suspected_bot": True # Cờ đánh dấu để dễ đối chiếu kết quả sau này
        }
    else:
        
        return {
            "event_id": str(uuid.uuid4()),
            "user_id": f"user_{random.randint(100, 999)}",
            "ip_address": f"172.16.{random.randint(0, 255)}.{random.randint(0, 255)}",
            "event_type": random.choice(EVENT_TYPES),
            "timestamp": timestamp,
            "is_suspected_bot": False
        }

def main() -> None:
    
    try:
        producer = create_producer()
        print(f"Bắt đầu đẩy dữ liệu vào topic '{TOPIC_NAME}' tại {KAFKA_BROKER}...")

    except Exception as e:
        print(f"Không thể kết nối đến Kafka Broker: {e}")
        return
        
    try:
        while True:
            is_bot = random.random() > 0.5
            event = generate_event(is_bot=is_bot)
            
            producer.send(TOPIC_NAME, event)
            
            print(f"Sent: {json.dumps(event)}")
            

            events_per_sec = random.randint(10, 30)
            time.sleep(1.0 / events_per_sec)
            
    except KeyboardInterrupt:
        print("\n Đã nhận lệnh dừng tiến trình sinh dữ liệu.")
    finally:

        producer.flush()
        producer.close()
        print("Đã đóng kết nối Kafka an toàn.")

if __name__ == "__main__":
    main()