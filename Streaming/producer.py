from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

event_types = ["login", "logout", "purchase", "click"]

print("Starting producer... sending events to topic 'Demo'")

while True:
    event = {
        "event_id": random.randint(1000, 9999),
        "event_type": random.choice(event_types),
        "event_time": datetime.utcnow().isoformat()
    }
    producer.send("Demo", value=event)
    print("Produced:", event)
    time.sleep(1)  
