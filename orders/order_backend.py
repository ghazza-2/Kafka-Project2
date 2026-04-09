import json
import time

from kafka import KafkaProducer 

ORDER_KAFKA_TOPIC = "order_details"
ORDER_LIMIT = 20

producer = KafkaProducer(bootstrap_servers="localhost:9092")

print("Will generate one unique order every 5 seconds")


for i in range(1, ORDER_LIMIT):
    data = {
        "order_id": i,
        "user_id": f"tom_{i}",
        "total_cost": i * 5,
        "items": "burger,sandwich",
    }

    producer.send(ORDER_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))
    print(f"Done Sending..{i}")
    # time.sleep(5)