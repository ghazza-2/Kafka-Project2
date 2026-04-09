import json

from kafka import KafkaConsumer
from kafka import KafkaProducer


ORDER_KAFKA_TOPIC = "order_details"
ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"
MAX_MESSAGES = 5
consumer = KafkaConsumer(
    ORDER_KAFKA_TOPIC, 
    bootstrap_servers="localhost:9092"
)
producer = KafkaProducer(bootstrap_servers="localhost:9092")


print("Gonna start listening")
for i, message in enumerate(consumer):
    for message in consumer:
        print("Ongoing transaction..")
        consumed_message = json.loads(message.value.decode())
        print(consumed_message)
        user_id = consumed_message["user_id"]
        total_cost = consumed_message["total_cost"]
        data = {
            "customer_id": user_id,
            "customer_email": f"{user_id}@gmail.com",
            "total_cost": total_cost
        }
        print("Successful transaction..")
        producer.send(ORDER_CONFIRMED_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))

            # Arrêter après MAX_MESSAGES
        if i + 1 >= MAX_MESSAGES:
            print(f"Processed {MAX_MESSAGES} messages, stopping consumer.")
            break