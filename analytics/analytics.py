import json

from kafka import KafkaConsumer


ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"


consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC, 
    bootstrap_servers="localhost:9092"
)

total_orders_count = 0
total_revenue = 0
print("Gonna start listening")
for i, message in enumerate(consumer):
        print("Updating analytics..")
        consumed_message = json.loads(message.value.decode())
        total_cost = float(consumed_message["total_cost"])
        total_orders_count += 1
        total_revenue += total_cost
        print(f"Orders so far today: {total_orders_count}")
        print(f"Revenue so far today: {total_revenue}")

         # Arrêter après MAX_MESSAGES
        # if i + 1 >= MAX_MESSAGES:
        #     print(f"Reached {MAX_MESSAGES} messages, stopping consumer.")
        #     break