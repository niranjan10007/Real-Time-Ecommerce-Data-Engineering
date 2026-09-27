from confluent_kafka import Consumer

config = {}

with open("client.properties", "r") as file:
    for line in file:
        line = line.strip()

        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            config[key] = value


config["group.id"] = "ecommerce-consumer-group"
config["auto.offset.reset"] = "earliest"

consumer = Consumer(config)

consumer.subscribe(["ecommerce_events"])

print("Consumer started. Waiting for messages...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Consumer error:", msg.error())
            continue

        print("Received:", msg.value().decode("utf-8"))

except KeyboardInterrupt:
    print("Consumer stopped.")

finally:
    consumer.close()
