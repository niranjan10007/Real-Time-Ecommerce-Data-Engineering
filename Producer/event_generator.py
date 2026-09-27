import json
from datetime import datetime, timezone
import random
import uuid
import time
from confluent_kafka import Producer


# Read product catalog
with open("data/product_catalog.json", "r") as file:
    products = json.load(file)


# Users
users = [
    "USER101",
    "USER102",
    "USER103",
    "USER104",
    "USER105"
]


# Session for each user
user_sessions = {
    "USER101": "SES1001",
    "USER102": "SES1002",
    "USER103": "SES1003",
    "USER104": "SES1004",
    "USER105": "SES1005"
}


# Payment methods
payment_methods = [
    "credit_card",
    "upi",
    "debit_card",
    "net_banking"
]


# Devices
devices = [
    "mobile",
    "desktop",
    "tablet"
]


# Cities
cities = [
    "Pune",
    "Mumbai",
    "Delhi",
    "Bengaluru",
    "Hyderabad"
]


# Kafka configuration
config = {}

with open("client.properties", "r") as file:

    for line in file:

        line = line.strip()

        if line and not line.startswith("#"):

            key, value = line.split("=", 1)

            config[key] = value


# Create Kafka producer
producer = Producer(config)

topic = "ecommerce_events"


# Realistic customer journeys
journeys = {

    "USER101": [
        "page_view",
        "add_to_cart",
        "checkout_attempt",
        "payment_success"
    ],

    "USER102": [
        "page_view",
        "add_to_cart",
        "checkout_attempt"
    ],

    "USER103": [
        "page_view",
        "add_to_cart"
    ],

    "USER104": [
        "page_view"
    ],

    "USER105": [
        "page_view",
        "add_to_cart",
        "checkout_attempt",
        "payment_failed"
    ]
}


# Track current position in each user's journey
journey_position = {

    "USER101": 0,
    "USER102": 0,
    "USER103": 0,
    "USER104": 0,
    "USER105": 0
}


# Select one product for each session
session_products = {

    user: random.choice(products)

    for user in users
}


# Process users one by one
journey_users = users.copy()

journey_user_index = 0


def generate_event():

    global journey_user_index

    # Select current user
    user_id = journey_users[journey_user_index]

    # Select next event from the user's journey
    event_type = journeys[user_id][journey_position[user_id]]

    # Generate unique event ID
    event_id = "EVT-" + str(uuid.uuid4())

    # Generate quantity
    quantity = random.randint(1, 3)

    # Use the same product for the session
    product = session_products[user_id]


    # Set payment status
    if event_type == "payment_success":

        payment_status = "success"

    elif event_type == "payment_failed":

        payment_status = "failed"

    else:

        payment_status = None


    # Set payment method only for payment events
    if event_type in ["payment_success", "payment_failed"]:

        payment_method = random.choice(payment_methods)

    else:

        payment_method = None


    # Random device and city
    device = random.choice(devices)

    city = random.choice(cities)


    # Create event
    event = {

        "event_id": event_id,

        "event_type": event_type,

        "event_time": datetime.now(timezone.utc).isoformat(),

        "user_id": user_id,

        "session_id": user_sessions[user_id],

        "product_id": product["product_id"],

        "product_name": product["product_name"],

        "category": product["category"],

        "price": product["price"],

        "rating": product["rating"],

        "availability": product["availability"],

        "quantity": quantity,

        "payment_method": payment_method,

        "payment_status": payment_status,

        "device": device,

        "city": city
    }


    # Move to next event in this user's journey
    if journey_position[user_id] < len(journeys[user_id]) - 1:

        journey_position[user_id] += 1

    else:

        # Restart the journey after completion
        journey_position[user_id] = 0


    # Move to next user
    journey_user_index = (
        journey_user_index + 1
    ) % len(journey_users)


    return event


def delivery_report(err, msg):

    if err is not None:

        print(f"Message delivery failed: {err}")

    else:

        print(
            f"Message delivered to {msg.topic()} "
            f"[partition {msg.partition()}]"
        )


# Continuously generate events
while True:

    event = generate_event()

    event_json = json.dumps(event)


    # Send event to Kafka
    producer.produce(
        topic,
        value=event_json.encode("utf-8"),
        callback=delivery_report
    )


    # Trigger Kafka delivery callback
    producer.poll(0)


    # Print event
    print(event_json)


    # Wait 1 second before next event
    time.sleep(1)


    # Flush producer
    producer.flush()