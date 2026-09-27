from pyspark.sql import SparkSession

# Create Spark application
spark = (
    SparkSession.builder
    .appName("EcommerceKafkaStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Read Confluent Cloud configuration
kafka_config = {}

with open("client.properties", "r") as file:
    for line in file:
        line = line.strip()

        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            kafka_config[key] = value


print("Security protocol:", kafka_config.get("security.protocol"))
print("SASL mechanism:", kafka_config.get("sasl.mechanisms"))
print("Username exists:", bool(kafka_config.get("sasl.username")))


# Read events from Kafka
events = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        kafka_config["bootstrap.servers"]
    )
    .option("subscribe", "ecommerce_events")
    .option("startingOffsets", "earliest")
    .option(
        "kafka.security.protocol",
        kafka_config["security.protocol"]
    )
    .option("kafka.sasl.mechanism",
    kafka_config["sasl.mechanisms"]
    )
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["sasl.username"]}" password="{kafka_config["sasl.password"]}";'
    )
    .load()
)


# Kafka stores message value as binary.
# Convert it into readable text.
events_text = events.selectExpr(
    "CAST(value AS STRING) AS event"
)


# Display incoming events
query = (
    events_text.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()