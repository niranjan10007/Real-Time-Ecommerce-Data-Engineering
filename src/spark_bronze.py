from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)

# Create the Spark session
CLIENT_PROPERTIES = "client.properties"

spark = SparkSession.builder \
    .appName("EcommerceBronzeStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# Read Client Properties
properties = {}

with open(CLIENT_PROPERTIES, "r") as file:
    for line in file:
        line = line.strip()

        if not line or line.startswith("#"):
            continue

        key, value = line.split("=", 1)

        properties[key.strip()] = value.strip()


print("SECURITY PROTOCOL:", properties.get("security.protocol"))
print("SASL MECHANISM:", properties.get("sasl.mechanisms"))


# Create JAAS configuration
jaas_config = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f"username='{properties['sasl.username']}' "
    f"password='{properties['sasl.password']}';"
)


# Kafka configuration
kafka_options = {
    "kafka.bootstrap.servers": properties["bootstrap.servers"],
    "kafka.security.protocol": properties["security.protocol"],
    "kafka.sasl.mechanism": properties["sasl.mechanisms"],
    "kafka.sasl.jaas.config": jaas_config,
    "subscribe": "ecommerce_events",
    "startingOffsets": "latest"
}


# Read data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .load()


# Convert Kafka value from binary to JSON string
json_df = kafka_df.select(
    col("value").cast("string").alias("json_value")
)


event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("rating", DoubleType(), True),
    StructField("availability", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("device", StringType(), True),
    StructField("city", StringType(), True),
])

# Parse JSON
parsed_df = json_df.select(
    from_json(col("json_value"), event_schema).alias("data")
)


# Flatten the structured data
structured_df = parsed_df.select("data.*")


# Write streaming data to Bronze layer
query = structured_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "data/bronze") \
    .option("checkpointLocation", "data/checkpoints/bronze") \
    .start()


# Keep the streaming query running
query.awaitTermination()
























































