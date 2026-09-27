from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum 

spark = SparkSession.builder \
.appName("EcommerceSilver") \
.getOrCreate()


spark.sparkContext.setLogLevel("WARN")

bronze_df = spark.read.parquet("data/bronze")


bronze_df.printSchema()
bronze_df.show(10, truncate = False)

print("NULL COUNTS:")

null_counts = bronze_df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in bronze_df.columns
])

null_counts.show()

silver_df = bronze_df.dropDuplicates(["event_id"])
print("Bronze count:", bronze_df.count())
print("Silver count:", silver_df.count())

silver_df.show(10, truncate=False)
print("Silver count:", silver_df.count())

silver_df.select(
    "event_type",
    "price",
    "quantity"
).show(20, truncate=False)

valid_event_types = [
    "page_view",
    "add_to_cart",
    "checkout_attempt",
    "payment_success",
    "payment_failed"
]


silver_df = silver_df.filter(
    col("event_type").isin(valid_event_types)
)

print("After event_type validation:", silver_df.count())

silver_df.select("event_type").distinct().show()



silver_df = silver_df.filter(
    (col("price") > 0) &
    (col("quantity") > 0)
)

print("After price and quantity validation:", silver_df.count())


silver_df.write \
.mode("overwrite") \
.parquet("data/silver")