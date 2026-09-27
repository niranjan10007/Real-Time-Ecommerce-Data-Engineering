from pyspark.sql import SparkSession
from pyspark.sql.functions import col , sum 

spark = SparkSession.builder \
.appName("EcommerceGold") \
.getOrCreate()

spark.sparkContext.setLogLevel("WARN")

silver_df = spark.read.parquet("data/silver")

silver_df.filter(
    col("event_type") == "payment_success"
).select(
    "event_type",
    "product_name",
    "price",
    "quantity",
    "payment_status"
).show(truncate=False)

silver_df.printSchema()
silver_df.show(10 , truncate = False )




revenue_df = silver_df.filter(
    col("event_type") == "payment_success"
)

revenue_df = revenue_df.withColumn(
    "revenue" , 
    col("price") * col("quantity")
)

total_revenue = revenue_df.select(
    sum("revenue").alias("total_revenue")
)

total_revenue.show()

#revenue by the each product 
product_revenue = revenue_df.groupBy(
    "product_name"
).agg(
    sum("revenue").alias("total_revenue")
).orderBy(
    col("total_revenue").desc()

)

product_revenue.show(truncate=False)
product_revenue.write \
.mode("overwrite") \
.parquet("data/gold/product_revenue")

total_revenue1= revenue_df.select(sum("revenue").alias("total_revenue1"))

total_revenue1.show()

total_revenue1.write \
    .mode("overwrite") \
    .parquet("data/gold/total_revenue1")


category_revenue = revenue_df.groupBy(
    "category"
    ).agg(
        sum("revenue").alias("category_vise_revenue")
    ).orderBy(
        col("category_vise_revenue").desc()
    )


category_revenue.show(truncate=False)

category_revenue.write \
.mode("overwrite")\
.parquet("data/gold/category_revenue")

payment_summary = silver_df.groupBy("" \
"payment_status").count() 
payment_summary.show()

payment_summary.write \
.mode("overwrite") \
.parquet("data/gold/payment_summary")

funnel_summary = silver_df.groupBy("" \
"event_type").count()
funnel_summary.show()

funnel_summary.write \
.mode("overwrite") \
.parquet("data/gold/funnel_summary")

session_summary = silver_df.groupBy(
    "session_id"
).agg(
    sum(
        (col("event_type") == "add_to_cart").cast("int")
    ).alias("add_to_cart_count"),

    sum(
        (col("event_type") == "checkout_attempt").cast("int")
    ).alias("checkout_count"),

    sum(
        (col("event_type") == "payment_success").cast("int")
    ).alias("payment_success_count"),

    sum(
        (col("event_type") == "payment_failed").cast("int")
    ).alias("payment_failed_count")
)

session_summary.show(truncate=False)


from pyspark.sql.functions import col, sum, when

session_summary = session_summary.withColumn(
    "final_stage",
    when(col("payment_success_count") > 0, "payment_success")
    .when(col("checkout_count") > 0, "checkout_attempt")
    .when(col("add_to_cart_count") > 0, "add_to_cart")
    .otherwise("page_view")
)

session_summary.show(truncate=False)

# Cart abandonment analysis main 
cart_abandonment = session_summary.withColumn(
    "cart_abandoned",
    when(
        (col("checkout_count") > 0) &
        (col("payment_success_count") == 0),
        "yes"
    ).otherwise("no")
)

cart_abandonment.show(truncate=False)

cart_abandonment.write \
    .mode("overwrite") \
    .parquet("data/gold/cart_abandonment")

abandonment_summary = cart_abandonment.groupBy(
    "cart_abandoned"
).count()

abandonment_summary.show()

abandonment_summary.write \
    .mode("overwrite") \
    .parquet("data/gold/abandonment_summary")