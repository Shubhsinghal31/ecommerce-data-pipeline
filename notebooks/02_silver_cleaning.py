# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer — Data Cleaning & Enrichment
# MAGIC 
# MAGIC Reads Bronze tables and applies:
# MAGIC - Timestamp parsing and standardization
# MAGIC - Derived metrics (delivery delay, late delivery flag)
# MAGIC - Payment aggregation (one row per order)
# MAGIC - Review deduplication (latest review per order via window function)
# MAGIC - Product enrichment with English category names
# MAGIC - Geolocation averaging per zip code
# MAGIC - Master joined table across all entities

# COMMAND ----------

from pyspark.sql.functions import (
    col, when, datediff, to_timestamp, avg, round as spark_round,
    trim, upper, coalesce, lit, count
)
from pyspark.sql.functions import sum as spark_sum, first
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Tables

# COMMAND ----------

orders         = spark.table("ecommerce_bronze.orders")
order_items    = spark.table("ecommerce_bronze.order_items")
payments       = spark.table("ecommerce_bronze.payments")
reviews        = spark.table("ecommerce_bronze.reviews")
customers      = spark.table("ecommerce_bronze.customers")
sellers        = spark.table("ecommerce_bronze.sellers")
products       = spark.table("ecommerce_bronze.products")
geolocation    = spark.table("ecommerce_bronze.geolocation")
category_names = spark.table("ecommerce_bronze.category_names")

print("✅ All bronze tables loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Clean Orders — Parse Timestamps, Derive Delivery Metrics

# COMMAND ----------

timestamp_cols = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

orders_clean = orders
for ts_col in timestamp_cols:
    orders_clean = orders_clean.withColumn(ts_col, to_timestamp(ts_col))

# Compute how many days the actual delivery deviated from the estimate
# Positive = late, Negative = early
orders_clean = (
    orders_clean
    .withColumn(
        "delivery_delay_days",
        datediff(col("order_delivered_customer_date"), col("order_estimated_delivery_date"))
    )
    .withColumn(
        "is_late_delivery",
        when(col("delivery_delay_days") > 0, 1).otherwise(0)
    )
    .filter(col("order_status").isin("delivered", "shipped", "canceled", "processing"))
    .drop("_ingested_at", "_source_file")
)

print(f"✅ orders_clean: {orders_clean.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Aggregate Payments — One Row Per Order

# COMMAND ----------

# Group by order + payment type, sum values, then deduplicate to keep
# the dominant payment method per order
payments_agg = (
    payments
    .groupBy("order_id", "payment_type")
    .agg(
        spark_round(spark_sum(col("payment_value").cast("double")), 2).alias("payment_value"),
        count("payment_sequential").alias("payment_installments"),
    )
    .dropDuplicates(["order_id"])
    .drop("_ingested_at", "_source_file")
)

print(f"✅ payments_agg: {payments_agg.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deduplicate Reviews — Keep Latest Per Order

# COMMAND ----------

review_window = Window.partitionBy("order_id").orderBy(col("review_creation_date").desc())

reviews_clean = (
    reviews
    .withColumn("review_creation_date", to_timestamp("review_creation_date"))
    .withColumn("_row_num", row_number().over(review_window))
    .filter(col("_row_num") == 1)
    .drop("_row_num", "_ingested_at", "_source_file")
    .select("order_id", "review_score", "review_creation_date")
)

print(f"✅ reviews_clean: {reviews_clean.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Enrich Products — Join English Category Names

# COMMAND ----------

products_clean = (
    products
    .join(category_names, on="product_category_name", how="left")
    .withColumn(
        "category_en",
        coalesce(col("product_category_name_english"), lit("unknown"))
    )
    .select(
        "product_id", "category_en",
        "product_weight_g", "product_length_cm",
        "product_height_cm", "product_width_cm",
    )
)

print(f"✅ products_clean: {products_clean.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Average Geolocation Per Zip Code

# COMMAND ----------

# Multiple lat/lng entries per zip — average them for a single coordinate
geo_clean = (
    geolocation
    .groupBy("geolocation_zip_code_prefix", "geolocation_state", "geolocation_city")
    .agg(
        spark_round(avg("geolocation_lat"), 4).alias("lat"),
        spark_round(avg("geolocation_lng"), 4).alias("lng"),
    )
    .dropDuplicates(["geolocation_zip_code_prefix"])
)

print(f"✅ geo_clean: {geo_clean.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Enrich Order Items — Join Products & Sellers

# COMMAND ----------

order_items_clean = (
    order_items
    .join(products_clean, on="product_id", how="left")
    .join(
        sellers.select("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"),
        on="seller_id",
        how="left",
    )
    .drop("_ingested_at", "_source_file")
)

print(f"✅ order_items_clean: {order_items_clean.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Build Master Table — Full Join Across Entities

# COMMAND ----------

master = (
    orders_clean
    .join(
        customers.drop("_ingested_at", "_source_file"),
        on="customer_id", how="left",
    )
    .join(payments_agg, on="order_id", how="left")
    .join(reviews_clean, on="order_id", how="left")
    .join(
        order_items_clean
        .groupBy("order_id", "seller_id", "category_en")
        .agg(spark_round(avg("price"), 2).alias("avg_item_price")),
        on="order_id",
        how="left",
    )
)

print(f"✅ master: {master.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Persist Silver Tables

# COMMAND ----------

silver_tables = {
    "orders":      orders_clean,
    "payments":    payments_agg,
    "reviews":     reviews_clean,
    "products":    products_clean,
    "geolocation": geo_clean,
    "order_items": order_items_clean,
    "master":      master,
}

for tbl_name, df in silver_tables.items():
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"ecommerce_silver.{tbl_name}")
    )
    print(f"  ✅ Saved → ecommerce_silver.{tbl_name}")

print("\n✅ SILVER LAYER COMPLETE")
