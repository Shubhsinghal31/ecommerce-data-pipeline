# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer — Business Aggregations
# MAGIC 
# MAGIC Builds five analytical tables from the Silver layer:
# MAGIC 1. **Seller Performance** — revenue, avg review, late delivery % per seller
# MAGIC 2. **Monthly Revenue** — revenue trend by product category and month
# MAGIC 3. **State Distribution** — customer count and revenue by Brazilian state
# MAGIC 4. **Late Delivery Heatmap** — late delivery rate by seller city
# MAGIC 5. **Category Performance** — revenue and review scores by product category

# COMMAND ----------

from pyspark.sql.functions import (
    col, avg, count, countDistinct, year, month, concat_ws, round as spark_round,
)
from pyspark.sql.functions import sum as spark_sum

spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Tables

# COMMAND ----------

orders      = spark.table("ecommerce_silver.orders")
order_items = spark.table("ecommerce_silver.order_items")
master      = spark.table("ecommerce_silver.master")
reviews     = spark.table("ecommerce_silver.reviews")

print("✅ Silver tables loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 1 — Seller Performance Scorecard

# COMMAND ----------

seller_performance = (
    order_items
    .join(
        orders.select("order_id", "order_status", "is_late_delivery"),
        on="order_id", how="left",
    )
    .join(
        reviews.select("order_id", "review_score"),
        on="order_id", how="left",
    )
    .groupBy("seller_id", "seller_city", "seller_state")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        spark_round(spark_sum(col("price").cast("double")), 2).alias("total_revenue"),
        spark_round(avg(col("price").cast("double")), 2).alias("avg_order_value"),
        spark_round(avg("review_score"), 2).alias("avg_review_score"),
        spark_round(
            (spark_sum("is_late_delivery") / count("order_id")) * 100, 2
        ).alias("late_delivery_pct"),
    )
    .filter(col("total_orders") >= 5)   # filter out low-volume sellers
    .orderBy(col("total_revenue").desc())
)

(
    seller_performance.write
    .format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("ecommerce_gold.seller_performance")
)

print(f"✅ seller_performance: {seller_performance.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 2 — Monthly Revenue by Product Category

# COMMAND ----------

monthly_revenue = (
    order_items
    .join(
        orders.select("order_id", "order_purchase_timestamp", "order_status"),
        on="order_id", how="left",
    )
    .filter(col("order_status") == "delivered")
    .withColumn("year", year("order_purchase_timestamp"))
    .withColumn("month", month("order_purchase_timestamp"))
    .withColumn("year_month", concat_ws("-", col("year"), col("month")))
    .groupBy("year_month", "year", "month", "category_en")
    .agg(
        spark_round(spark_sum(col("price").cast("double")), 2).alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        spark_round(avg(col("price").cast("double")), 2).alias("avg_order_value"),
    )
    .orderBy("year", "month")
)

(
    monthly_revenue.write
    .format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("ecommerce_gold.monthly_revenue")
)

print(f"✅ monthly_revenue: {monthly_revenue.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 3 — State-Level Customer & Revenue Distribution

# COMMAND ----------

state_distribution = (
    master
    .filter(col("order_status") == "delivered")
    .groupBy("customer_state")
    .agg(
        countDistinct("customer_id").alias("unique_customers"),
        countDistinct("order_id").alias("total_orders"),
        spark_round(spark_sum(col("payment_value").cast("double")), 2).alias("total_revenue"),
        spark_round(avg(col("payment_value").cast("double")), 2).alias("avg_order_value"),
        spark_round(avg("review_score"), 2).alias("avg_review_score"),
    )
    .orderBy(col("total_revenue").desc())
)

(
    state_distribution.write
    .format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("ecommerce_gold.state_distribution")
)

print(f"✅ state_distribution: {state_distribution.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 4 — Late Delivery Heatmap by Seller City

# COMMAND ----------

late_delivery_heatmap = (
    order_items
    .join(
        orders.select("order_id", "is_late_delivery", "delivery_delay_days"),
        on="order_id", how="left",
    )
    .groupBy("seller_city", "seller_state")
    .agg(
        count("order_id").alias("total_orders"),
        spark_sum("is_late_delivery").alias("late_orders"),
        spark_round(
            (spark_sum("is_late_delivery") / count("order_id")) * 100, 2
        ).alias("late_delivery_pct"),
        spark_round(avg("delivery_delay_days"), 1).alias("avg_delay_days"),
    )
    .filter(col("total_orders") >= 10)
    .orderBy(col("late_delivery_pct").desc())
)

(
    late_delivery_heatmap.write
    .format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("ecommerce_gold.late_delivery_heatmap")
)

print(f"✅ late_delivery_heatmap: {late_delivery_heatmap.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 5 — Product Category Performance

# COMMAND ----------

category_performance = (
    order_items
    .join(orders.select("order_id", "order_status"), on="order_id", how="left")
    .join(reviews.select("order_id", "review_score"), on="order_id", how="left")
    .filter(col("order_status") == "delivered")
    .groupBy("category_en")
    .agg(
        spark_round(spark_sum(col("price").cast("double")), 2).alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        spark_round(avg(col("price").cast("double")), 2).alias("avg_price"),
        spark_round(avg("review_score"), 2).alias("avg_review_score"),
    )
    .orderBy(col("total_revenue").desc())
)

(
    category_performance.write
    .format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("ecommerce_gold.category_performance")
)

print(f"✅ category_performance: {category_performance.count():,} rows")
print("\n✅ GOLD LAYER COMPLETE")
