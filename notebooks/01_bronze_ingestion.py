# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer — Raw Data Ingestion
# MAGIC 
# MAGIC This notebook ingests the Olist Brazilian E-Commerce dataset (9 CSV files)
# MAGIC into managed Delta tables under the `ecommerce_bronze` schema.
# MAGIC 
# MAGIC **What happens here:**
# MAGIC - Read each CSV with schema inference
# MAGIC - Append lineage metadata (`_ingested_at`, `_source_file`)
# MAGIC - Run basic data quality checks (null PKs, duplicate counts)
# MAGIC - Persist as managed Delta tables

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Ensure the bronze volume and database exist
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_bronze")

# Verify volumes available
spark.sql("SHOW VOLUMES IN workspace.default").show()

# COMMAND ----------

# Path to uploaded Kaggle CSVs in Unity Catalog Volume
DATA_DIR = "/Volumes/workspace/default/raw_data/Brazil E-Commerce/"

# Registry mapping: table_name -> (csv_filename, primary_key_column)
TABLE_REGISTRY = {
    "orders":         ("olist_orders_dataset.csv",              "order_id"),
    "order_items":    ("olist_order_items_dataset.csv",         "order_id"),
    "payments":       ("olist_order_payments_dataset.csv",      "order_id"),
    "reviews":        ("olist_order_reviews_dataset.csv",       "review_id"),
    "customers":      ("olist_customers_dataset.csv",           "customer_id"),
    "sellers":        ("olist_sellers_dataset.csv",             "seller_id"),
    "products":       ("olist_products_dataset.csv",            "product_id"),
    "geolocation":    ("olist_geolocation_dataset.csv",         "geolocation_zip_code_prefix"),
    "category_names": ("product_category_name_translation.csv", "product_category_name"),
}

# COMMAND ----------

def load_and_persist_bronze(tbl_name, csv_filename, pk_column):
    """
    Read a single CSV into a DataFrame, stamp it with audit columns,
    run quick quality checks, and write it as a managed Delta table.
    """
    print(f"\n>>> Processing: {tbl_name}")

    # Read with header + schema inference; handle multi-line fields
    raw_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(f"{DATA_DIR}{csv_filename}")
    )

    # Add lineage columns
    raw_df = (
        raw_df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit(csv_filename))
    )

    # Quality checks
    total_rows = raw_df.count()
    null_pk_count = raw_df.filter(raw_df[pk_column].isNull()).count()
    duplicate_count = total_rows - raw_df.dropDuplicates([pk_column]).count()

    print(f"    Rows       : {total_rows:,}")
    print(f"    Null PKs   : {null_pk_count}")
    print(f"    Duplicates : {duplicate_count}")

    # Write as managed Delta table (Databricks handles storage location)
    (
        raw_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"ecommerce_bronze.{tbl_name}")
    )

    print(f"    ✅ Saved → ecommerce_bronze.{tbl_name}")
    return {
        "table": tbl_name,
        "rows": total_rows,
        "null_pks": null_pk_count,
        "duplicates": duplicate_count,
    }

# COMMAND ----------

# Run ingestion for all tables
ingestion_results = []
for name, (csv_file, pk_col) in TABLE_REGISTRY.items():
    result = load_and_persist_bronze(name, csv_file, pk_col)
    ingestion_results.append(result)

# COMMAND ----------

# Print summary report
print("\n" + "=" * 60)
print("  BRONZE INGESTION SUMMARY")
print("=" * 60)
for r in ingestion_results:
    flag = "✅" if r["null_pks"] == 0 and r["duplicates"] == 0 else "⚠️"
    print(
        f"  {flag}  {r['table']:<22} | "
        f"rows: {r['rows']:>8,} | "
        f"null_pks: {r['null_pks']} | "
        f"dups: {r['duplicates']}"
    )
