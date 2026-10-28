# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Export Gold Views to CSV for Tableau
# MAGIC 
# MAGIC Exports all 5 Gold SQL views as single CSV files to a Unity Catalog Volume
# MAGIC so they can be downloaded and connected to Tableau Public.

# COMMAND ----------

import os

EXPORT_DIR = "/Volumes/workspace/default/raw_data/tableau_exports/"
os.makedirs(EXPORT_DIR, exist_ok=True)

# Tables to export (reads from the views, not the base tables)
GOLD_VIEWS = [
    "seller_performance",
    "monthly_revenue",
    "state_distribution",
    "late_delivery_heatmap",
    "category_performance",
]

# COMMAND ----------

for table_name in GOLD_VIEWS:
    df = spark.table(f"ecommerce_gold.{table_name}")

    temp_path = f"{EXPORT_DIR}{table_name}_temp"

    # Write as a single coalesced CSV
    (
        df.coalesce(1)
        .write
        .option("header", "true")
        .mode("overwrite")
        .csv(temp_path)
    )

    # Rename the part-* file to a clean filename
    part_files = [
        f.path for f in dbutils.fs.ls(temp_path) if f.name.startswith("part-")
    ]

    if part_files:
        dbutils.fs.cp(part_files[0], f"{EXPORT_DIR}{table_name}.csv")
        dbutils.fs.rm(temp_path, recurse=True)
        print(f"  ✅ Exported: {table_name}.csv ({df.count():,} rows)")
    else:
        print(f"  ⚠️  No part file found for {table_name}")

print(f"\n✅ ALL EXPORTS COMPLETE")
print(f"📁 Location: {EXPORT_DIR}")
