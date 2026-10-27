# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # SQL Views — Enriched Gold Views for BI Tools
# MAGIC 
# MAGIC Creates 5 Databricks SQL views on top of Gold tables, adding:
# MAGIC - Seller tier classification (Top / Average / Underperformer)
# MAGIC - Cumulative revenue window functions
# MAGIC - Revenue share percentages
# MAGIC - Delivery risk level labels
# MAGIC - Revenue rank per category

# COMMAND ----------

# Define all views in a single dictionary for easy maintenance
VIEW_DEFINITIONS = {

    "vw_seller_performance": """
        SELECT
            seller_id,
            seller_city,
            seller_state,
            total_orders,
            total_revenue,
            avg_order_value,
            avg_review_score,
            late_delivery_pct,
            CASE
                WHEN avg_review_score >= 4.5 AND late_delivery_pct < 10 THEN 'Top Seller'
                WHEN avg_review_score >= 3.5 AND late_delivery_pct < 25 THEN 'Average Seller'
                ELSE 'Underperformer'
            END AS seller_tier
        FROM ecommerce_gold.seller_performance
    """,

    "vw_monthly_revenue_trend": """
        SELECT
            year,
            month,
            year_month,
            category_en,
            total_revenue,
            total_orders,
            avg_order_value,
            SUM(total_revenue) OVER (
                PARTITION BY category_en
                ORDER BY year, month
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_revenue
        FROM ecommerce_gold.monthly_revenue
    """,

    "vw_state_distribution": """
        SELECT
            customer_state,
            unique_customers,
            total_orders,
            total_revenue,
            avg_order_value,
            avg_review_score,
            ROUND(total_revenue / SUM(total_revenue) OVER () * 100, 2) AS revenue_share_pct
        FROM ecommerce_gold.state_distribution
    """,

    "vw_late_delivery_heatmap": """
        SELECT
            seller_city,
            seller_state,
            total_orders,
            late_orders,
            late_delivery_pct,
            avg_delay_days,
            CASE
                WHEN late_delivery_pct >= 50 THEN 'Critical'
                WHEN late_delivery_pct >= 25 THEN 'High Risk'
                WHEN late_delivery_pct >= 10 THEN 'Moderate'
                ELSE 'Good'
            END AS delivery_risk_level
        FROM ecommerce_gold.late_delivery_heatmap
    """,

    "vw_category_performance": """
        SELECT
            category_en,
            total_revenue,
            total_orders,
            avg_price,
            avg_review_score,
            ROUND(total_revenue / SUM(total_revenue) OVER () * 100, 2) AS revenue_share_pct,
            RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
        FROM ecommerce_gold.category_performance
    """,
}

# COMMAND ----------

# Create (or replace) all views
for view_name, view_sql in VIEW_DEFINITIONS.items():
    full_name = f"ecommerce_gold.{view_name}"
    spark.sql(f"DROP VIEW IF EXISTS {full_name}")
    spark.sql(f"CREATE VIEW {full_name} AS {view_sql}")
    print(f"  ✅ Created → {full_name}")

print("\n✅ ALL SQL VIEWS CREATED")
