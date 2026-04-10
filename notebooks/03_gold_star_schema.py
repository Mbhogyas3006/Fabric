# ============================================================
# NOTEBOOK 3 — Gold Layer: Star Schema Fact Table
# Microsoft Fabric Retail Sales Lakehouse
# ============================================================
# Run AFTER Notebook 2 completes successfully.
# Produces:
#   gold_fact_sales         — main fact table (2,000 rows)
#   gold_sales_daily        — daily aggregated KPIs
#   gold_sales_by_category  — category performance
#   gold_customer_summary   — customer-level RFM metrics
# ============================================================

# ── CELL 1 ── Setup
from pyspark.sql import functions as F, Window
from datetime import datetime

print(f"Gold layer build started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ── CELL 2 ── Read Silver tables
txns      = spark.table("silver_transactions")
customers = spark.table("silver_customer_dim")
items     = spark.table("silver_item_dim")
stores    = spark.table("silver_store_dim")
time      = spark.table("silver_time_dim")

print("Silver tables loaded:")
for name, df in [("transactions",txns),("customers",customers),
                  ("items",items),("stores",stores),("time",time)]:
    print(f"  {name:<20} {df.count():>5,} rows")

# ── CELL 3 ── Gold Fact Sales (denormalized star schema join)
print("\nBuilding gold_fact_sales...")

gold_fact = (
    txns
    .join(customers.select("customer_id","full_name","city","state",
                            "loyalty_tier","age_band","gender"),
          on="customer_id", how="left")
    .join(items.select("item_id","item_name","category","sub_category",
                        "price_band","margin_pct"),
          on="item_id", how="left")
    .join(stores.select("store_id","store_name","store_type","region"),
          on="store_id", how="left")
    .join(time.select("date_id","full_date","day_name","month_name",
                       "month","quarter","year","is_weekend","is_holiday"),
          on="date_id", how="left")
    .select(
        # Keys
        "transaction_id","customer_id","item_id","store_id","date_id",
        # Customer attrs
        "full_name","city","state","loyalty_tier","age_band","gender",
        # Item attrs
        "item_name","category","sub_category","price_band",
        # Store attrs
        "store_name","store_type","region",
        # Time attrs
        "full_date","day_name","month_name","month","quarter","year",
        "is_weekend","is_holiday",
        # Measures
        "quantity","unit_price","discount_pct","discount_amount",
        "gross_amount","net_amount","tax_amount","total_amount",
        "profit_amount","payment_method","channel",
        "return_flag","transaction_status",
        F.current_timestamp().alias("_gold_created_at")
    )
)

(
    gold_fact.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold_fact_sales")
)
print(f"  gold_fact_sales: {gold_fact.count():,} rows")

# ── CELL 4 ── Gold Daily Sales Aggregation (for trend charts)
print("\nBuilding gold_sales_daily...")

gold_daily = (
    gold_fact
    .groupBy("full_date", "year", "month", "month_name", "quarter", "is_weekend")
    .agg(
        F.count("transaction_id")               .alias("transaction_count"),
        F.sum("total_amount")                   .alias("total_revenue"),
        F.sum("profit_amount")                  .alias("total_profit"),
        F.sum("quantity")                       .alias("total_units_sold"),
        F.avg("total_amount")                   .alias("avg_order_value"),
        F.countDistinct("customer_id")          .alias("unique_customers"),
        F.sum(F.col("return_flag").cast("int")) .alias("return_count"),
        F.sum("discount_amount")                .alias("total_discounts"),
    )
    .withColumn("profit_margin_pct",
        F.round(F.col("total_profit") / F.col("total_revenue") * 100, 2))
    .withColumn("return_rate_pct",
        F.round(F.col("return_count") / F.col("transaction_count") * 100, 2))
    .orderBy("full_date")
)

(
    gold_daily.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold_sales_daily")
)
print(f"  gold_sales_daily: {gold_daily.count():,} rows")

# ── CELL 5 ── Gold Category Performance
print("\nBuilding gold_sales_by_category...")

gold_category = (
    gold_fact
    .groupBy("category", "sub_category", "price_band")
    .agg(
        F.sum("total_amount")          .alias("total_revenue"),
        F.sum("profit_amount")         .alias("total_profit"),
        F.sum("quantity")              .alias("units_sold"),
        F.count("transaction_id")      .alias("transaction_count"),
        F.avg("total_amount")          .alias("avg_order_value"),
        F.avg("discount_pct")          .alias("avg_discount_pct"),
        F.countDistinct("customer_id") .alias("unique_customers"),
    )
    .withColumn("profit_margin_pct",
        F.round(F.col("total_profit") / F.col("total_revenue") * 100, 2))
    .withColumn("revenue_rank",
        F.rank().over(Window.orderBy(F.col("total_revenue").desc())))
    .orderBy("revenue_rank")
)

(
    gold_category.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold_sales_by_category")
)
print(f"  gold_sales_by_category: {gold_category.count():,} rows")

# ── CELL 6 ── Gold Customer Summary (RFM metrics)
print("\nBuilding gold_customer_summary...")

gold_customers = (
    gold_fact
    .groupBy("customer_id","full_name","city","state","loyalty_tier","age_band","gender")
    .agg(
        F.count("transaction_id")               .alias("total_orders"),
        F.sum("total_amount")                   .alias("total_spend"),
        F.avg("total_amount")                   .alias("avg_order_value"),
        F.max("full_date")                      .alias("last_purchase_date"),
        F.min("full_date")                      .alias("first_purchase_date"),
        F.sum("quantity")                       .alias("total_units"),
        F.sum(F.col("return_flag").cast("int")) .alias("total_returns"),
        F.countDistinct("category")             .alias("categories_bought"),
    )
    .withColumn("days_since_purchase",
        F.datediff(F.current_date(), F.col("last_purchase_date")))
    .withColumn("customer_tenure_days",
        F.datediff(F.col("last_purchase_date"), F.col("first_purchase_date")))
    .withColumn("return_rate_pct",
        F.round(F.col("total_returns") / F.col("total_orders") * 100, 2))
    # RFM segmentation
    .withColumn("rfm_segment",
        F.when((F.col("total_orders") >= 5) & (F.col("total_spend") >= 500), "Champion")
         .when((F.col("total_orders") >= 3) & (F.col("days_since_purchase") <= 60), "Loyal")
         .when(F.col("days_since_purchase") > 180, "At Risk")
         .otherwise("Regular"))
)

(
    gold_customers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold_customer_summary")
)
print(f"  gold_customer_summary: {gold_customers.count():,} rows")

# ── CELL 7 ── Final audit
print("\n========== GOLD AUDIT ==========")
for t in ["fact_sales","sales_daily","sales_by_category","customer_summary"]:
    c = spark.table(f"gold_{t}").count()
    print(f"  gold_{t:<25} {c:>5,} rows")

print("\nSample — Top 5 revenue categories:")
spark.table("gold_sales_by_category") \
     .select("category","total_revenue","profit_margin_pct","units_sold") \
     .limit(5).display()
