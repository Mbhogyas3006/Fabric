# ============================================================
# NOTEBOOK 2 — Silver Layer: Clean, Type, Validate
# Microsoft Fabric Retail Sales Lakehouse
# ============================================================
# Run AFTER Notebook 1 completes successfully.
# ============================================================

# ── CELL 1 ── Setup
from pyspark.sql import functions as F, Window
from datetime import datetime

print(f"Silver transforms started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ── CELL 2 ── Silver Customers
print("\nProcessing silver_customer_dim...")

silver_customers = (
    spark.table("bronze_customer_dim")
    # Type casting
    .withColumn("customer_id",  F.col("customer_id").cast("integer"))
    .withColumn("age",          F.col("age").cast("integer"))
    .withColumn("join_date",    F.to_date("join_date", "yyyy-MM-dd"))
    # Standardise
    .withColumn("full_name",    F.initcap(F.trim(F.col("full_name"))))
    .withColumn("email",        F.lower(F.trim(F.col("email"))))
    .withColumn("country",      F.upper(F.trim(F.col("country"))))
    .withColumn("is_active",    F.col("is_active").cast("boolean"))
    # Derived fields
    .withColumn("age_band",
        F.when(F.col("age") < 25, "18-24")
         .when(F.col("age") < 35, "25-34")
         .when(F.col("age") < 45, "35-44")
         .when(F.col("age") < 55, "45-54")
         .otherwise("55+"))
    .withColumn("tenure_years",
        F.round(F.datediff(F.current_date(), F.col("join_date")) / 365.25, 1))
    # Drop bronze metadata
    .drop("_source_file", "_ingested_at", "_batch_date")
    .withColumn("_updated_at", F.current_timestamp())
)

(
    silver_customers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_customer_dim")
)
print(f"  silver_customer_dim: {silver_customers.count():,} rows")

# ── CELL 3 ── Silver Items
print("\nProcessing silver_item_dim...")

silver_items = (
    spark.table("bronze_item_dim")
    .withColumn("item_id",      F.col("item_id").cast("integer"))
    .withColumn("unit_price",   F.col("unit_price").cast("double"))
    .withColumn("cost_price",   F.col("cost_price").cast("double"))
    .withColumn("weight_kg",    F.col("weight_kg").cast("double"))
    .withColumn("launch_date",  F.to_date("launch_date", "yyyy-MM-dd"))
    .withColumn("is_available", F.col("is_available").cast("boolean"))
    .withColumn("margin_pct",
        F.round((F.col("unit_price") - F.col("cost_price")) / F.col("unit_price") * 100, 2))
    .withColumn("price_band",
        F.when(F.col("unit_price") < 25,  "Budget")
         .when(F.col("unit_price") < 100, "Mid-Range")
         .when(F.col("unit_price") < 300, "Premium")
         .otherwise("Luxury"))
    .drop("_source_file", "_ingested_at", "_batch_date")
    .withColumn("_updated_at", F.current_timestamp())
)

(
    silver_items.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_item_dim")
)
print(f"  silver_item_dim: {silver_items.count():,} rows")

# ── CELL 4 ── Silver Stores
print("\nProcessing silver_store_dim...")

silver_stores = (
    spark.table("bronze_store_dim")
    .withColumn("store_id",    F.col("store_id").cast("integer"))
    .withColumn("sq_footage",  F.col("sq_footage").cast("integer"))
    .withColumn("open_date",   F.to_date("open_date", "yyyy-MM-dd"))
    .withColumn("is_active",   F.col("is_active").cast("boolean"))
    .withColumn("store_age_years",
        F.round(F.datediff(F.current_date(), F.col("open_date")) / 365.25, 1))
    .drop("_source_file", "_ingested_at", "_batch_date")
    .withColumn("_updated_at", F.current_timestamp())
)

(
    silver_stores.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_store_dim")
)
print(f"  silver_store_dim: {silver_stores.count():,} rows")

# ── CELL 5 ── Silver Time
print("\nProcessing silver_time_dim...")

silver_time = (
    spark.table("bronze_time_dim")
    .withColumn("date_id",      F.col("date_id").cast("integer"))
    .withColumn("full_date",    F.to_date("full_date", "yyyy-MM-dd"))
    .withColumn("day",          F.col("day").cast("integer"))
    .withColumn("month",        F.col("month").cast("integer"))
    .withColumn("quarter",      F.col("quarter").cast("integer"))
    .withColumn("year",         F.col("year").cast("integer"))
    .withColumn("week_of_year", F.col("week_of_year").cast("integer"))
    .withColumn("day_of_week",  F.col("day_of_week").cast("integer"))
    .withColumn("is_weekend",   F.col("is_weekend").cast("boolean"))
    .withColumn("is_holiday",   F.col("is_holiday").cast("boolean"))
    .drop("_source_file", "_ingested_at", "_batch_date")
    .withColumn("_updated_at", F.current_timestamp())
)

(
    silver_time.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_time_dim")
)
print(f"  silver_time_dim: {silver_time.count():,} rows")

# ── CELL 6 ── Silver Transactions
print("\nProcessing silver_transactions...")

silver_txns = (
    spark.table("bronze_transactions")
    .withColumn("transaction_id", F.col("transaction_id").cast("integer"))
    .withColumn("customer_id",    F.col("customer_id").cast("integer"))
    .withColumn("item_id",        F.col("item_id").cast("integer"))
    .withColumn("store_id",       F.col("store_id").cast("integer"))
    .withColumn("date_id",        F.col("date_id").cast("integer"))
    .withColumn("quantity",       F.col("quantity").cast("integer"))
    .withColumn("unit_price",     F.col("unit_price").cast("double"))
    .withColumn("discount_pct",   F.col("discount_pct").cast("double"))
    .withColumn("discount_amount",F.col("discount_amount").cast("double"))
    .withColumn("gross_amount",   F.col("gross_amount").cast("double"))
    .withColumn("net_amount",     F.col("net_amount").cast("double"))
    .withColumn("tax_amount",     F.col("tax_amount").cast("double"))
    .withColumn("total_amount",   F.col("total_amount").cast("double"))
    .withColumn("return_flag",    F.col("return_flag").cast("boolean"))
    # Derived
    .withColumn("profit_amount",
        F.round(F.col("net_amount") - (F.col("quantity") * F.col("unit_price") * 0.55), 2))
    .drop("_source_file", "_ingested_at", "_batch_date")
    .withColumn("_updated_at", F.current_timestamp())
)

(
    silver_txns.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_transactions")
)
print(f"  silver_transactions: {silver_txns.count():,} rows")

# ── CELL 7 ── Audit
print("\n========== SILVER AUDIT ==========")
for t in ["customer_dim","item_dim","store_dim","time_dim","transactions"]:
    c = spark.table(f"silver_{t}").count()
    print(f"  silver_{t:<22} {c:>5,} rows")
print("===================================")
