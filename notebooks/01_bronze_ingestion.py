# ============================================================
# NOTEBOOK 1 — Bronze Layer: Raw Ingestion
# Microsoft Fabric Retail Sales Lakehouse
# ============================================================
# HOW TO USE IN FABRIC:
# 1. Open your Fabric workspace
# 2. Click New → Notebook
# 3. Copy each cell below into a new code cell
# 4. Attach notebook to "RetailSalesLakehouse"
# 5. Run cells top to bottom
# ============================================================

# ── CELL 1 ── Setup
from pyspark.sql import functions as F
from datetime import datetime

BATCH_TS = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"Bronze ingestion started: {BATCH_TS}")

# ── CELL 2 ── Helper function
def load_csv_to_bronze(file_name, table_name):
    """
    Read CSV from Lakehouse Files/raw/ folder.
    Stamp with metadata columns.
    Write as Delta table (bronze_ prefix).
    """
    print(f"\nIngesting {file_name}...")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"Files/raw/{file_name}")
    )

    df = (
        df
        .withColumn("_source_file",  F.lit(file_name))
        .withColumn("_ingested_at",  F.lit(BATCH_TS))
        .withColumn("_batch_date",   F.current_date())
    )

    row_count = df.count()

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"bronze_{table_name}")
    )

    print(f"  Rows loaded : {row_count:,}")
    print(f"  Delta table : bronze_{table_name}")
    return row_count

# ── CELL 3 ── Load all 5 source files
counts = {}
counts["customer_dim"]  = load_csv_to_bronze("customer_dim.csv",  "customer_dim")
counts["item_dim"]      = load_csv_to_bronze("item_dim.csv",       "item_dim")
counts["store_dim"]     = load_csv_to_bronze("store_dim.csv",      "store_dim")
counts["time_dim"]      = load_csv_to_bronze("time_dim.csv",       "time_dim")
counts["transactions"]  = load_csv_to_bronze("Trans_dim.csv",      "transactions")

# ── CELL 4 ── Audit summary
print("\n========== BRONZE AUDIT ==========")
for t, c in counts.items():
    print(f"  bronze_{t:<20} {c:>5,} rows")
print(f"  {'TOTAL':<28} {sum(counts.values()):>5,} rows")
print("===================================")

# ── CELL 5 ── Show sample from transactions
print("\nSample: bronze_transactions (top 5 rows)")
spark.table("bronze_transactions").limit(5).display()
