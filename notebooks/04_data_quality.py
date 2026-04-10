# ============================================================
# NOTEBOOK 4 — Data Quality Checks
# Microsoft Fabric Retail Sales Lakehouse
# ============================================================
# Run AFTER Notebook 2 (Silver). Checks run before Gold build.
# Any FAIL prints a warning — review before proceeding.
# ============================================================

# ── CELL 1 ── Setup
from pyspark.sql import functions as F
from datetime import datetime

print(f"Data quality checks started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
results = []

def check(name, table, passed, failing=0, total=0):
    pct   = round((1 - failing/total)*100, 1) if total > 0 else 100.0
    status = "PASS" if passed else "FAIL"
    results.append({"check": name, "table": table, "status": status,
                     "failing": failing, "total": total, "pass_pct": pct})
    icon = "PASS" if passed else "FAIL"
    print(f"  [{icon}] {name:<45} {failing:>4} failing / {total:>5} total")
    return passed

# ── CELL 2 ── Load Silver tables
customers = spark.table("silver_customer_dim")
items     = spark.table("silver_item_dim")
stores    = spark.table("silver_store_dim")
time      = spark.table("silver_time_dim")
txns      = spark.table("silver_transactions")

# ── CELL 3 ── Customer checks
print("\n--- silver_customer_dim ---")
total = customers.count()
check("customer_id not null",     "customers", customers.filter(F.col("customer_id").isNull()).count()==0, customers.filter(F.col("customer_id").isNull()).count(), total)
check("customer_id unique",       "customers", customers.select("customer_id").distinct().count()==total, total-customers.select("customer_id").distinct().count(), total)
check("email not null",           "customers", customers.filter(F.col("email").isNull()).count()==0, customers.filter(F.col("email").isNull()).count(), total)
check("age between 18 and 100",   "customers", customers.filter((F.col("age")<18)|(F.col("age")>100)).count()==0, customers.filter((F.col("age")<18)|(F.col("age")>100)).count(), total)
check("loyalty_tier valid values","customers", customers.filter(~F.col("loyalty_tier").isin("Bronze","Silver","Gold","Platinum")).count()==0, customers.filter(~F.col("loyalty_tier").isin("Bronze","Silver","Gold","Platinum")).count(), total)

# ── CELL 4 ── Item checks
print("\n--- silver_item_dim ---")
total = items.count()
check("item_id unique",           "items", items.select("item_id").distinct().count()==total, total-items.select("item_id").distinct().count(), total)
check("unit_price > 0",           "items", items.filter(F.col("unit_price")<=0).count()==0, items.filter(F.col("unit_price")<=0).count(), total)
check("cost_price < unit_price",  "items", items.filter(F.col("cost_price")>=F.col("unit_price")).count()==0, items.filter(F.col("cost_price")>=F.col("unit_price")).count(), total)
check("category not null",        "items", items.filter(F.col("category").isNull()).count()==0, items.filter(F.col("category").isNull()).count(), total)

# ── CELL 5 ── Transaction checks
print("\n--- silver_transactions ---")
total = txns.count()
check("transaction_id unique",    "txns", txns.select("transaction_id").distinct().count()==total, total-txns.select("transaction_id").distinct().count(), total)
check("total_amount > 0",         "txns", txns.filter(F.col("total_amount")<=0).count()==0, txns.filter(F.col("total_amount")<=0).count(), total)
check("quantity > 0",             "txns", txns.filter(F.col("quantity")<=0).count()==0, txns.filter(F.col("quantity")<=0).count(), total)
check("discount_pct between 0-1", "txns", txns.filter((F.col("discount_pct")<0)|(F.col("discount_pct")>1)).count()==0, txns.filter((F.col("discount_pct")<0)|(F.col("discount_pct")>1)).count(), total)
check("customer_id ref integrity","txns",
    txns.join(customers.select("customer_id"), on="customer_id", how="left_anti").count()==0,
    txns.join(customers.select("customer_id"), on="customer_id", how="left_anti").count(), total)
check("item_id ref integrity",    "txns",
    txns.join(items.select("item_id"), on="item_id", how="left_anti").count()==0,
    txns.join(items.select("item_id"), on="item_id", how="left_anti").count(), total)
check("store_id ref integrity",   "txns",
    txns.join(stores.select("store_id"), on="store_id", how="left_anti").count()==0,
    txns.join(stores.select("store_id"), on="store_id", how="left_anti").count(), total)

# ── CELL 6 ── Summary
pass_count = sum(1 for r in results if r["status"]=="PASS")
fail_count = sum(1 for r in results if r["status"]=="FAIL")

print(f"\n========== DQ SUMMARY ==========")
print(f"  PASS : {pass_count}")
print(f"  FAIL : {fail_count}")
print(f"=================================")

if fail_count > 0:
    print("\nFailed checks — review before running Gold notebook:")
    for r in results:
        if r["status"] == "FAIL":
            print(f"  {r['table']}.{r['check']} — {r['failing']} failing rows")
else:
    print("\nAll checks passed. Safe to run Notebook 3 (Gold layer).")
