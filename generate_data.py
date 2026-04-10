"""
Synthetic Data Generator — Microsoft Fabric Retail Sales Lakehouse
Generates star schema datasets matching the project CSV files.
Run: python generate_data.py
Output: data/*.csv
"""

import csv, random, uuid
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)
OUT = Path(__file__).parent / "data"
OUT.mkdir(exist_ok=True)

# ── helpers ───────────────────────────────────────────────────────────────────
FIRST  = ["James","Mary","Robert","Patricia","John","Jennifer","Michael","Linda","Wei","Priya","Carlos","Sofia","Amir","Yuki","Fatima"]
LAST   = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Patel","Kim","Nguyen","Chen","Singh","Lee","Martinez"]
CITIES = ["New York","Los Angeles","Chicago","Houston","Phoenix","Philadelphia","San Antonio","San Diego","Dallas","San Jose"]
STATES = ["NY","CA","IL","TX","AZ","PA","TX","CA","TX","CA"]
ITEMS  = [
    ("Laptop","Electronics",799.99),("Smartphone","Electronics",599.99),
    ("Headphones","Electronics",149.99),("T-Shirt","Apparel",24.99),
    ("Jeans","Apparel",49.99),("Running Shoes","Footwear",89.99),
    ("Coffee Maker","Appliances",59.99),("Blender","Appliances",39.99),
    ("Desk Chair","Furniture",199.99),("Bookshelf","Furniture",129.99),
    ("Novel — Fiction","Books",14.99),("Cookbook","Books",24.99),
    ("Yoga Mat","Sports",29.99),("Dumbbells Set","Sports",79.99),
    ("Face Cream","Beauty",34.99),("Shampoo","Beauty",12.99),
    ("Vitamin C","Health",19.99),("Protein Powder","Health",44.99),
    ("Notebook","Stationery",8.99),("Pen Set","Stationery",15.99),
]
STORE_NAMES = [
    "Downtown Flagship","Mall of America","Westside Plaza","Airport Express",
    "University District","Harbor View","Uptown","Eastside","Tech Corridor","Suburban South",
]
STORE_TYPES = ["Flagship","Express","Standard","Premium"]

def rand_date(start, end):
    return start + timedelta(days=random.randint(0, (end-start).days))

# ── 1. customer_dim ───────────────────────────────────────────────────────────
customers = []
for i in range(1, 201):
    idx   = random.randint(0, len(CITIES)-1)
    fn, ln = random.choice(FIRST), random.choice(LAST)
    customers.append({
        "customer_id":   i,
        "first_name":    fn,
        "last_name":     ln,
        "full_name":     f"{fn} {ln}",
        "email":         f"{fn.lower()}.{ln.lower()}{i}@email.com",
        "phone":         f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
        "city":          CITIES[idx],
        "state":         STATES[idx],
        "country":       "USA",
        "age":           random.randint(18, 75),
        "gender":        random.choice(["Male","Female","Other"]),
        "loyalty_tier":  random.choices(["Bronze","Silver","Gold","Platinum"], weights=[.5,.3,.15,.05])[0],
        "join_date":     rand_date(datetime(2018,1,1), datetime(2023,12,31)).strftime("%Y-%m-%d"),
        "is_active":     random.choices(["Y","N"], weights=[.9,.1])[0],
    })
with open(OUT/"customer_dim.csv","w",newline="") as f:
    w = csv.DictWriter(f, fieldnames=customers[0].keys()); w.writeheader(); w.writerows(customers)
print(f"customer_dim.csv   → {len(customers):,} rows")

# ── 2. item_dim ───────────────────────────────────────────────────────────────
items = []
for i, (name, cat, price) in enumerate(ITEMS, 1):
    items.append({
        "item_id":        i,
        "item_name":      name,
        "category":       cat,
        "sub_category":   f"{cat} — Type {random.randint(1,3)}",
        "brand":          random.choice(["BrandA","BrandB","BrandC","BrandD","BrandE"]),
        "unit_price":     price,
        "cost_price":     round(price * random.uniform(0.45, 0.65), 2),
        "weight_kg":      round(random.uniform(0.1, 15.0), 2),
        "is_available":   "Y",
        "launch_date":    rand_date(datetime(2018,1,1), datetime(2023,1,1)).strftime("%Y-%m-%d"),
        "supplier_id":    random.randint(1, 20),
    })
with open(OUT/"item_dim.csv","w",newline="") as f:
    w = csv.DictWriter(f, fieldnames=items[0].keys()); w.writeheader(); w.writerows(items)
print(f"item_dim.csv       → {len(items):,} rows")

# ── 3. store_dim ──────────────────────────────────────────────────────────────
stores = []
for i, name in enumerate(STORE_NAMES, 1):
    idx = random.randint(0, len(CITIES)-1)
    stores.append({
        "store_id":       i,
        "store_name":     name,
        "store_type":     random.choice(STORE_TYPES),
        "city":           CITIES[idx],
        "state":          STATES[idx],
        "country":        "USA",
        "region":         random.choice(["Northeast","Southeast","Midwest","West","Southwest"]),
        "sq_footage":     random.randint(2000, 20000),
        "open_date":      rand_date(datetime(2010,1,1), datetime(2020,1,1)).strftime("%Y-%m-%d"),
        "manager_name":   f"{random.choice(FIRST)} {random.choice(LAST)}",
        "phone":          f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
        "is_active":      "Y",
    })
with open(OUT/"store_dim.csv","w",newline="") as f:
    w = csv.DictWriter(f, fieldnames=stores[0].keys()); w.writeheader(); w.writerows(stores)
print(f"store_dim.csv      → {len(stores):,} rows")

# ── 4. time_dim ───────────────────────────────────────────────────────────────
times = []
start_date = datetime(2022, 1, 1)
for i in range(730):  # 2 years
    d = start_date + timedelta(days=i)
    times.append({
        "date_id":        int(d.strftime("%Y%m%d")),
        "full_date":      d.strftime("%Y-%m-%d"),
        "day":            d.day,
        "month":          d.month,
        "month_name":     d.strftime("%B"),
        "quarter":        (d.month - 1) // 3 + 1,
        "year":           d.year,
        "week_of_year":   d.isocalendar()[1],
        "day_of_week":    d.weekday() + 1,
        "day_name":       d.strftime("%A"),
        "is_weekend":     "Y" if d.weekday() >= 5 else "N",
        "is_holiday":     "Y" if (d.month == 12 and d.day == 25) or (d.month == 1 and d.day == 1) else "N",
        "fiscal_quarter": f"FY{d.year}Q{(d.month-1)//3+1}",
    })
with open(OUT/"time_dim.csv","w",newline="") as f:
    w = csv.DictWriter(f, fieldnames=times[0].keys()); w.writeheader(); w.writerows(times)
print(f"time_dim.csv       → {len(times):,} rows")

# ── 5. Trans_dim (fact table) ─────────────────────────────────────────────────
transactions = []
valid_dates = [t["date_id"] for t in times]
for i in range(1, 2001):
    item      = random.choice(items)
    qty       = random.randint(1, 5)
    unit_price = item["unit_price"]
    discount   = random.choices([0, 0.05, 0.10, 0.15, 0.20], weights=[.5,.2,.15,.1,.05])[0]
    gross      = round(unit_price * qty, 2)
    disc_amt   = round(gross * discount, 2)
    net        = round(gross - disc_amt, 2)
    tax        = round(net * 0.08, 2)
    total      = round(net + tax, 2)
    transactions.append({
        "transaction_id":   i,
        "customer_id":      random.choice(customers)["customer_id"],
        "item_id":          item["item_id"],
        "store_id":         random.choice(stores)["store_id"],
        "date_id":          random.choice(valid_dates),
        "quantity":         qty,
        "unit_price":       unit_price,
        "discount_pct":     discount,
        "discount_amount":  disc_amt,
        "gross_amount":     gross,
        "net_amount":       net,
        "tax_amount":       tax,
        "total_amount":     total,
        "payment_method":   random.choice(["Credit Card","Debit Card","Cash","Digital Wallet"]),
        "channel":          random.choice(["In-Store","Online","Mobile App"]),
        "return_flag":      random.choices(["N","Y"], weights=[.93,.07])[0],
        "transaction_status": random.choices(["Completed","Refunded","Pending"], weights=[.92,.05,.03])[0],
    })
with open(OUT/"Trans_dim.csv","w",newline="") as f:
    w = csv.DictWriter(f, fieldnames=transactions[0].keys()); w.writeheader(); w.writerows(transactions)
print(f"Trans_dim.csv      → {len(transactions):,} rows")

total = len(customers)+len(items)+len(stores)+len(times)+len(transactions)
print(f"\nTotal rows: {total:,} across 5 files")
