# Microsoft Fabric Retail Sales Analytics — End-to-End Lakehouse

> **Complete end-to-end data engineering project on Microsoft Fabric** — covering Data Factory pipelines, Lakehouse with Delta tables, PySpark notebooks (Bronze/Silver/Gold medallion architecture), SQL Analytics Endpoint, Power BI DirectLake reporting, and Data Activator alerts.

[![Microsoft Fabric](https://img.shields.io/badge/Microsoft_Fabric-Lakehouse-purple)]()
[![PySpark](https://img.shields.io/badge/PySpark-3.4-orange)]()
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-Tables-blue)]()
[![Power BI](https://img.shields.io/badge/Power_BI-DirectLake-yellow)]()

---

## Business Context

A retail chain operates 10 stores across the USA, selling products across 6 categories. The business needs a unified analytics platform to answer:

- Which products and categories drive the most revenue and profit?
- Which customers are at risk of churning?
- How does sales performance vary by region, channel, and store?
- What is the daily revenue trend and are we hitting our targets?

This Fabric Lakehouse solves it by unifying 5 source data files into a clean star schema, powering a live Power BI dashboard.

---

## Architecture

```
CSV Source Files (5 tables)
        │
        ▼
┌─────────────────────────────────────────────────────┐
│           Microsoft Fabric Data Factory              │
│   Copy Activity × 5  →  Trigger Notebooks           │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│              Fabric Lakehouse (OneLake)              │
│                                                      │
│  BRONZE          SILVER              GOLD            │
│  Raw Delta  →   Typed/Clean   →   Star Schema       │
│  5 tables        5 tables          4 tables          │
│                                                      │
│  bronze_*        silver_*          gold_fact_sales   │
│                                    gold_sales_daily  │
│                                    gold_by_category  │
│                                    gold_customer_sum │
└─────────────────────────────────────────────────────┘
        │                    │
        ▼                    ▼
┌──────────────┐   ┌──────────────────────┐
│  SQL Analytics│   │  Power BI DirectLake │
│  Endpoint    │   │  5-page Dashboard    │
│  8 views     │   │  No data copy!       │
│  T-SQL queries│  │  Sub-second queries  │
└──────────────┘   └──────────────────────┘
                            │
                            ▼
                   ┌─────────────────┐
                   │  Data Activator │
                   │  Revenue alerts │
                   └─────────────────┘
```

---

## Fabric Components Used

| Component | What it does in this project |
|---|---|
| Data Factory Pipeline | Orchestrates the full flow — copy CSVs, trigger notebooks |
| Lakehouse | OneLake storage — Delta tables for all 3 layers |
| Synapse Data Engineering (Notebooks) | PySpark Bronze/Silver/Gold transforms |
| SQL Analytics Endpoint | T-SQL views and analytics queries on Gold tables |
| Power BI DirectLake | Live dashboard — reads Delta tables directly, no import |
| Data Activator | Alert fires when daily revenue drops below $5,000 |

---

## Star Schema Data Model

```
                    ┌──────────────────┐
                    │  silver_time_dim │
                    │  date_id (PK)    │
                    └────────┬─────────┘
                             │
┌──────────────────┐         │         ┌──────────────────┐
│ silver_customer  │         │         │  silver_item_dim  │
│ customer_id (PK) ├─────────┤         │  item_id (PK)     │
└──────────────────┘         │         └────────┬──────────┘
                             │                  │
                    ┌────────▼──────────────────▼──────────┐
                    │          gold_fact_sales              │
                    │  transaction_id · customer_id         │
                    │  item_id · store_id · date_id         │
                    │  total_amount · profit_amount         │
                    │  quantity · discount_pct              │
                    └────────┬─────────────────────────────┘
                             │
                    ┌────────▼─────────┐
                    │ silver_store_dim  │
                    │ store_id (PK)     │
                    └──────────────────┘
```

---

## Repository Structure

```
fabric-retail-sales-lakehouse/
│
├── generate_data.py                    # Generates all 5 synthetic CSV files
│
├── data/                               # Your source CSV files
│   ├── customer_dim.csv
│   ├── item_dim.csv
│   ├── store_dim.csv
│   ├── time_dim.csv
│   └── Trans_dim.csv
│
├── notebooks/
│   ├── 01_bronze_ingestion.py          # Load CSVs → Bronze Delta tables
│   ├── 02_silver_transforms.py         # Clean, type, validate → Silver
│   ├── 03_gold_star_schema.py          # Star schema + aggregations → Gold
│   └── 04_data_quality.py             # 15+ DQ checks before Gold build
│
├── sql/
│   └── analytics_views.sql             # 5 views + 8 analytics queries
│
├── pipelines/
│   └── PL_RetailSales_Master.json      # Data Factory pipeline design
│
└── docs/
    └── PROJECT_COVER_SHEET.md
```

---

## How to Run in Microsoft Fabric

### Step 1 — Create workspace and Lakehouse
1. Go to app.fabric.microsoft.com
2. Create workspace: `RetailSalesWorkspace`
3. New Item → Lakehouse → name: `RetailSalesLakehouse`

### Step 2 — Upload CSV files
1. In the Lakehouse → Files section → New folder → name `raw`
2. Upload all 5 CSV files from `data/` folder into `Files/raw/`

### Step 3 — Create notebooks
1. New Item → Notebook
2. Copy code from each `.py` file in `notebooks/` folder
3. Attach each notebook to `RetailSalesLakehouse`
4. Run in order: 01 → 04 → 02 → 03

### Step 4 — Build the pipeline
1. New Item → Data Pipeline
2. Add Copy activities for each CSV file
3. Add Notebook activities triggered in sequence
4. Schedule: daily at 2:00 AM

### Step 5 — Connect Power BI
1. Open the Lakehouse → click "New Power BI report"
2. DirectLake mode connects automatically — no setup needed
3. Build visuals on top of `gold_fact_sales` and the Gold views

### Step 6 — Set up Data Activator alert
1. In Power BI report → set alert on daily revenue KPI card
2. Condition: value drops below 5000
3. Action: send email notification

---

## Power BI Dashboard Pages

| Page | Key visuals |
|---|---|
| Executive Overview | Revenue KPI, profit %, units sold, top category |
| Sales Trends | Monthly revenue line, day-of-week bar, seasonal patterns |
| Product Performance | Category bar chart, top 10 items table, margin heat map |
| Customer Insights | Loyalty tier donut, RFM segments, geographic map |
| Store Performance | Regional bar, store comparison table, channel mix |

---

## Interview Talking Points

**On Fabric:** "Microsoft Fabric gives you everything in one place — Data Factory, Lakehouse, Spark notebooks, SQL endpoint, and Power BI — all on top of OneLake. You don't stitch together separate services. Data lives in one place and every layer reads from the same Delta tables."

**On DirectLake:** "Power BI in DirectLake mode reads directly from the Delta Parquet files in OneLake — there's no data import, no scheduled refresh, no copy. The moment the Gold notebook writes new data, the dashboard reflects it. That's a fundamental shift from traditional Import mode."

**On medallion architecture:** "Bronze is raw — exactly what came from the source, never modified. Silver cleans and types the data. Gold is business-ready — my star schema fact table with customer, item, store and time dimensions joined in. Each layer has a clear responsibility."

**On Data Activator:** "Data Activator is Fabric's alerting layer. I set a rule: if daily revenue in the Power BI report drops below $5,000, fire an email to the ops team. It watches the live data — not a scheduled check, a continuous stream watch."

---

## Resume Bullets

> "Built an end-to-end retail sales analytics platform on Microsoft Fabric — implementing a Bronze/Silver/Gold medallion Lakehouse architecture with PySpark notebooks, Data Factory pipeline orchestration, star schema Gold layer, and Power BI DirectLake dashboard covering revenue, profitability, and customer segmentation across 2,000+ transactions"

> "Implemented Data Factory pipeline in Microsoft Fabric orchestrating CSV ingestion, PySpark data quality validation, and multi-layer Delta table transforms — reducing manual reporting effort from days to a fully automated daily refresh"

---

## Author

**Bhogya Swetha Malladi** · Data Engineer · New York, NY
Microsoft Fabric · Azure Databricks · Snowflake · PySpark · ADF
