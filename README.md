# Microsoft Fabric Retail Sales Lakehouse

End-to-end retail sales analytics platform built entirely on Microsoft Fabric — Data Factory pipeline orchestration, Lakehouse with Delta tables, PySpark notebooks, SQL Analytics Endpoint, and Power BI DirectLake reporting across a Bronze/Silver/Gold medallion architecture.

![Microsoft Fabric](https://img.shields.io/badge/Microsoft%20Fabric-0078D4?style=flat&logo=microsoft&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=flat&logo=powerbi&logoColor=black)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=flat)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)

---

## Business Context

A retail chain operating 10 stores across the United States needs a unified analytics platform that refreshes daily and answers questions across four domains — revenue performance, product profitability, customer behavior, and store-level comparisons.

Source data arrives as flat files from five upstream systems. The analytics team needs a single governed platform where analysts can write SQL queries, managers can view live Power BI dashboards, and operations teams receive automated alerts when revenue drops below target — without managing separate infrastructure for each layer.

This project demonstrates how Microsoft Fabric unifies data ingestion, transformation, warehousing, and visualization on a single platform backed by OneLake.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                      SOURCE FILES (5)                        │
│   customers · items · stores · Trans_dim · time_dim          │
└───────────────────────────────┬──────────────────────────────┘
                                │
                                ▼  Data Factory Pipeline
                                │  Copy activities → notebook triggers
                                │  Daily schedule · DQ gate
                                │
┌───────────────────────────────▼──────────────────────────────┐
│                  Fabric Lakehouse — OneLake                  │
│                                                              │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────┐  │
│  │   BRONZE     │   │   SILVER     │   │      GOLD        │  │
│  │              │   │              │   │                  │  │
│  │ Raw Delta    │→  │ Typed fields │→  │ gold_fact_sales  │  │
│  │ 5 tables     │   │ Derived cols │   │ gold_sales_daily │  │
│  │ Append-only  │   │ 15+ DQ checks│   │ gold_by_category │  │
│  │              │   │ DQ gate      │   │ gold_customer_   │  │
│  │              │   │              │   │ summary          │  │
│  └──────────────┘   └──────────────┘   └──────────────────┘  │
└────────────────────────────┬─────────────────────────────────┘
                             │                    │
                             ▼                    ▼
                   SQL Analytics           Power BI DirectLake
                   Endpoint               Reads Delta files directly
                   T-SQL views            No import · No refresh
                             │
                             ▼
                      Data Activator
                      Revenue alert rule
```

---

## Fabric Components

| Component | Role in this project |
|---|---|
| **Data Factory** | Orchestrates the full pipeline — copy source files, trigger notebooks in sequence, daily schedule at 02:00 |
| **Lakehouse** | Central storage on OneLake — all three medallion layers stored as Delta tables |
| **Synapse Data Engineering** | PySpark notebooks for Bronze ingestion, Silver transforms, Gold aggregation, and data quality validation |
| **SQL Analytics Endpoint** | T-SQL interface on top of Lakehouse Delta tables — 5 views for analyst self-service queries |
| **Power BI** | DirectLake mode — reads Parquet files directly from OneLake, always reflects latest data without scheduled refresh |
| **Data Activator** | Native Fabric alerting — monitors daily revenue KPI and fires an email alert when it drops below threshold |

---

## Data Model — Star Schema

Gold layer produces a proper star schema with one fact table and four dimension tables. All joins are resolved at write time so Power BI reads a single denormalized table.

```
          dim_customer          dim_item
               │                   │
               └─────────┬─────────┘
                         │
                  gold_fact_sales  ──────  dim_store
                         │
                    dim_time
```

**Gold tables**

| Table | Rows | Description |
|---|---|---|
| `gold_fact_sales` | 2,000 | Core fact table — transactions joined to all 4 dimensions |
| `gold_sales_daily` | ~730 | Daily revenue, profit, and transaction aggregations |
| `gold_sales_by_category` | 20 | Revenue and margin by product category with rank |
| `gold_customer_summary` | 200 | RFM metrics — orders, spend, recency, return rate per customer |

---

## Repository Structure

```
├── generate_data.py                  # Synthetic data generator — 2,960 rows across 5 files
│
├── notebooks/
│   ├── 01_bronze_ingestion.py       # Load CSV files → Bronze Delta tables
│   ├── 02_silver_transforms.py      # Type casting, derived fields, deduplication
│   ├── 03_gold_star_schema.py       # Star schema build + 4 Gold aggregation tables
│   └── 04_data_quality.py          # 15+ DQ checks — runs before Gold promotion
│
├── sql/
│   └── analytics_views.sql          # 5 T-SQL views + 8 business analytics queries
│
└── pipelines/
    └── PL_RetailSales_Master.json   # Data Factory pipeline definition
```

---

## Running in Fabric

**Prerequisites:** Microsoft Fabric workspace with Lakehouse capacity (free trial available at fabric.microsoft.com)

```
Step 1 — Create Lakehouse
  Workspace → New item → Lakehouse → name: RetailSalesLakehouse

Step 2 — Upload source files
  Lakehouse → Files → New folder (name: raw)
  Upload all files from data/ into Files/raw/

Step 3 — Import notebooks
  New item → Notebook → paste code from notebooks/
  Attach each notebook to RetailSalesLakehouse

Step 4 — Run in order
  01_bronze_ingestion → 04_data_quality → 02_silver_transforms → 03_gold_star_schema

Step 5 — Connect Power BI
  Lakehouse → New Power BI report
  DirectLake mode activates automatically — no gateway or import required
```

---

## Synthetic Dataset

| File | Rows | Contents |
|---|---|---|
| customer_dim.csv | 200 | Demographics, loyalty tier, join date |
| item_dim.csv | 20 | Product catalog with cost price and unit price |
| store_dim.csv | 10 | Store locations across 5 US regions |
| Trans_dim.csv | 2,000 | Sales transactions — channel, payment method, return flag |
| time_dim.csv | 730 | Two years of daily date dimension records |

All data is synthetically generated — no real retail or customer data.

---

## Key Engineering Decisions

| Decision | Rationale |
|---|---|
| DirectLake over Import mode | Power BI reads Delta Parquet files directly from OneLake — no scheduled refresh, no data duplication, always current |
| DQ gate before Gold | 15+ checks run after Silver completes — any failure blocks the Gold write, preventing bad data from reaching dashboards |
| Star schema in Gold layer | All dimension joins resolved at write time — Power BI queries a single denormalized table with no runtime joins |
| Data Activator for alerting | Native Fabric layer — no additional infrastructure. Monitors live dashboard data and fires alerts on threshold breach |
| Single Lakehouse for all layers | Bronze, Silver, and Gold stored in one OneLake location — simplifies access control, lineage, and cost management |
