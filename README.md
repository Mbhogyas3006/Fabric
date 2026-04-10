# Microsoft Fabric Retail Sales Analytics — End-to-End Lakehouse

A complete end-to-end retail sales analytics platform built on Microsoft Fabric — covering Data Factory pipelines, Lakehouse with Delta tables, PySpark notebooks, SQL Analytics Endpoint, and Power BI DirectLake reporting.

---

## Overview

This project demonstrates how Microsoft Fabric consolidates traditionally separate services — data ingestion, transformation, warehousing, and visualization — into a unified platform on top of OneLake. The use case is a retail chain with stores across the USA, requiring a daily analytics refresh covering revenue, profitability, customer segmentation, and store performance.

---

## Architecture

```
Source CSV Files
customers · items · stores · transactions · time
        │
        ▼  Data Factory Pipeline (daily schedule)
        │
┌──────────────────────────────────────────┐
│         Fabric Lakehouse (OneLake)       |
│                                          │
│  Bronze       Silver          Gold       │
│  Raw Delta → Typed/Clean  → Star Schema  │
│  5 tables     5 tables       4 tables    │
└──────────────────────────────────────────┘
        │                    │
        ▼                    ▼
  SQL Analytics         Power BI
  Endpoint              DirectLake
  T-SQL views           No data copy
        │
        ▼
  Data Activator
  Revenue alerts
```

---

## Fabric Components

| Component | Role in this project |
|---|---|
| Data Factory | Orchestrates CSV ingestion and notebook triggers |
| Lakehouse | OneLake Delta storage — Bronze, Silver, Gold layers |
| Synapse Data Engineering | PySpark notebooks for all transformations |
| SQL Analytics Endpoint | T-SQL views for ad-hoc analyst queries |
| Power BI | DirectLake dashboard — reads Delta files directly |
| Data Activator | Alert when daily revenue drops below threshold |

---

## Data Model — Star Schema

```
silver_time_dim ─────────────────────────┐
                                          │
silver_customer_dim ──┐                  │
                      ├── gold_fact_sales ┤
silver_item_dim ──────┤                  │
                      │                  │
silver_store_dim ─────┘                  │
                                         │
                    ─────────────────────┘
```

---

## Repository Structure

```
├── generate_data.py                   # Synthetic data generator
├── notebooks/
│   ├── 01_bronze_ingestion.py        # CSV → Bronze Delta tables
│   ├── 02_silver_transforms.py       # Typing, derived fields
│   ├── 03_gold_star_schema.py        # Star schema + aggregations
│   └── 04_data_quality.py           # 15+ DQ checks
├── sql/
│   └── analytics_views.sql           # 5 views + 8 analytics queries
└── pipelines/
    └── PL_RetailSales_Master.json    # Data Factory pipeline design
```

---

## Quick Start — Running in Fabric

1. Create a Fabric workspace and Lakehouse named `RetailSalesLakehouse`
2. Upload CSV files from `data/` to `Files/raw/` in the Lakehouse
3. Import notebooks from `notebooks/` — attach to `RetailSalesLakehouse`
4. Run in order: `01` → `04` → `02` → `03`
5. Open Lakehouse → New Power BI report (DirectLake connects automatically)

---

## Synthetic Dataset

| Table | Rows |
|---|---|
| Customers | 200 |
| Items | 20 |
| Stores | 10 |
| Transactions | 2,000 |
| Time | 730 |

---

## Key Engineering Decisions

- **DirectLake over Import** — Power BI reads Delta Parquet files directly from OneLake, eliminating scheduled refresh and data duplication
- **Medallion within Fabric** — Bronze/Silver/Gold pattern applied inside a single Lakehouse, keeping the architecture consistent with enterprise standards
- **Data Activator for alerting** — native Fabric alerting layer replaces custom Logic Apps or Azure Monitor setups
