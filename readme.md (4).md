# ✈️ US Domestic Flight Delay Analytics Platform

> **End-to-end Data Engineering project** — Raw BTS flight data → Medallion Lakehouse → Power BI Dashboard  
> Stack: `Amazon S3` · `Databricks DLT` · `Apache Spark` · `Delta Lake` · `Snowflake` · `Power BI`

---

## 📋 Project Overview

A production-grade Data Lakehouse pipeline built for **AeroMetrics Inc.** (simulated client) that:

- Ingests **~7 GB** of raw US domestic flight data (2021–2023, ~50M rows) from BTS TranStats
- Processes through **Bronze → Silver → Gold** Medallion Architecture using Databricks Delta Live Tables
- Applies comprehensive **Data Quality checks** at every layer
- Delivers clean KPIs to **Snowflake** for Power BI dashboard reporting

| Field | Detail |
|---|---|
| **Data Source** | Bureau of Transportation Statistics (BTS) TranStats |
| **Dataset Size** | ~7 GB raw, 36 months (2021–2023), ~50M rows |
| **Architecture** | Medallion (Bronze → Silver → Gold) Data Lakehouse |
| **Pipeline Type** | Databricks Delta Live Tables (DLT) |

---

## 🏗️ Architecture

```
BTS TranStats / Kaggle
        │
        ▼
  Amazon S3 (Landing)
  s3://bucket/flights/
  s3://bucket/raw/airports/
        │
        ▼
┌─────────────────────────────────────────┐
│         Databricks DLT Pipeline         │
│                                         │
│  BRONZE          SILVER          GOLD   │
│  ───────         ───────         ────   │
│  Raw parquet  →  Cleaned      →  KPIs  │
│  + DQ flags      Typed           Agg.  │
│                  Deduped               │
│                  Airport join          │
└─────────────────────────────────────────┘
        │
        ▼
   Snowflake (Serving Layer)
   STAGING → ANALYTICS → REPORTING views
        │
        ▼
   Power BI Dashboard
```

---

## 🛠️ Tech Stack

| Tool | Purpose | Cost |
|---|---|---|
| Amazon S3 | Raw file landing + Delta table storage | Free tier (5 GB) |
| Databricks Community Edition | Spark processing via DLT | Free |
| Apache Spark + Delta Lake | Distributed transforms, ACID tables | Included |
| Snowflake | Serving layer for BI | 30-day free trial ($400 credit) |
| Power BI Desktop | Dashboard & visualisation | Free forever |

---

## 📁 Repository Structure

```
flight-delay-analytics/
│
├── pipeline/
│   └── flight_delay_dlt_complete.py   # Complete DLT pipeline (Bronze + Silver + Gold)
│
├── snowflake/
│   └── phase4_snowflake_setup.sql     # DDL — databases, schemas, views
│
├── data/
│   └── airports.json                  # Airport reference (OpenFlights, 7698 airports)
│
└── README.md
```

---

## 🔄 Pipeline Layers

### 🥉 Bronze — `bronze_flightes`
- Reads raw Parquet files from S3 with explicit 36-column schema
- Adds `source_file` and `ingestion_time` metadata columns
- **Never modifies source data** — observation only

### 🥈 Silver — `silver_flights`
- Column renaming → clean snake_case
- `FlightDate` STRING → proper `DATE` + year/month/day_of_week columns
- HHMM integer (e.g. `1430`) → minutes since midnight (`870`)
- Boolean cast: `is_cancelled`, `is_diverted`, `is_dep_del15`, `is_arr_del15`
- Delay bucketing: `EARLY / ON_TIME / MINOR_DELAY / MAJOR_DELAY / SEVERE_DELAY / CANCELLED`
- Window-based deduplication (natural key, latest ingestion wins)
- Broadcast join with airport reference → `origin_lat/lon`, `dest_lat/lon`, timezone
- DQ flags: `dq_delay_cause_mismatch`, `dq_cancel_code_mismatch`, `dq_unknown_origin_airport`, `dq_unknown_dest_airport`

### 🥇 Gold — 4 Aggregated Tables

| Table | Description |
|---|---|
| `gold_airline_monthly_kpis` | On-time %, avg delay, P50/P95, cancellation rate, monthly rank per airline |
| `gold_route_annual_performance` | Normalised OD-pair route stats (ATL→LAX = LAX→ATL) |
| `gold_airport_monthly_performance` | Hub-level departure metrics with lat/lon for map visual |
| `gold_delay_cause_analysis` | Carrier / Weather / NAS / Security / Late Aircraft % breakdown |

---

## 📊 KPIs Delivered

| KPI | Definition |
|---|---|
| On-Time Rate (%) | Flights arriving ≤15 min late / Total flights × 100 |
| Avg Arrival Delay | Mean arrival delay (non-cancelled flights) |
| Cancellation Rate | Cancelled / Total × 100 |
| Delay Cause Mix | % of total delay minutes by cause |
| P95 Arrival Delay | 95th percentile — worst-case passenger experience |

---

## 🚀 Setup & Run

### Prerequisites
- AWS account (free tier)
- Databricks Community Edition account
- Snowflake 30-day free trial
- Power BI Desktop

### Step 1 — S3 Setup
```bash
# Create bucket structure
s3://your-bucket/flights/year=2021/month=01/
s3://your-bucket/raw/airports/airports.json
s3://your-bucket/delta/bronze/
s3://your-bucket/delta/silver/
s3://your-bucket/delta/gold/
```

### Step 2 — Databricks Cluster Setup
- Runtime: **13.x LTS** (Spark 3.4, Delta Lake 2.4)
- Set cluster environment variables:
```
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
```
- Install Maven library: `net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4`

### Step 3 — Download Data
**Option A (Recommended):** Kaggle
```bash
kaggle datasets download -d robikscube/flight-delay-dataset-20182022
```
**Option B:** BTS TranStats — https://www.transtats.bts.gov

### Step 4 — Run DLT Pipeline
- Create a new **Delta Live Tables** pipeline in Databricks
- Point to `pipeline/flight_delay_dlt_complete.py`
- Click **Start / Full Refresh**

### Step 5 — Snowflake Setup
Run `snowflake/phase4_snowflake_setup.sql` in Snowflake Worksheet

### Step 6 — Power BI
- Connect to Snowflake → `REPORTING` schema
- Load views: `vw_airline_scorecard`, `vw_delay_trend_monthly`, `vw_airport_performance_map`

---

## 💰 Cost Summary

| Platform | Tier | Estimated Cost |
|---|---|---|
| Amazon S3 | Free tier | $0–$2/month |
| Databricks | Community Edition | $0 |
| Snowflake | 30-day free trial | $5–$15 total |
| Power BI | Desktop (free forever) | $0 |
| **Total** | | **~$5–$20** |

---

## ⚠️ Data Engineering Notes

- BTS Parquet files use `int64` for all numeric columns — always use `LongType` or `DoubleType`, never `IntegerType` directly
- Airport reference (OpenFlights) contains `\N` as null marker for IATA codes — filter these out
- IATA codes may have hidden leading/trailing spaces — always `trim()` before joining
- Databricks Community Edition auto-terminates after 2 hours — split large Bronze loads into yearly batches
- Never hardcode AWS credentials in notebook cells — use cluster environment variables

---

## 📈 Expected Results (2021–2023)

| Metric | Approximate Value |
|---|---|
| Overall on-time arrival rate | 75–80% |
| Average arrival delay | 12–16 minutes |
| Average cancellation rate | 2.5–4% |
| Top delay cause | Late Aircraft (35–40%) |

---

*Built by [Your Name] · Data Engineering Project · Regex Software*
