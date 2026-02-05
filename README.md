# Eurostat Lakehouse (Bronze → Silver → Gold) + Airflow + Databricks SQL (Free)

## Overview
This repository implements an end-to-end **Data Engineering** project using **Eurostat Structural Business Statistics (SBS)** datasets.
The pipeline follows a **Lakehouse-style architecture** (Bronze → Silver → Gold), orchestrated with **Apache Airflow** locally and optionally published to **Databricks SQL (Free)** for querying and dashboards.

**Key goals**
- Build a reproducible pipeline with clear layers (Bronze/Silver/Gold)
- Handle real-world messy Eurostat files (TSV, flags, wide-year columns)
- Orchestrate everything with Airflow (DAG + task dependencies)
- Keep it runnable locally, and optionally mirror outputs into Databricks SQL

## Architecture

### Data Flow
Raw Eurostat TSV (local)
   ↓
🥉 Bronze (Parquet, minimal changes + metadata)
   ↓
🥈 Silver (normalized long format + cleaned values)
   ↓
🥇 Gold (data marts: rankings, time series, YoY)
   ↓
(Optional) Publish to Databricks SQL (Free) as tables or uploaded query-ready outputs

### Layers
**Bronze**
- Reads TSV exactly as downloaded
- Adds metadata: `source_file`, `ingested_at`
- Stores in `data/bronze/` as Parquet

**Silver**
- Splits dimensions from Eurostat: `freq,nace_r2,indic_sbs,geo`
- Converts wide years (2021, 2022, 2023...) → long format (`year`, `value_raw`)
- Cleans Eurostat flags (examples: `:`, `e`, `b`, `p`)
- Produces consistent schema and types
- Stores in `data/silver/` as Parquet

**Gold**
Creates analytical marts:
1) `gold_top_countries` — top N countries by indicator/year
2) `gold_timeseries` — time series by geo + indicator
3) `gold_yoy` — year-over-year growth per geo + indicator
Stores in `data/gold/` (Parquet + CSV)


## Repository Structure

eurostat-lakehouse/
├─ data/
│  ├─ raw/              # Eurostat TSV files (not committed)
│  ├─ bronze/           # Bronze Parquet
│  ├─ silver/           # Silver Parquet
│  └─ gold/             # Gold marts (Parquet + CSV)
├─ src/
│  ├─ bronze_ingest.py
│  ├─ silver_transform.py
│  ├─ gold_marts.py
│  └─ utils.py
├─ airflow/
│  ├─ dags/
│  │  └─ eurostat_lakehouse_dag.py
│  └─ docker-compose.yml
├─ outputs/
│  └─ checks/           # quality checks outputs
├─ requirements.txt
├─ .gitignore
└─ README.md

---

## Requirements
- Python 3.10+ (recommended 3.10/3.11)
- VS Code
- Docker Desktop (recommended for Airflow via docker-compose)

Python packages:
- pandas
- pyarrow
- duckdb (optional for fast local SQL checks)
- deltalake (optional for Delta writes locally)
- apache-airflow (only if running without Docker)


## How to Run (Local)

### Step 1 — Place raw datasets
Put your Eurostat TSV files into:
`data/raw/`

Example:
- estat_sbs_ovw_act.tsv
- estat_sbs_ovw_iep.tsv
- estat_sbs_sc_ovw.tsv
- estat_sbs_ovw_smc.tsv

### Step 2 — Install dependencies
Create venv and install:
- Windows PowerShell:
  - `python -m venv .venv`
  - `.\.venv\Scripts\Activate.ps1`
  - `python -m pip install --upgrade pip`
  - `pip install -r requirements.txt`

### Step 3 — Run pipeline scripts
- Bronze:
  - `python src/bronze_ingest.py`
- Silver:
  - `python src/silver_transform.py`
- Gold:
  - `python src/gold_marts.py`

Outputs:
- `data/bronze/*.parquet`
- `data/silver/*.parquet`
- `data/gold/*.parquet` and `data/gold/*.csv`


## Orchestration with Airflow (Local)

### Option A — Recommended (Docker Compose)
1) Go to `airflow/`
2) Run:
   - `docker compose up -d`
3) Open Airflow UI:
   - http://localhost:8080
4) Enable the DAG:
   - `eurostat_lakehouse_dag`

### Option B — Native Airflow (no Docker)
Install:
- `pip install apache-airflow`
Initialize:
- `airflow db init`
Create admin:
- `airflow users create ...`
Run:
- `airflow webserver`
- `airflow scheduler`


## Databricks (Free) Integration — Optional
Databricks Free often restricts compute/clusters. This project supports two practical modes:

### Mode 1 — Use Databricks SQL to query outputs
- Export Gold marts as CSV
- Upload them into Databricks SQL (Add data → Upload file)
- Create tables/views in Databricks SQL editor

### Mode 2 — Keep pipeline local, publish results
- Run pipeline locally
- Upload only final curated outputs (Gold) to Databricks SQL for showcasing

This still demonstrates strong DE skills:
- pipeline design
- orchestration
- data modeling
- quality checks
- reproducibility


## Data Quality Checks
The pipeline includes sanity checks:
- row counts by layer
- null rates
- schema validation (expected columns)
- numeric conversion success rates


## Data Source
Eurostat — Structural Business Statistics (SBS)
https://ec.europa.eu/eurostat


## Author
Mauricio Esquivel
Data Engineer / Analytics Engineer
Focus: Databricks, lakehouse pipelines, orchestration, cloud analytics
