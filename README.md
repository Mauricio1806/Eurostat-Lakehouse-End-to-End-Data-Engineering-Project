🇪🇺 Eurostat Lakehouse on AWS
Bronze → Silver → Gold · Airflow Orchestration · AWS S3 Publishing · Production-Style Data Engineering
📌 Overview

This project implements a production-style end-to-end Data Engineering pipeline using Eurostat Structural Business Statistics (SBS).

It follows a Lakehouse architecture pattern (Bronze → Silver → Gold), orchestrated with Apache Airflow and integrated with AWS S3 for cloud publishing.

The objective is to simulate a real-world analytics engineering workflow using public European economic data.

🎯 What This Project Demonstrates

Layered Lakehouse modeling

ETL modular design

Airflow DAG orchestration

Data normalization & cleaning

Analytical mart construction

HTML report generation

AWS S3 cloud publishing

CLI automation

Reproducible local workflow

Git-based versioning

🏗 Architecture
High-Level Data Flow
Eurostat TSV (Raw)
        ↓
🥉 Bronze (Parquet - raw preserved + metadata)
        ↓
🥈 Silver (Normalized + Cleaned + Typed)
        ↓
🥇 Gold (Analytical Marts)
        ↓
📊 HTML Business Report
        ↓
☁ AWS S3 (Curated Publishing Layer)

🧱 Lakehouse Layers
🥉 Bronze Layer

Purpose: Preserve raw data with minimal transformation.

Reads TSV exactly as downloaded

Adds ingestion metadata:

source_file

ingested_at

Converts to Parquet

Schema preserved

Stored in:

data-bronze/

🥈 Silver Layer

Purpose: Clean and normalize data for analytical readiness.

Transformations:

Split composite dimension column:

freq,nace_r2,indic_sbs,geo


Convert wide year columns:

2005, 2006, 2007 ...


→ to normalized format:

year | value_raw


Clean Eurostat flags:

:

e

b

p

Cast numeric fields

Standardize schema

Stored in:

data-silver/

🥇 Gold Layer

Purpose: Create analytical marts for reporting & BI.

Includes:

Country rankings

Year-over-Year growth

CAGR

Top movers

Indicator-level aggregation

Main outputs:

gold_country_indicator_year.parquet
gold_structural_metrics.parquet
gold_yoy_growth.parquet


Stored in:

data-gold/

🔁 Airflow Orchestration

DAG:

airflow/dags/eurostat_lakehouse_dag.py


Pipeline execution order:

download_raw
    ↓
bronze_ingest
    ↓
silver_transform
    ↓
gold_analytics
    ↓
quality_checks
    ↓
generate_html_report


Ensures:

Controlled task dependencies

Reproducibility

Clear modular separation

Production-style orchestration

📊 HTML Analytical Report

After Gold layer generation:

reports/out/gold_report.html


Includes:

Executive summary

Market leaders

YoY growth charts

Rank movers

CAGR performance

Structural business metrics

This simulates a business-facing analytics deliverable.

☁ AWS Integration (S3 Publishing)

The project publishes curated artifacts to AWS S3.

Example structure:

s3://mauricio-eurostat-lakehouse-prod/
│
├── bronze/
├── silver/
├── gold/
└── reports/
    ├── gold_report.html
    └── assets/


Publishing commands:

aws s3 cp reports/out/gold_report.html \
    s3://mauricio-eurostat-lakehouse-prod/reports/gold_report.html

aws s3 sync reports/out/assets \
    s3://mauricio-eurostat-lakehouse-prod/reports/assets


This demonstrates:

Object storage integration

Curated artifact publishing

Lakehouse-to-cloud workflow

Cloud-ready architecture

📂 Repository Structure
eurostat-lakehouse/
│
├── airflow/
│   ├── dags/
│   └── logs/
│
├── data-raw/
├── data-bronze/
├── data-silver/
├── data-gold/
│
├── reports/
│   ├── templates/
│   └── out/
│
├── src/
│   ├── 00_download_raw.py
│   ├── 01_extract_raw.py
│   ├── 02_bronze_ingest.py
│   ├── 03_silver_transform.py
│   ├── 04_gold_analytics.py
│   ├── 05_quality_checks.py
│   └── utils.py
│
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md

🚀 Running Locally
1️⃣ Create Virtual Environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt

2️⃣ Run Pipeline (Manual)
python src/00_download_raw.py
python src/02_bronze_ingest.py
python src/03_silver_transform.py
python src/04_gold_analytics.py
python src/05_quality_checks.py

3️⃣ Run with Airflow (Docker)
cd airflow
docker compose up -d


Open:

http://localhost:8080


Enable:

eurostat_lakehouse_dag

✅ Data Quality Checks

The pipeline validates:

Row count consistency

Schema validation

Null rate analysis

Numeric conversion success

Layer consistency

Outputs stored in:

outputs-checks/

🛠 Technical Stack

Python

Pandas

PyArrow

Apache Airflow

Docker

AWS CLI

Amazon S3

HTML reporting

Lakehouse modeling pattern

📚 Data Source

Eurostat – Structural Business Statistics (SBS)
https://ec.europa.eu/eurostat

👤 Author

Mauricio Esquivel
Data Engineer | Analytics Engineer

Focus Areas:

Lakehouse Architectures

Airflow Orchestration

AWS & Cloud Storage

Analytics Engineering

Reproducible Data Pipelines
