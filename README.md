Eurostat Lakehouse on AWS

Bronze → Silver → Gold | Airflow Orchestration | S3 Publishing | Production-Style Data Engineering

Overview

This repository implements a production-style Data Engineering project using Eurostat Structural Business Statistics (SBS) datasets.

The pipeline follows a Lakehouse architecture pattern (Bronze → Silver → Gold), orchestrated with Apache Airflow locally, and publishes curated outputs to AWS S3.

This project demonstrates:

End-to-end data pipeline design

Layered lakehouse modeling

Airflow DAG orchestration

Data quality validation

AWS S3 integration

Reproducible local-to-cloud workflow

Git-based version control

The goal is to simulate a real-world analytics engineering workflow using public economic data.

Architecture
High-Level Flow
Eurostat TSV (Raw)
        ↓
🥉 Bronze Layer (Parquet, minimal transformation)
        ↓
🥈 Silver Layer (Normalized + Cleaned + Typed)
        ↓
🥇 Gold Layer (Analytical marts + KPIs)
        ↓
📊 HTML Analytical Report
        ↓
☁ AWS S3 (Curated publishing)

Lakehouse Layers
🥉 Bronze Layer

Reads Eurostat TSV exactly as downloaded

Preserves raw structure

Adds ingestion metadata:

source_file

ingested_at

Converts to Parquet

Stored in:

data-bronze/


Minimal transformation, schema preserved.

🥈 Silver Layer

Splits Eurostat composite dimension column:

freq,nace_r2,indic_sbs,geo


Converts wide year columns:

2005, 2006, 2007 ...


→ into normalized format:

year | value_raw


Cleans Eurostat flags:

:

e

b

p

Casts numeric fields

Produces consistent schema

Stored in:

data-silver/

🥇 Gold Layer

Analytical marts designed for reporting and BI consumption.

Includes:

gold_country_indicator_year.parquet

gold_structural_metrics.parquet

gold_yoy_growth.parquet

Metrics include:

Country rankings

Year-over-Year growth

CAGR

Top movers

Market leaders by indicator

Stored in:

data-gold/

Airflow Orchestration

The pipeline is orchestrated through a DAG:

airflow/dags/eurostat_lakehouse_dag.py


Task flow:

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


The DAG ensures:

Dependency control

Reproducibility

Modular execution

Clear separation of concerns

HTML Analytical Report

After Gold layer generation, the pipeline produces:

reports/out/gold_report.html


This report includes:

Executive snapshot

Top 10 countries

YoY growth charts

Rank movers

CAGR performance

Structural business metrics

The report simulates a business-ready analytics deliverable.

AWS Integration (S3 Publishing)

This project publishes curated outputs to AWS S3.

Example structure inside S3:

s3://mauricio-eurostat-lakehouse-prod/
    ├── bronze/
    ├── silver/
    ├── gold/
    └── reports/
        ├── gold_report.html
        └── assets/


Publishing is done via AWS CLI:

aws s3 cp reports/out/gold_report.html s3://bucket-name/reports/gold_report.html
aws s3 sync reports/out/assets s3://bucket-name/reports/assets


This demonstrates:

Cloud integration

Storage layer separation

Production-style artifact publishing

Lakehouse to object storage workflow

Repository Structure
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

How to Run (Local)
1. Create Virtual Environment

Windows PowerShell:

python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt

2. Run Pipeline (Manual)
python src/00_download_raw.py
python src/02_bronze_ingest.py
python src/03_silver_transform.py
python src/04_gold_analytics.py
python src/05_quality_checks.py

3. Run via Airflow (Docker)

Inside airflow/:

docker compose up -d


Open:

http://localhost:8080


Enable:

eurostat_lakehouse_dag

Data Quality Checks

Includes:

Row count validation per layer

Schema validation

Null rate checks

Numeric conversion checks

Layer consistency validation

Outputs saved in:

outputs-checks/

Technical Skills Demonstrated

Lakehouse modeling

Data normalization

ETL modularization

Airflow orchestration

Data quality engineering

Cloud storage integration (AWS S3)

CLI automation

Analytical data mart design

Reproducible local development workflow

Git versioning

Data Source

Eurostat – Structural Business Statistics (SBS)
https://ec.europa.eu/eurostat

Author

Mauricio Esquivel
Data Engineer | Analytics Engineer
Focus: Lakehouse Architectures, Airflow, AWS, Databricks-style pipelines, Cloud Analytics
