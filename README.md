# Stock Market Data Pipeline (AWS + Airflow)

## Overview
This project is an end‑to‑end data pipeline that collects daily stock data from Yahoo Finance, stores it in AWS S3, and converts it to Parquet for analytics.  
Everything is automated through Apache Airflow, giving me hands‑on experience with orchestration, ETL design, and cloud data workflows.

---

## Architecture
1. **Ingestion Layer**
   - A Python script uses `yfinance` to pull historical stock prices (AAPL, AMZN, NVDA, META, TSLA).
   - The data is cleaned and uploaded to **S3** as CSV files in a `stock_data/<date>/` folder.
   - Data is also streamed to **AWS Kinesis** for real-time processing.

2. **Transformation Layer**
   - Another Python script reads the raw CSVs from S3.
   - The files are converted into **Parquet** format for faster querying using `pyarrow`.
   - The results are written back to a `processed/<date>/` folder in S3.

3. **Orchestration Layer**
   - Apache Airflow manages the workflow using a DAG (`stock_market_pipeline_dag.py`).
   - The DAG runs two tasks in order:
     1. `fetch_stock_data`
     2. `convert_to_parquet`
   - It’s scheduled to run once a day, but can also be triggered manually from the Airflow UI.

4. **Analytics Layer**
   - Integrated AWS Athena to query processed Parquet data directly from S3.
   Created an external table (stocks_parquet) and ran SQL queries to analyze stock trends and trading volume.
   Verified that the data pipeline produces analytics-ready datasets for dashboards.



---

## Tech Stack
| Component | Tools Used |
|------------|-------------|
| Programming | Python, Pandas, yfinance, boto3, pyarrow |
| Storage | AWS S3 |
| Streaming | AWS Kinesis |
| Orchestration | Apache Airflow |
| Data Format | CSV → Parquet |
| Optional Analytics | AWS Athena, QuickSight |

---

## Folder Structure
```
stock-market-pipeline/
├── ingestion/
│   └── fetch_stocks.py
├── transformation/
│   └── convert_to_parquet.py
├── airflow/
│   └── dags/
│       └── stock_market_pipeline_dag.py
├── data/
│   └── raw/
├── requirements.txt
└── README.md
```

---

## How It Works
1. The Airflow DAG triggers the `fetch_stocks.py` script each day.
2. New data is fetched from Yahoo Finance and uploaded to S3.
3. The second task converts yesterday’s CSVs into Parquet format.
4. The processed data is ready for Athena or dashboarding.

---

## How to Run It
1. Clone the repo and install dependencies.
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
2. Configure AWS credentials.
   ```bash
   aws configure
   ```
3. Copy the DAG to Airflow's DAGs folder.
   ```bash
   cp airflow/dags/stock_market_pipeline_dag.py /Users/jane/airflow/dags/
   ```
4. Start Airflow.
   ```bash
   airflow db init
   airflow users create --username admin --password admin --firstname Jane --lastname Park --role Admin --email janesypark122@gmail.com
   airflow scheduler &
   airflow webserver --port 8080
   ```
5. Open [http://localhost:8080](http://localhost:8080), unpause the DAG, and trigger it manually.
