from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Define default DAG arguments
default_args = {
    "owner": "Jane Park",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def run_parquet_conversion():
    import sys
    sys.path.append("/Users/jane/Desktop/stock-market-pipeline/transformation")
    from convert_to_parquet import convert_to_parquet
    convert_to_parquet()

# DAG definition
with DAG(
    dag_id="stock_market_pipeline",
    default_args=default_args,
    description="Fetch and upload daily stock data to S3",
    schedule_interval="@daily",        
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["data-engineering", "stocks"],
) as dag:

    # run the ingestion script
    run_ingestion = BashOperator(
        task_id="fetch_stock_data",
        bash_command="python3 /Users/jane/Desktop/stock-market-pipeline/ingestion/fetch_stocks.py",
    )

    # convert CSV files to parquet
    convert_to_parquet_task = PythonOperator(
        task_id="convert_to_parquet",
        python_callable=run_parquet_conversion,
    )

    # Set task dependencies
    run_ingestion >> convert_to_parquet_task