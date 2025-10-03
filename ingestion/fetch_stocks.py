import warnings
import yfinance as yf
import pandas as pd
import numpy as np
import os
import datetime
import boto3
import json

s3 = boto3.client('s3')
BUCKET_NAME = 'stock-market-data20'
kinesis = boto3.client('kinesis', region_name='us-east-2')

TICKERS = ['AAPL', 'NVDA', 'META', 'AMZN', 'TSLA']
PERIOD = '1y'
INTERVAL = '1d'
OUTPUT_DIR = '../data/raw'
today = datetime.date.today().strftime('%Y-%m-%d')

# Fetch stock data and save results to CSV files
stock_data = yf.download(tickers=TICKERS, period=PERIOD, interval=INTERVAL, group_by='ticker', auto_adjust=True, threads=True)

date_dir = os.path.join(OUTPUT_DIR, today)
if not os.path.exists(date_dir):
    os.makedirs(date_dir)

for ticker in TICKERS:
    df = stock_data[ticker].reset_index()
    df['Ticker'] = ticker
    df.to_csv(os.path.join(date_dir, f'{ticker}_data.csv'), index=False)
    print(f"Stock data for {TICKERS} saved to {OUTPUT_DIR}")
    
    for _, row in df.iterrows():
        record = {
            'ticker': row['Ticker'],
            'timestamp': int(row['Date'].timestamp()),
            'open': row['Open'],
            'high': row['High'],
            'low': row['Low'],
            'close': row['Close'],
            'volume': int(row['Volume'])
        }

        kinesis.put_record(
            StreamName='stock_data_stream',
            Data=json.dumps(record),
            PartitionKey=row['Ticker']
        )
    print(f"Sent record for {ticker} on {row['Date']} to Kinesis")

records = []

for ticker in TICKERS:
    df_ticker = stock_data[ticker].reset_index()
    df_ticker['ticker'] = ticker
    records.extend(df_ticker.to_dict(orient='records'))

df = pd.DataFrame(records)
df['timestamp'] = pd.to_datetime(df['Date'])

print(df.head())

def send_to_kinesis(client, stream_name, row_json, partition_key):
    """
    Send a single record to Kinesis Data Stream.
    """
    try:
        response = client.put_record(
            StreamName=stream_name,
            Data=row_json,
            PartitionKey=partition_key
        )
        print(f"Successfully sent record to Kinesis: {response}")
    except Exception as e:
        print(f"Failed to send record to Kinesis: {e}")

def process_df(df,stream_name,region_name='us-east-2'):
    """
    Process the DataFrame and send each row to Kinesis Data Stream.
    """
    kinesis_client = boto3.client('kinesis', region_name=region_name)
    
    for _, row in df.iterrows():
        row_dict = row.asdict()
        row_json = json.dumps(row_dict)
        partition_key = row['ticker'][0]  # Use the first ticker as partition key
        send_to_kinesis(kinesis_client, stream_name, row_json, partition_key)

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore")


