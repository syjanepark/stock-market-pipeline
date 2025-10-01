import warnings
import yfinance as yf
import pandas as pd
import os
import datetime
import boto3

s3 = boto3.client('s3')
BUCKET_NAME = 'stock-market-data20'


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
    
    s3_key = f'stock_data/{today}/{ticker}_data.csv'
    try:
        s3.upload_file(os.path.join(date_dir, f'{ticker}_data.csv'), BUCKET_NAME, s3_key)
        print(f"Uploaded {ticker}_data.csv to S3 bucket {BUCKET_NAME} at {s3_key}")
    except Exception as e:
        print(f"Failed to upload {ticker}_data.csv to S3: {e}")

print("Stock data fetching and uploading completed.")
print(stock_data.head())

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore")


