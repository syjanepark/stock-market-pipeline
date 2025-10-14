import pandas as pd
import boto3
import io

s3 = boto3.client("s3")
BUCKET_NAME = "stock-market-data20"

def convert_to_parquet():
    prefix = "stock_data/"  
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)

    for obj in response.get("Contents", []):
        if obj["Key"].endswith(".csv"):
            csv_obj = s3.get_object(Bucket=BUCKET_NAME, Key=obj["Key"])
            df = pd.read_csv(csv_obj["Body"])

            # Write to Parquet in memory
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)

            parquet_key = obj["Key"].replace("stock_data", "processed").replace(".csv", ".parquet")
            s3.put_object(Bucket=BUCKET_NAME, Key=parquet_key, Body=parquet_buffer.getvalue())

            print(f"✅ Converted {obj['Key']} → {parquet_key}")

if __name__ == "__main__":
    convert_to_parquet()