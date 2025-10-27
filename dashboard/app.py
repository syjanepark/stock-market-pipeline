import streamlit as st
import boto3
import pandas as pd
import io
from datetime import datetime
from pyathena import connect

# --- Load AWS secrets ---
aws_key = st.secrets["aws"]["aws_access_key_id"]
aws_secret = st.secrets["aws"]["aws_secret_access_key"]
region = st.secrets["aws"]["aws_default_region"]

# --- Streamlit title and description ---
st.title("📈 Stock Market Data Pipeline Dashboard")
st.markdown("""
This dashboard displays stock performance trends pulled from Yahoo Finance, 
processed in AWS S3, and visualized with Streamlit.
""")

# --- Initialize AWS clients ---
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_key,
    aws_secret_access_key=aws_secret,
    region_name=region
)

# --- Load stock data (Athena first, fallback to S3) ---
@st.cache_data
def load_stock_data():
    """Load stock data with Athena fallback to S3"""
    bucket_name = "stock-market-data20"
    
    # Try Athena first
    try:
        conn = connect(
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret,
            s3_staging_dir="s3://stock-market-data20/athena-results/",
            region_name=region,
            schema_name=st.secrets["athena"]["database"]
        )

        query = """
        SELECT 
            Ticker as ticker,
            Date as date,
            Open as open,
            High as high,
            Low as low,
            Close as close,
            Volume as volume
        FROM stock_market_db.stocks_parquet
        LIMIT 1000
        """

        df = pd.read_sql(query, conn)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            st.success("✅ Data loaded from Athena")
            return df

    except Exception as e:
        st.warning(f"⚠️ Athena not available - using S3 instead")

    # --- Fallback to S3 ---
    st.info("🔄 Loading data directly from S3...")

    try:
        # Find the latest date folder automatically
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix="processed/", Delimiter="/")
        folders = [c["Prefix"] for c in response.get("CommonPrefixes", [])]

        if not folders:
            st.error("❌ No folders found under processed/.")
            return pd.DataFrame()

        latest_prefix = sorted(folders)[-1]  # get latest folder by date
        st.info(f"📅 Loading data from `{latest_prefix}`")

        # List parquet files inside the latest folder
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=latest_prefix)
        if "Contents" not in response:
            st.error(f"❌ No files found in {latest_prefix}")
            return pd.DataFrame()

        all_data = []

        for obj in response["Contents"]:
            if obj["Key"].endswith(".parquet"):
                parquet_obj = s3.get_object(Bucket=bucket_name, Key=obj["Key"])
                df_part = pd.read_parquet(io.BytesIO(parquet_obj["Body"].read()))
                
                # Normalize column names
                df_part.columns = [c.strip().lower() for c in df_part.columns]
                df_part = df_part.rename(columns={"adj close": "close", "symbol": "ticker"})
                
                expected_cols = {"ticker", "date", "open", "high", "low", "close", "volume"}
                
                if not expected_cols.issubset(df_part.columns):
                     st.warning(f"⚠️ Skipping file {obj['Key']} — missing expected columns: {df_part.columns.tolist()}")
                     continue

                all_data.append(df_part)

        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df["date"] = pd.to_datetime(combined_df["date"], errors="coerce")
            combined_df.dropna(subset=["date"], inplace=True)
            st.success("✅ Data loaded from S3")
            return combined_df.sort_values("date", ascending=False)

        st.error("❌ No valid parquet files found.")
        return pd.DataFrame()

    except Exception as e:
        st.error(f"❌ Error loading data from S3: {e}")
        return pd.DataFrame()

# --- Load the data ---
df = load_stock_data()

if df.empty:
    st.error("No data found. Please ensure the pipeline has run and parquet files are available in S3.")
    st.info("Check that parquet files exist in s3://stock-market-data20/processed/")
    st.stop()

# --- Basic data info ---
st.info(f"📊 Loaded {len(df)} records from {df['ticker'].nunique()} stocks")

# --- Metrics ---
latest_close = df["close"].iloc[-1]
avg_close = df["close"].mean()
max_close = df["close"].max()

col1, col2, col3 = st.columns(3)
col1.metric("📊 Latest Close", f"${latest_close:.2f}")
col2.metric("📈 Average Close", f"${avg_close:.2f}")
col3.metric("🏆 Highest Close", f"${max_close:.2f}")

# --- Stock selector ---
selected_ticker = st.selectbox("Select Stock", df["ticker"].unique())
df_filtered = df[df["ticker"] == selected_ticker]

# --- Date filtering ---
if not df_filtered.empty:
    start_date, end_date = st.date_input(
        "Select Date Range",
        [df_filtered["date"].min().date(), df_filtered["date"].max().date()]
    )

    mask = (df_filtered["date"].dt.date >= start_date) & (df_filtered["date"].dt.date <= end_date)
    df_filtered = df_filtered.loc[mask]

    # --- Display filtered data ---
    st.subheader(f"📊 {selected_ticker} Stock Data")
    st.dataframe(df_filtered)

    # --- Charts ---
    if not df_filtered.empty:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader(f"📈 {selected_ticker} Stock Price Trend")
            st.line_chart(df_filtered.set_index("date")["close"])

        with col2:
            st.subheader(f"📊 {selected_ticker} Volume")
            st.bar_chart(df_filtered.set_index("date")["volume"])

        # --- Extra metrics ---
        st.subheader(f"📈 {selected_ticker} Performance Metrics")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Current Price", f"${df_filtered['close'].iloc[-1]:.2f}")
        col2.metric("Average Price", f"${df_filtered['close'].mean():.2f}")
        col3.metric("Highest Price", f"${df_filtered['close'].max():.2f}")
        col4.metric("Lowest Price", f"${df_filtered['close'].min():.2f}")