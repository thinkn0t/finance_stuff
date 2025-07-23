import yfinance as yf
import pandas as pd
import os
from datetime import datetime

# Path to your ticker CSV
cik_csv_path = 'Scraper/cik_list/cik_list.csv'

# Output directory for historical prices
output_dir = 'Scraper/stock_historical'
os.makedirs(output_dir, exist_ok=True)

# Load tickers
df = pd.read_csv(cik_csv_path)
tickers = df['symbol'].dropna().unique()

for ticker in tickers:
    try:
        print(f"Processing: {ticker}")
        # Force input as list to avoid MultiIndex column issue
        data = yf.download([ticker], start='1980-01-01', end=datetime.now().strftime('%Y-%m-%d'), progress=False)

        if data.empty:
            print(f"❌ No data for {ticker}")
            continue

        # If column is a MultiIndex (e.g., ('Open', 'AAPL')), flatten it
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)

        # Keep only relevant columns
        columns_to_keep = ['Open', 'High', 'Low', 'Close', 'Volume']
        data = data[columns_to_keep]

        # Reset index to move Date into a column
        data.reset_index(inplace=True)

        # Save to CSV
        output_file = os.path.join(output_dir, f"{ticker}.csv")
        data.to_csv(output_file, index=False)
        print(f"✅ Saved cleaned: {output_file}")

    except Exception as e:
        print(f"🔥 Error processing {ticker}: {e}")
