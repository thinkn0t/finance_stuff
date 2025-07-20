import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timedelta

# Path to your ticker CSV
cik_csv_path = 'Scraper/cik_list/cik_list.csv'

# Output directory for historical prices
output_dir = 'Scraper/stock_historical'
os.makedirs(output_dir, exist_ok=True)

# Load tickers
df = pd.read_csv(cik_csv_path)
tickers = df['symbol'].dropna().unique().tolist()
print(f"Loaded {len(tickers)} tickers.")

def get_last_date_for_ticker(ticker):
    filename = os.path.join(output_dir, f"{ticker}.csv")
    if not os.path.exists(filename):
        return None
    try:
        df_existing = pd.read_csv(filename, parse_dates=['Date'])
        if df_existing.empty:
            return None
        return df_existing['Date'].max()
    except Exception as e:
        print(f"Error reading existing file for {ticker}: {e}")
        return None

for ticker in tickers:
    print(f"\nProcessing ticker: {ticker}")
    
    last_date = get_last_date_for_ticker(ticker)
    
    if last_date is None:
        # If no existing data, start from 1980-01-01 or earlier
        start_date = '1980-01-01'
        print(f"No existing data found. Downloading full history for {ticker} from {start_date}.")
    else:
        # Start from day after last_date to avoid overlap
        start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"Existing data found. Downloading new data for {ticker} from {start_date}.")
    
    end_date = datetime.today().strftime('%Y-%m-%d')

    if start_date > end_date:
        print(f"No new data to download for {ticker}.")
        continue
    
    try:
        data = yf.download(ticker, start=start_date, end=end_date, progress=False)
    except Exception as e:
        print(f"Failed to download data for {ticker}: {e}")
        continue

    if data.empty:
        print(f"No new data found for {ticker} between {start_date} and {end_date}.")
        continue
    
    # Reset index to have Date as a column
    data.reset_index(inplace=True)
    
    # Ensure columns are named simply: Date, Open, High, Low, Close, Volume, etc.
    # If any multi-level columns exist, flatten them (should not happen here but safe)
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = ['_'.join(col).strip() for col in data.columns.values]
    
    # Read existing data if present
    filename = os.path.join(output_dir, f"{ticker}.csv")
    if os.path.exists(filename):
        existing_df = pd.read_csv(filename, parse_dates=['Date'])
        combined_df = pd.concat([existing_df, data], ignore_index=True)
        # Remove possible duplicates (e.g. overlapping last day)
        combined_df.drop_duplicates(subset=['Date'], keep='last', inplace=True)
        combined_df.sort_values('Date', inplace=True)
    else:
        combined_df = data

    # Save combined data back to CSV with simple headers
    combined_df.to_csv(filename, index=False)
    print(f"Saved data for {ticker} ({len(data)} new rows) to {filename}.")

print("\nAll done.")
