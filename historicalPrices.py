import yfinance as yf
import pandas as pd
import logging
import os
from datetime import datetime, timedelta

# Setup logging
logging.basicConfig(
    filename='yfinance_scrape.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logging.info("=== Starting yfinance historical price scrape ===")

# Path to your CIK ticker CSV file
cik_csv_path = 'Scraper/cik_list/cik_list.csv'

if not os.path.exists(cik_csv_path):
    logging.error(f"CIK CSV file not found: {cik_csv_path}")
    raise FileNotFoundError(f"CIK CSV file not found: {cik_csv_path}")

df = pd.read_csv(cik_csv_path)
tickers = df['symbol'].dropna().unique().tolist()
logging.info(f"Loaded {len(tickers)} tickers from CIK CSV.")

output_dir = 'historical_prices_yfinance'
os.makedirs(output_dir, exist_ok=True)

end_date = datetime.today().strftime('%Y-%m-%d')  # Today's date as end

def load_ticker_data(filename):
    if not os.path.exists(filename):
        return None
    try:
        df = pd.read_csv(filename, parse_dates=['Date'], index_col='Date')
        return df
    except ValueError:
        # Try without parse_dates first, then convert
        try:
            df = pd.read_csv(filename)
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
                df.set_index('Date', inplace=True)
                return df
            else:
                logging.warning(f"'Date' column missing in {filename}. Skipping file.")
                return None
        except Exception as e:
            logging.error(f"Error reading {filename}: {e}")
            return None
    except Exception as e:
        logging.error(f"Error reading {filename}: {e}")
        return None

for ticker in tickers:
    filename = os.path.join(output_dir, f"{ticker}.csv")
    existing_df = load_ticker_data(filename)
    
    if existing_df is None or existing_df.empty:
        start_date = '1980-01-01'
        logging.info(f"No existing data for {ticker}, starting from {start_date}")
    else:
        last_date = existing_df.index.max()
        start_date_dt = pd.to_datetime(last_date) + timedelta(days=1)
        start_date = start_date_dt.strftime('%Y-%m-%d')
        logging.info(f"{ticker} data exists up to {last_date.date()}, updating from {start_date}")

    # If start_date is after or equal to end_date, skip ticker
    if start_date >= end_date:
        logging.info(f"{ticker} already up to date. Skipping download.")
        print(f"{ticker} already up to date. Skipping.")
        continue

    try:
        data = yf.download(ticker, start=start_date, end=end_date, auto_adjust=True)
        if data.empty:
            logging.warning(f"No new data for {ticker} from {start_date} to {end_date}.")
            print(f"No new data for {ticker}.")
            continue

        # Show each new date in terminal
        for date in data.index:
            print(f"{ticker} - Date: {date.date()} - Close: {data.loc[date]['Close']}")

        # Append or create file
        if existing_df is not None and not existing_df.empty:
            combined = pd.concat([existing_df, data])
            combined = combined[~combined.index.duplicated(keep='last')]  # Remove duplicate dates
            combined.to_csv(filename)
            logging.info(f"Appended new data for {ticker} to {filename}")
        else:
            data.to_csv(filename)
            logging.info(f"Saved new data for {ticker} to {filename}")

    except Exception as e:
        logging.error(f"Error downloading data for {ticker}: {e}")
        print(f"Error downloading data for {ticker}: {e}")

logging.info("=== Finished yfinance historical price scrape ===")
print("=== Finished yfinance historical price scrape ===")
