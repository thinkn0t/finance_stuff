'''
.##.......####.########..########.....###....########..####.########..######.
.##........##..##.....##.##.....##...##.##...##.....##..##..##.......##....##
.##........##..##.....##.##.....##..##...##..##.....##..##..##.......##......
.##........##..########..########..##.....##.########...##..######....######.
.##........##..##.....##.##...##...#########.##...##....##..##.............##
.##........##..##.....##.##....##..##.....##.##....##...##..##.......##....##
.########.####.########..##.....##.##.....##.##.....##.####.########..######.
'''
import requests
import pandas as pd
import os
import json
import time
from datetime import datetime, timedelta
import logging
from requests.exceptions import RequestException
from pandas.tseries.holiday import USFederalHolidayCalendar



'''
.########...#######...#######..########....########..####.########...######.
.##.....##.##.....##.##.....##....##.......##.....##..##..##.....##.##....##
.##.....##.##.....##.##.....##....##.......##.....##..##..##.....##.##......
.########..##.....##.##.....##....##.......##.....##..##..########...######.
.##...##...##.....##.##.....##....##.......##.....##..##..##...##.........##
.##....##..##.....##.##.....##....##.......##.....##..##..##....##..##....##
.##.....##..#######...#######.....##.......########..####.##.....##..######.
'''
root_dir = 'Scraper'



'''
..######..##.....##.########.....########..####.########...######.
.##....##.##.....##.##.....##....##.....##..##..##.....##.##....##
.##.......##.....##.##.....##....##.....##..##..##.....##.##......
..######..##.....##.########.....##.....##..##..########...######.
.......##.##.....##.##.....##....##.....##..##..##...##.........##
.##....##.##.....##.##.....##....##.....##..##..##....##..##....##
..######...#######..########.....########..####.##.....##..######.
'''
cik_list_dir = f'{root_dir}/cik_list'
earnings_daily_dir = f'{root_dir}/earnings_daily'
earnings_all_dir = f'{root_dir}/earnings_all'
error_handling_dir = f'{root_dir}/error_handling'
weekend_holiday_dir = f'{error_handling_dir}/weekend_holiday'
no_data_dir = f'{error_handling_dir}/no_data'



'''
..######..########..########....###....########.########....########..####.########...######.
.##....##.##.....##.##.........##.##......##....##..........##.....##..##..##.....##.##....##
.##.......##.....##.##........##...##.....##....##..........##.....##..##..##.....##.##......
.##.......########..######...##.....##....##....######......##.....##..##..########...######.
.##.......##...##...##.......#########....##....##..........##.....##..##..##...##.........##
.##....##.##....##..##.......##.....##....##....##..........##.....##..##..##....##..##....##
..######..##.....##.########.##.....##....##....########....########..####.##.....##..######.
'''
# Create necessary directories if they do not exist
os.makedirs(cik_list_dir, exist_ok=True)
os.makedirs(earnings_daily_dir, exist_ok=True)
os.makedirs(earnings_all_dir, exist_ok=True)
os.makedirs(error_handling_dir, exist_ok=True)
os.makedirs(weekend_holiday_dir, exist_ok=True)
os.makedirs(no_data_dir, exist_ok=True)



'''
.##.....##....###.....######..########.########.########......######...######..##.....##
.###...###...##.##...##....##....##....##.......##.....##....##....##.##....##.##.....##
.####.####..##...##..##..........##....##.......##.....##....##.......##.......##.....##
.##.###.##.##.....##..######.....##....######...########.....##........######..##.....##
.##.....##.#########.......##....##....##.......##...##......##.............##..##...##.
.##.....##.##.....##.##....##....##....##.......##....##.....##....##.##....##...##.##..
.##.....##.##.....##..######.....##....########.##.....##.....######...######.....###...
'''
master_csv_name = f"{earnings_all_dir}/earnings_master.csv"



'''
....###....########..####....##.....##.########..##........######.
...##.##...##.....##..##.....##.....##.##.....##.##.......##....##
..##...##..##.....##..##.....##.....##.##.....##.##.......##......
.##.....##.########...##.....##.....##.########..##........######.
.#########.##.........##.....##.....##.##...##...##.............##
.##.....##.##.........##.....##.....##.##....##..##.......##....##
.##.....##.##........####.....#######..##.....##.########..######.
'''
cik_url = 'https://www.sec.gov/files/company_tickers.json'
earnings_url = 'https://api.nasdaq.com/api/calendar/earnings?date='



'''
.##.....##.########....###....########..########.########...######.
.##.....##.##.........##.##...##.....##.##.......##.....##.##....##
.##.....##.##........##...##..##.....##.##.......##.....##.##......
.#########.######...##.....##.##.....##.######...########...######.
.##.....##.##.......#########.##.....##.##.......##...##.........##
.##.....##.##.......##.....##.##.....##.##.......##....##..##....##
.##.....##.########.##.....##.########..########.##.....##..######.
'''
# SEC Headers to avoid 403 errors
sec_headers = {
    'User-Agent': 'ResearchProject iintiice@gmail.com',
    'Accept': 'application/json, text, json',
    'Accept-Encoding': 'gzip, deflate',
    'Host': 'www.sec.gov'
}

# Whatever Headers.. For everything else.. -_(o.O)_-
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
    'Accept': 'application/json, text/plain, */*'
}



'''
..######...########.....###....########......######..####.##....##..######.
.##....##..##.....##...##.##...##.....##....##....##..##..##...##..##....##
.##........##.....##..##...##..##.....##....##........##..##..##...##......
.##...####.########..##.....##.########.....##........##..#####.....######.
.##....##..##...##...#########.##.....##....##........##..##..##.........##
.##....##..##....##..##.....##.##.....##....##....##..##..##...##..##....##
..######...##.....##.##.....##.########......######..####.##....##..######.
'''
# Check if CIK list CSV already exists
def check_or_create_cik_list(cik_csv_name):
    if not os.path.exists(cik_csv_name):
        print("CIK list file not found, scraping new CIK data...")

        # List to store CIKs
        cik_list = []

         # Verify endpoint is up and scrape
        cik_r = requests.get(cik_url, headers=sec_headers)
        if cik_r.status_code == 200:
            cik_data = cik_r.json()

            # Scrape required data
            for company_cik in cik_data.values():
                # Check for both 'ticker' and 'symbol' in the JSON data
                ticker_or_symbol = company_cik.get('ticker') or company_cik.get('symbol')

                # Handle cases where neither 'ticker' nor 'symbol' is present
                if not ticker_or_symbol:
                    print(f"Missing ticker or symbol for {company_cik['title']}, skipping.")
                    continue

                dot_symbols = ticker_or_symbol.replace('-', '.')
                ten_digits = str(company_cik['cik_str']).zfill(10)

                cik_list.append({
                    'CIK': ten_digits,
                    'symbol': dot_symbols,
                    'company': company_cik['title']
                })

            # Save CIK data to CSV
            cik_df = pd.DataFrame(cik_list)
            cik_df['CIK'] = cik_df['CIK'].astype(str)
            cik_df['investing.com name'] = ""  # Initialize the new column
            cik_df.to_csv(cik_csv_name, index=False)
            print(f"CIK list created at {cik_csv_name}")
        else:
            print("Failed to retrieve CIK data.")
    else:
        print("CIK list already exists.")



'''
.##.....##.########.########...######...########.....######...######..##.....##..######.
.###...###.##.......##.....##.##....##..##..........##....##.##....##.##.....##.##....##
.####.####.##.......##.....##.##........##..........##.......##.......##.....##.##......
.##.###.##.######...########..##...####.######......##........######..##.....##..######.
.##.....##.##.......##...##...##....##..##..........##.............##..##...##........##
.##.....##.##.......##....##..##....##..##..........##....##.##....##...##.##...##....##
.##.....##.########.##.....##..######...########.....######...######.....###.....######.
'''
# Merge earnings data with CIK list and save the daily CSV
def save_daily_earnings_with_cik(date_string, companies_earn, cik_df):
    # Convert earnings data to a DataFrame
    daily_earnings_df = pd.DataFrame(companies_earn)

    ## Debugging: Print the first few rows of the DataFrame to inspect its structure
    # print(f"Daily earnings data for {date_string}:")
    # print(daily_earnings_df.head())

    # Check if 'symbol' column exists, otherwise handle the issue
    if 'symbol' not in daily_earnings_df.columns:
        print("Error: 'symbol' column not found in the earnings data.")
        return

    # Merge with CIK data on the 'symbol' field
    merged_df = pd.merge(daily_earnings_df, cik_df, how='left', left_on='symbol', right_on='symbol')

    # Save daily CSV with CIKs added
    daily_csv_name = f"{earnings_daily_dir}/{date_string}_earnings.csv"
    merged_df.to_csv(daily_csv_name, index=False)
    print(f"{date_string} earnings record with CIKs saved.")

    # Append to master CSV
    if os.path.exists(master_csv_name):
        merged_df.to_csv(master_csv_name, mode='a', header=False, index=False)
    else:
        merged_df.to_csv(master_csv_name, index=False)
    print(f"Earnings for {date_string} appended to master CSV.")



'''
..######.....###....##.......########.##....##.########.....###....########.
.##....##...##.##...##.......##.......###...##.##.....##...##.##...##.....##
.##........##...##..##.......##.......####..##.##.....##..##...##..##.....##
.##.......##.....##.##.......######...##.##.##.##.....##.##.....##.########.
.##.......#########.##.......##.......##..####.##.....##.#########.##...##..
.##....##.##.....##.##.......##.......##...###.##.....##.##.....##.##....##.
..######..##.....##.########.########.##....##.########..##.....##.##.....##
'''
# Check if a date is a weekend
def is_weekend(date):
    return date.weekday() >= 5

# Check if a date is a holiday
def is_holiday(date):
    cal = USFederalHolidayCalendar()
    holidays = cal.holidays(start=date, end=date)
    return not holidays.empty

# Function to handle logging weekend/holiday days
def log_weekend_holiday(date_string, weekend, holiday):
    file_path = f'{weekend_holiday_dir}/non_trade_days.csv'

    # Load existing CSV if it exists, otherwise create a new DataFrame with consistent columns
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
    else:
        df = pd.DataFrame(columns=['date', 'weekend', 'holiday'])

    # Append new data, ensuring alignment with column names
    new_entry = pd.DataFrame([{'date': date_string, 'weekend': weekend, 'holiday': holiday}])
    df = pd.concat([df, new_entry], ignore_index=True)

    # Ensure no duplicate columns or misplaced data by reordering columns and dropping any unwanted columns
    df = df[['date', 'weekend', 'holiday']]
    df.to_csv(file_path, index=False)



'''
.########.########..########...#######..########...######.
.##.......##.....##.##.....##.##.....##.##.....##.##....##
.##.......##.....##.##.....##.##.....##.##.....##.##......
.######...########..########..##.....##.########...######.
.##.......##...##...##...##...##.....##.##...##.........##
.##.......##....##..##....##..##.....##.##....##..##....##
.########.##.....##.##.....##..#######..##.....##..######.
'''
# Log Errors for no data available
def log_no_data(date_string):
    file_path = f'{no_data_dir}/no_data.csv'
    with open(file_path, 'a') as file:
        file.write(f"{date_string}\n")

# Logging setup
logging.basicConfig(filename=f'{error_handling_dir}/earnings_scrape.log', level=logging.INFO)

# Functions for last processed date
def read_last_processed_date():
    try:
        file_path = f'{error_handling_dir}/last_processed.txt'
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                last_date = file.read().strip()
                return datetime.strptime(last_date, '%Y-%m-%d') if last_date else None
        return None
    except (FileNotFoundError, ValueError):
        return None

def save_last_processed_date(date_string):
    file_path = f'{error_handling_dir}/last_processed.txt'
    with open(file_path, 'w') as file:
        file.write(date_string)



'''
..######...########.....###....########.....########....###....########..##....##.####.##....##..######....######.
.##....##..##.....##...##.##...##.....##....##.........##.##...##.....##.###...##..##..###...##.##....##..##....##
.##........##.....##..##...##..##.....##....##........##...##..##.....##.####..##..##..####..##.##........##......
.##...####.########..##.....##.########.....######...##.....##.########..##.##.##..##..##.##.##.##...####..######.
.##....##..##...##...#########.##.....##....##.......#########.##...##...##..####..##..##..####.##....##........##
.##....##..##....##..##.....##.##.....##....##.......##.....##.##....##..##...###..##..##...###.##....##..##....##
..######...##.....##.##.....##.########.....########.##.....##.##.....##.##....##.####.##....##..######....######.
'''
# Main scraping function with retry mechanism
def scrape_earnings_data(start_date, end_date, cik_df):
    last_processed_date = read_last_processed_date()

    if last_processed_date:
        start_date = max(start_date, last_processed_date + timedelta(days=1))

    current_date = start_date

    while current_date <= end_date:
        date_string = current_date.strftime('%Y-%m-%d')

        # Check for weekends or holidays
        if is_weekend(current_date):
            log_weekend_holiday(date_string, weekend=True, holiday=False)
            current_date += timedelta(days=1)
            continue # Skip to next date


        if is_holiday(current_date):
            log_weekend_holiday(date_string, weekend=False, holiday=True)
            current_date += timedelta(days=1)
            continue # Skip to next date

        daily_earnings_url = earnings_url + date_string

        retries = 5
        for attempt in range(retries):

            try:
                earnings_r = requests.get(daily_earnings_url, headers=headers, timeout=15)
                earnings_r.raise_for_status()
                earnings_data = earnings_r.json()

                if not earnings_data.get('data') or not earnings_data['data'].get('rows'):
                    logging.warning(f"No earnings data found for {date_string}")
                    log_no_data(date_string)
                else:
                    companies_earn = earnings_data['data']['rows']
                    save_daily_earnings_with_cik(date_string, companies_earn, cik_df)
                break

            except RequestException as e:
                logging.error(f"Attempt {attempt + 1} failed for {date_string}: {e}")
                if attempt < retries - 1:
                    delay = 2 ** attempt + 1
                    time.sleep(delay)
                else:
                    logging.error(f"Max retries reached for {date_string}. Skipping date.")

        # Save the last processed date
        save_last_processed_date(date_string)

        # Move to the next day
        current_date += timedelta(days=1)



'''
.##.....##....###....####.##....##....########.##.....##.##....##..######..########.####..#######..##....##
.###...###...##.##....##..###...##....##.......##.....##.###...##.##....##....##.....##..##.....##.###...##
.####.####..##...##...##..####..##....##.......##.....##.####..##.##..........##.....##..##.....##.####..##
.##.###.##.##.....##..##..##.##.##....######...##.....##.##.##.##.##..........##.....##..##.....##.##.##.##
.##.....##.#########..##..##..####....##.......##.....##.##..####.##..........##.....##..##.....##.##..####
.##.....##.##.....##..##..##...###....##.......##.....##.##...###.##....##....##.....##..##.....##.##...###
.##.....##.##.....##.####.##....##....##........#######..##....##..######.....##....####..#######..##....##
'''
# Main function to coordinate scraping process
def main():
    cik_csv_name = f"{cik_list_dir}/cik_list.csv"
    check_or_create_cik_list(cik_csv_name)
    cik_df = pd.read_csv(cik_csv_name, dtype={'CIK': str})

    start_date = datetime(2008, 1, 7)
    end_date = datetime.now() + timedelta(days=23)
    scrape_earnings_data(start_date, end_date, cik_df)

# Run the main function
if __name__ == "__main__":
    main()
