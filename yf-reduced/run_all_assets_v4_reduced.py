import requests
# import nasdaqdatalink as ns
import seaborn as sns
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from datetime import timedelta
import matplotlib.pyplot as plt
# from sklearn.cluster import KMeans
from bs4 import BeautifulSoup
import datetime
import signal
import time
import subprocess # CLI script
import os
import json
# import pandas_market_calendars as mcal
import threading
from dateutil.parser import parse
import logging # no crashing computer
import multiprocessing
from pandas_datareader import data as pdr
import sqlite3
yf.pdr_override() # Override for speed


# Task scheduler 
# loc: C:\Users\hello\AppData\Local\Programs\Python\Python311\python.exe 
# args: C:\Users\hello\Documents\GitHub\eq-sys-2\yf-reduced\run_all_assets_v4_reduced.py

####################################################################################
####################################################################################
####################################################################################
##################################       MAIN       ################################
##################################     FUNCTIONS    ################################
####################################################################################
####################################################################################
####################################################################################

# from holidays import get_holidays
def get_us_holidays(year):
    holidays = [
        datetime.date(year, 1, 1),  # New Year's Day
        datetime.date(year, 7, 4),  # Independence Day
        datetime.date(year, 12, 25)  # Christmas Day
    ]
    return holidays

 # Define function to get list of symbols to download
def get_symbols(path):
    # Example list of symbols
    with open(path, 'r') as file:
        symbols_list = [s.strip() for s in file]
    return symbols_list

# Description: Downloads symbol using pandas datareader. Includes threading
# Input: symbol, period, interval
# Output: Dataframe with index reset
# Takes in symbol, downloads symbol and returns df

def yfNewData(symbol, period, interval):
    print(f"Executing yfNewData({symbol}, {period}, {interval})")
    a = yf.download( # or yf.download
            # tickers list or string as well
            tickers = symbol,
            
            # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
            # (optional, default is '1mo')
            period = period,

            # fetch data by interval (including intraday if period < 60 days)
            # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
            # (optional, default is '1d')
            interval = interval,

            # Whether to ignore timezone when aligning ticker data from 
            # different timezones. Default is True. False may be useful for 
            # minute/hourly data.
            ignore_tz = False,

            # group by ticker (to access via data['SPY'])
            # (optional, default is 'column')
            group_by = 'column',

            # adjust all OHLC automatically
            # (optional, default is False)
            auto_adjust = False,

            # attempt repair of missing data or currency mixups e.g. $/cents
            repair = False,

            # download pre/post regular market hours data
            # (optional, default is False)
            prepost = True,

            # use threads for mass downloading? (True/False/Integer)
            # (optional, default is True)
            threads = True,

            # proxy URL scheme use use when downloading?
            # (optional, default is None)
            proxy = None
        )
    
    data = a.reset_index()
    
    # Check if index column exists in downloaded data
    if 'index' in data:
        print('index')
        return data.drop('index', axis=1)  # axis = 0 rows, axis = 1 columns
    else:
        return data  # axis = 0 rows, axis = 1 column   
    

# Define a function to download data for multiple intervals
def download_all_intervals(intervals, symbols):
    for interval in intervals:
        period = interval_dict[interval]

        # Construct the database path and create the connection to engine object
        db_path = r"C:/Users/hello/Documents/GitHub/eq-sys-1/yf/yf-reduced/"
        db_path = os.path.join(db_path, f'yf-all-{typeAsset}-{interval}-reduced.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        for symbol in symbols:
            # Format symbol into a valid table name. Replace . with _ and - with _
            download_symbol_data(symbol, period, interval, cursor, conn)
        # Finished 1m interval
        logging.info(f"Done interval {interval} for database.")
        cursor.close()



# Define function to download data for a single symbol and interval using multithreading
def download_symbol_data(symbol, period, interval, cursor, conn):
    # if table has Date aka key == 'max'. Else use Datetime format for columns
    if interval_dict[interval] == 'max':
        # print('Date format')
        date_format = 'Date'
    else:
        # print("Datetime format")
        date_format = 'Datetime'
    
    # check if table exists

    symbol_str_db = symbol.replace(".", "_").replace("-", "_")
    cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name=?;", (symbol_str_db,))
    table_exists = cursor.fetchone()[0]

    # exit()

    # Create new viable ticker for table name.

    if not table_exists or table_exists is None or table_exists == 0:
        try:
            # create new table if it doesn't exist
            cursor.execute(f'''CREATE TABLE "{symbol_str_db}"
                            ({date_format} TEXT, Open REAL, High REAL, Low REAL, Close REAL, Adj_Close REAL, Volume INTEGER)''')

            print("New table created.")
            # logging.info("New table created.")
        except Exception as e:
            print(f"Table {symbol_str_db} already exists. Skipping creation.")
            # logging.info(f"Table {symbol_str_db} already exists. Skipping creation.")


    # download data to insert
    df = yfNewData(symbol, period, interval)
    
    # fetch all data from the database table
    cursor.execute(f"SELECT * FROM '{symbol_str_db}' ORDER BY {date_format} DESC")
    data = pd.DataFrame(cursor.fetchall(), columns=[f"{date_format}", "Open", "High", "Low", "Close", "Adj Close", "Volume"])

    count_insert = 0
    # loop through each row in the dataframe
    for i, row in df.iterrows():
        # check if data already exists in database
        datetime_str = row[f"{date_format}"].strftime("%Y-%m-%d %H:%M:%S%z")
        data_exists = datetime_str in data[f"{date_format}"].values

        # if data doesn't exist, insert row into table
        if not data_exists:
            cursor.execute(f"INSERT INTO '{symbol_str_db}' VALUES (?, ?, ?, ?, ?, ?, ?)",
                          (datetime_str, row["Open"], row["High"], row["Low"], row["Close"], row["Adj Close"], row["Volume"]))
            conn.commit()
            # print(f"Inserted data for {symbol_str_db} {row[f'{date_format}']}.")
            # logging.info(f"Inserted data for {symbol_str_db} {row[f'{date_format}']}.")
            count_insert += 1

            # print(f"Data for {symbol_str_db} {row[f'{date_format}']} already exists.")
            # logging.info(f"Data for {symbol_str_db} {row[f'{date_format}']} already exists.")

    if count_insert == 0:
        print(f"Done processing {symbol_str_db} for interval {interval}. No new rows inserted") 
        logging.info(f"Done processing {symbol_str_db} for interval {interval}. No new rows inserted") 
    else:
        print(f"Done processing {symbol_str_db} for interval {interval}. Inserted {count_insert} rows.") 
        logging.info(f"Done processing {symbol_str_db} for interval {interval}. Inserted {count_insert} rows.") 


intervals = ['1m', '2m', '1h', '1d']
# Define the interval dictionary
interval_dict = {
    '1m': '7d',
    '2m': '60d',
    '5m': '60d',
    '15m': '60d',
    '30m': '60d',
    '60m': '730d', # Datetime
    '90m': '60d', # Datetime
    '1h': '730d', # Datetime
    '1d': 'max', # Date
    '5d': 'max', # Date
    '1wk': 'max', # Date column
    '1mo': 'max',
    '3mo': 'max',
}




################# BEGIN ###################
assetType_list = ['equities', 'etf', 'futures', 'indices']
for typeAsset in assetType_list:
    print(typeAsset)
    if typeAsset == 'equities':
        path_to_tickers = 'C:/Users/hello/Documents/GitHub/eq-sys-1/yf/yf-reduced/symbols-yf-main/yf_equities-NYSE-NASDAQ-NASDAQGS-NASDAQGM-Vol-100k-US-3662.txt'
        print("path_to_tickers:", path_to_tickers)
    elif typeAsset == 'etf':
        path_to_tickers = 'C:/Users/hello/Documents/GitHub/eq-sys-1/yf/yf-reduced/symbols-yf-main/yf_ETF_vol_greater_10_all_exchanges_298.txt'
        print("path_to_tickers:", path_to_tickers)
    elif typeAsset == 'futures':
        path_to_tickers = "C:/Users/hello/Documents/GitHub/eq-sys-1/yf/yf-reduced/symbols-yf-main/yf_futures_VOL_greater_1_358.txt"
        print("path_to_tickers:", path_to_tickers)

    elif typeAsset == 'indices':
        path_to_tickers = "C:/Users/hello/Documents/GitHub/eq-sys-1/yf/yf-reduced/symbols-yf-main/yf_indices_USA_vol_greater_50_249.txt"
        print("path_to_tickers:", path_to_tickers)

    symbols = get_symbols(path_to_tickers)
    # 4567 tickers
    # Configure the logging module
    start_time = datetime.datetime.now()
    timestamp = start_time.strftime('%Y-%m-%d_%H-%M-%S')
    path_to_log_directory = r'C:/Users/hello\Documents/GitHub/eq-sys-1/yf/yf-reduced/logs'

    filename = r'{}\output_{}.log'.format(path_to_log_directory, timestamp)
    logging.basicConfig(filename=filename, level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

    # Redirect the output of log messages to the file
    # print("Starting program...")
    logging.info(f'Starting yf-{typeAsset} program...')


    download_all_intervals(intervals, symbols)

    # Get the current date and time at the end of the program
    end_time = datetime.datetime.now()

    # Calculate the total time taken to download data
    total_time = end_time - start_time
    logging.info(f"Start time: {start_time}")
    logging.info(f"End time: {end_time}")
    logging.info(f"Total time taken: {total_time}")
    logging.info(f'Finished yf-{typeAsset} program')



##### Notes
# Put main-all.bat into Startup folder