import finnhub
import pandas as pd
from datetime import date, datetime, timedelta
import time
import os
from os import listdir, path
from tqdm import tqdm, trange

from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override()

# Setup client: https://finnhub.io/docs/api#stock-symbols
finnhub_client = finnhub.Client(api_key="buo33hf48v6vd0a77a9g")

stock_symbols_de = finnhub_client.stock_symbols('DE')
stock_symbols_us = finnhub_client.stock_symbols('US')
stock_symbols_ss = finnhub_client.stock_symbols('SS')

print(len(stock_symbols_de))
print(len(stock_symbols_us))
print(len(stock_symbols_ss))

DF_DE = pd.DataFrame.from_dict(stock_symbols_de)
DF_US = pd.DataFrame.from_dict(stock_symbols_us)
DF_SS = pd.DataFrame.from_dict(stock_symbols_ss)

def getData(symbol, start_date, end_date):
    data = pdr.get_data_yahoo(symbol, start=start_date, end=end_date)
    return data

def SaveData(df, file_path):
    df.to_pickle(file_path)

def LoadData(file_path):
    df = pd.read_pickle(file_path)
    return df

start_date = "1990-01-01"
end_date = "2019-12-31"
today = date.today()
files = []
failure_files = []


def load_or_update_market_data(symbols_list):
    for symbol in tqdm(symbols_list):
        file_path = './data/' + symbol + '.pkl'
        if path.isfile(file_path):
            print(file_path)
            symbol_data = LoadData(file_path)
            if symbol_data.index[-1] < today:
                print('Last day saved: ', symbol_data.index[-1])
                # if not (symbol_data.index[-1]+timedelta(days=1) >= today):
                # print(symbol_data.tail(5))
                # print(symbol_data.index[-1]+timedelta(days=1))
                print(symbol_data.index[-1], today)
                new_symbol_data = getData(symbol, symbol_data.index[-1], today);
                if not new_symbol_data.empty:
                    # print(new_symbol_data[symbol_data.index[-1]+timedelta(days=1):])
                    print('Start Append: ', symbol_data.index[-1] + timedelta(days=1))
                    try:
                        symbol_data = symbol_data.append(new_symbol_data[symbol_data.index[-1] + timedelta(days=1):],
                                                         verify_integrity=True)
                        SaveData(symbol_data, file_path)
                    except:
                        print(new_symbol_data[symbol_data.index[-1] + timedelta(days=1):])
                        print(symbol_data)
                        os.remove(file_path)
                        symbol_data = getData(symbol, start_date, today)
                        if not symbol_data.empty:
                            files.append(symbol)
                            SaveData(symbol_data, file_path)
                        else:
                            failure_files.append(symbol)

                else:
                    pass
        else:
            symbol_data = getData(symbol, start_date, today)
            if not symbol_data.empty:
                files.append(symbol)
                SaveData(symbol_data, file_path)
            else:
                failure_files.append(symbol)


load_or_update_market_data(DF_DE['symbol'])
load_or_update_market_data(DF_US['symbol'])
load_or_update_market_data(DF_SS['symbol'])