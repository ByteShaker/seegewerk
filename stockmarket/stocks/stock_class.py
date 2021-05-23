from os import path
from logging import Logger

import pandas as pd
from datetime import date, timedelta

from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override()


class Stock:
    def __init__(self, symbol):
        self._symbol = symbol
        self._file_path = './data/' + self._symbol + '.pkl'
        self._symbol_data = None

    def load(self):
        if self._symbol_data:
            Logger.info('Symbol data is not empty.')
        elif path.isfile(self.file_path):
            self._symbol_data = pd.read_pickle(self.file_path)
            Logger.info('Symbol data loaded from pickle file: ' + self._file_path)
        else:
            Logger.error('Symbol data does not exist: ' + self.file_path)

        return self._symbol_data is not None

    def save(self):
        file_path = './data/' + self._symbol + '.pkl'
        self._symbol_data.to_pickle(file_path)
        Logger.info('Symbol data saved to: ' + self._file_path)
        return 0

    def get_data(self, start_date="1990-01-01", end_date=date.today()):
        self._symbol_data = pdr.get_data_yahoo(self._symbol, start=start_date, end=end_date)
        return 0

    def update(self):
        if self.load():
            if self._symbol_data.index[-1] < date.today():
                Logger.info('Last day saved: ' + self._symbol_data.index[-1])
                new_symbol_data = pdr.get_data_yahoo(self._symbol, start=self.symbol_data.index[-1], end=date.today())
                if not new_symbol_data.empty:
                    try:
                        self._symbol_data = self._symbol_data.append(new_symbol_data[self._symbol_data.index[-1] + timedelta(days=1):], verify_integrity=True)
                    except:
                        Logger.error('Update impossible.')
                        '''
                        os.remove(file_path)
                        symbol_data = getData(symbol, start_date, today)
                        if not symbol_data.empty:
                            files.append(symbol)
                            SaveData(symbol_data, file_path)
                        else:
                            failure_files.append(symbol)
                        '''
                else:
                    Logger.info('No new symbol data available or failed download.')
        else:
            self.get_data()