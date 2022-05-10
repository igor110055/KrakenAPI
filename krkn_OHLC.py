# Download Full Archive + Incremental OHLCV data

import os
from numpy import delete
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
import shutil
import sys
import requests
import json
sys.path.append(os.getenv("KRAKEN_PATH"))
from krkn_Database import KrakenDB

class Kraken_OHLC(KrakenDB):
    def __init__(self):
        """
        Inherits KarkenDB
        """
        KrakenDB.__init__(self)
        self.time_frames = {
        '1m':1, '5m':5,'15m': 15,'30m': 30,
        '1H': 60, '4H': 240, '12H':720, 
        '1D': 1440,'1W': 10080,
        '15D': 21600
        }
        self.pairs_df = self.get_TradeablePairsByQuote()
        self.DATA_PATH = os.path.join(os.getenv("KRAKEN_DATA_PATH"),'OHLC')
        self.COLS = ['Date','Open','High','Low','Close','VWAP','Volume','Trades']
        

    def dir_checks(self):
        for time_frame in self.time_frames:
            if not os.path.exists(os.path.join(self.DATA_PATH,time_frame)):
                os.mkdir(os.path.join(self.DATA_PATH,time_frame))
                print(f"Created: {os.path.join(self.DATA_PATH,time_frame)}")
            else:
                print(f"Found: {os.path.join(self.DATA_PATH,time_frame)}")


    def market_candles(self,ticker,time_frame,since=None):
        # [int <time>, 
        # string <open>, string <high>, string <low>, string <close>, 
        # string <vwap>, string <volume>, int <count>]
        interval = self.time_frames[time_frame]

        ticker_info = self.pairs_df[self.pairs_df.altname==ticker]
        if len(ticker_info)==1:
            ticker_info = ticker_info.iloc[0]
        else:
            print('Ticker Not Found In Tradeable Tickers DB')
            ticker_info = None
        
        if ticker_info is not None:
            resp = requests.get(f'https://api.kraken.com/0/public/OHLC',
                                params={
                                    'pair':ticker,
                                    'interval':interval,
                                    'since':since
                                })
            
            if len(resp.json()['error'])>0:
                print(f"Error Downloading OHLC: {ticker}")
                _df = pd.DataFrame(columns=self.COLS)
                since = 0
            else:
                resp = resp.json()
                since = resp['result']['last']

                resp_keys = list(resp['result'].keys())

                if ticker_info.base+ticker_info.quote in resp_keys: 
                    _df = pd.DataFrame(resp['result'][ticker_info.base+ticker_info.quote],columns=self.COLS)
                else:
                    _df = pd.DataFrame(resp['result'][ticker],columns=self.COLS)
                _df = _df.astype(float)
                _df.Date = _df.Date.apply(lambda x:datetime.fromtimestamp(x))
        else:
            print("Ticker info not Found = get_ohlcv() Failed...")
            _df = pd.DataFrame(columns=self.COLS)
            since = 0
        return _df, since

    def ohlc_update(self,ticker,time_frame,update=True):
        """
        - Check if the Dirs exists
        - if exists > get main_df else empty main_df
        - Get the latest DF
        - Append and Cleanup maindf
        - Save to maindf - store total number of rows of main_df for each trading pair
        """
        #GET SINCE
        all_since = self.OHLC_last_update()
        # PATHS
        time_frame_path = os.path.join(os.getenv("KRAKEN_DATA_PATH"),'OHLC',time_frame)
        ticker_path = os.path.join(os.getenv("KRAKEN_DATA_PATH"),'OHLC',time_frame,ticker+'.csv')

        # CHECK FOR THE CURRENT DIRS
        if not os.path.exists(time_frame_path):
            self.dir_checks()

        # GET MAIN_DF
        if os.path.exists(ticker_path):
            main_df = pd.read_csv(ticker_path,index_col=0)
            main_df.Date = pd.to_datetime(main_df.Date)
            if update==False:
                return main_df
        else:
            main_df = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'VWAP', 'Volume', 'Trades'])

        # GET Last CheckPoint
        if ticker in all_since.index.tolist():
            last = all_since.at[ticker,time_frame]
            del all_since
        else:
            last = None

        # GET LATEST DATA
        df,since = self.market_candles(ticker,time_frame,last)

        # APPEND TO MAIN_DF and CLEAN UP DUPLICATES
        main_df = main_df.append(df,ignore_index=True)
        main_df = main_df[~main_df['Date'].duplicated(keep='last')]
        main_df.sort_values('Date',inplace=True)

        # SAVE TO MAIN_DF
        main_df.to_csv(ticker_path)

        # UPDATE LAST CHECKPOINT
        checkpoint = self.OHLC_last_update(ticker,time_frame,since,True)
        # UPDATE TOTAL NUMBER OF ROWS
        total_rows = len(main_df)
        self.OHLC_rows_count(ticker,time_frame,total_rows,True)
        print("*"*50)
        print(f"Ticker: {ticker}")
        print(f"Last CheckPoint: {checkpoint.at[ticker,time_frame]}")
        print(f"Total PTS: {total_rows}")
        print("*"*50)
        return main_df

    def quote(self,pair):
        try:
            resp = requests.get("https://api.kraken.com/0/public/Ticker",params={"pair":pair})    
            data = resp.json()['result']
            data = list(data.values())[0]
            values = {
                        'a':'Ask',
                        'b':'Bid',
                        'c':'Close',
                        'v':"Volume",
                        'p':'VWAP',
                        't':'Trades',
                        'l':"Low",
                        'h':'High',
                        'o':'Open'
                    }
            stats = {}
            for i in values:
                if type(data[i])==list:
                    stats[values[i]] = float(data[i][0])
                else:
                    stats[values[i]] = float(data[i])
            return stats
        except:
            print(f"{pair} Quote Failed!")
            return {
                'Ask': 0,
                'Bid': 0,
                'Close': 0,
                'Volume': 0,
                'VWAP': 0,
                'Trades': 0,
                'Low': 0,
                'High': 0,
                'Open': 0
            }

    def archived_resample_ohlc(self,time_frame='1H',conversion_time_frame='4H'):
        """Place Archived Kraken Dump"""
        # GET 1H CANDLESTICK DATA AND CONVERT TO HIGHER TIMEFRAMES>> 4H

        # INIT
        tickers = self.get_TradeablePairsByQuote().altname.tolist()
        dump_path = os.path.join(os.getenv("KRAKEN_DATA_PATH"),'Temp','OHLC_Dump')
        ohlc_dump_path = [os.path.join(dump_path,t) for t in os.listdir(dump_path) if t.split("_")]
        
        time_interval = self.time_frames[time_frame]
        
        converted_path = os.path.join(os.getenv("KRAKEN_DATA_PATH"),'Temp','OHLC_Converted',conversion_time_frame)

        if not os.path.exists(converted_path):
            os.makedirs(converted_path)


        ohlc = {
            'Open':'first',
            'High':'max',
            'Low':'min',
            'Close':'last',
            'Volume':'sum',
            'Trades':'sum'
        }

        for f in ohlc_dump_path:
            try:
                ticker = os.path.basename(f).split("_")[0]+'USD'
                print(f"Processing {ticker}")
                file_name = f"{ticker}_{time_interval}.csv"
                ticker_file_path = os.path.join(f,file_name)

                # Columns
                COLS =['Date','Open','High','Low','Close','Volume','Trades']
                # Main
                df = pd.read_csv(ticker_file_path,names=COLS,header=None) 

                # Make sure all values are float
                df = df.astype(float)
                # Convert Date Column to DateTime
                df.Date = df.Date.apply(lambda x:datetime.fromtimestamp(x))
                # Sort DataFrame by Date
                df.sort_values('Date',inplace=True)
                #Set Date as Index
                df.set_index('Date',inplace=True)

                if time_frame!=conversion_time_frame:
                    n_df = df.resample(f"{self.time_frames[conversion_time_frame]}min").apply(ohlc)
                    # CHECKING FOR NAN VALUES
                    n_df.drop(n_df[n_df.Open.isnull()].index,inplace=True)
                else:
                    n_df = df.copy()

                # Add VWAP Column
                if 'VWAP' not in df.columns.tolist():
                    n_df['VWAP'] = None

                n_df.to_csv(os.path.join(converted_path,file_name.split("_")[0]+'.csv'))
            except:
                print(f"Skipping {f}")
        print("\n!!!Initializing Appeding to Live DataSource....\n")
        self.update_from_converted(conversion_time_frame)
    
    def update_from_converted(self,time_frame='4H'):

        tradable_tickers = self.get_TradeablePairsByQuote().altname.tolist()

        converted_path = os.path.join(os.getenv("KRAKEN_DATA_PATH"),'Temp','OHLC_Converted',time_frame)

        tickers_path = [os.path.join(converted_path,p) for p in os.listdir(converted_path)]

        for ticker_path in tickers_path:

            ticker = os.path.basename(ticker_path).rstrip(".csv")

            if ticker in tradable_tickers:
                print(f"Updating: {ticker}")

                main_df = pd.read_csv(ticker_path,parse_dates=True)

                main_df.Date = pd.to_datetime(main_df.Date)

                df = self.ohlc_update(ticker,time_frame)

                main_ticker_path = os.path.join(os.getenv("KRAKEN_DATA_PATH"),'OHLC',time_frame,ticker+'.csv')

                # APPEND TO MAIN_DF and CLEAN UP DUPLICATES
                main_df = main_df.append(df,ignore_index=True)
                main_df = main_df[~main_df['Date'].duplicated(keep='last')]
                main_df.sort_values('Date',inplace=True)

                # SAVE TO MAIN_DF
                main_df.to_csv(main_ticker_path)
            else:
                print(f"Skipping {ticker}")


if __name__=='__main__':
    market = Kraken_OHLC()
    market.archived_resample_ohlc('5m','5m')
