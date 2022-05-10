import requests
import os
import pandas as pd
import json


class KrakenDB:
    def __init__(self):
        self.last_update = {}
    
    def getAllAssetPairs(self, update=False):
        PAIRS_DB_PATH = os.path.join(
            os.getenv("KRAKEN_DATA_PATH"),'Database','AllTradedPairs.csv'
        )
        if not os.path.exists(os.path.dirname(PAIRS_DB_PATH)) or update:
            r = requests.get("https://api.kraken.com/0/public/AssetPairs")
            r = r.json()['result']
            COLS = list(r.keys())
            all_pairs = pd.DataFrame(r.values())
            if os.path.exists(os.path.dirname(PAIRS_DB_PATH)):
                all_pairs.to_csv(PAIRS_DB_PATH)
            else:
                print("DB Path not found for Archiving...")
        elif os.path.exists(PAIRS_DB_PATH):
            all_pairs = pd.read_csv(PAIRS_DB_PATH,index_col=0)
        else:
            print("Data Not Found: Enable 'Update' Parameter")
            all_pairs = self.getAllAssetPairs(True)
            # all_pairs = None
        return all_pairs
    
    def get_TradeablePairsByQuote(self,_quote='ZUSD'):
        pairs_df = self.getAllAssetPairs()
        pairs_df = pairs_df[pairs_df.quote==_quote]

        # GET TRADABLE TICKERS (ONLY)
        non_traded = os.path.join(os.getenv("KRAKEN_DATA_PATH"),'Database','non-tradable-pairs.txt')

        with open(non_traded,'r') as f:
            d = f.read()
            f.close()

        n_tickers = [t.lstrip().rstrip() for t in d.rstrip("\n").split(",")]
        pairs_df = pairs_df[pairs_df.base.apply(lambda x: x not in n_tickers)]
        return pairs_df

    def get_ticker_info(self,ticker):
        pairs_df = self.get_TradeablePairsByQuote()
        ticker_info = pairs_df[pairs_df.altname==ticker]
        if len(ticker_info)==1:
            return ticker_info.iloc[0]
        else:
            return 'Ticker Not Found In Tradeable Tickers DB'
    
    def OHLC_rows_count(self,ticker=None,time_frame=None,total_rows=None, update=False):
        """
        Get CSV of all the tickers with total rows available
        """
        ohlc_count_path = os.path.join(os.getenv("KRAKEN_DATA_PATH"),'Database','BigOHLCFiles.csv')
        if os.path.exists(ohlc_count_path):
            ohlc_count = pd.read_csv(ohlc_count_path,index_col=0)
        else:
            ohlc_count = pd.DataFrame(columns=['Tickers']+list(self.time_frames.keys()))
            ohlc_count.set_index('Tickers',inplace=True)
        
        if update and ticker!=None and time_frame!=None and total_rows!=None:
            ohlc_count.at[ticker,time_frame] = total_rows
            ohlc_count.to_csv(ohlc_count_path)
        return ohlc_count

    
    def OHLC_last_update(self,ticker=None,time_frame=None,since=None,update=False):
        """
        Get CSV of all the tickers with last updated time value
        """
        # GET SINCE FILE
        last_update_path = os.path.join(os.getenv("KRAKEN_DATA_PATH"),'Database','last_update.csv')
        if os.path.exists(last_update_path):
            last_update = pd.read_csv(last_update_path,index_col=0)
        else:
            last_update = pd.DataFrame(columns=['Tickers']+list(self.time_frames.keys()))
            last_update.set_index('Tickers',inplace=True)
        
        if update and ticker!=None and time_frame!=None and since!=None:
            last_update.at[ticker,time_frame] = since
            last_update.to_csv(last_update_path)
        return last_update

    """ 
    #########################DEPRECATED#########################
    # def update_OHLC_rows(self, ticker,_time_frame,row_count:int):
    #     timeframes_path = os.path.join(os.getenv("KRAKEN_DATA_PATH"),'OHLC')
    #     time_frames = os.listdir(timeframes_path)
    #     ohlc_count_path = os.path.join(os.getenv("KRAKEN_DATA_PATH"),'Database','BigOHLCFiles.json')
    #     ohlc_count = self.get_OHLC_rows()
    #     # PRELIMINARY CHECKS
    #     c = 0
    #     for time_frame in time_frames:
    #         if time_frame not in ohlc_count:
    #             ohlc_count[time_frame] = {}
    #             c+=1
                
    #     # CHECK IF TICKER IN OHLC_COUNT
    #     if ticker in list(ohlc_count[_time_frame].keys()):
    #         ohlc_count[_time_frame][ticker] = row_count
    #         c+=1
    #     else:
    #         tickers = list(ohlc_count[_time_frame].keys())
    #         if len(tickers)==0:
    #             ohlc_count[_time_frame] = {ticker:row_count}
    #             c+=1
    #         else:
    #             ohlc_count[_time_frame][ticker] = row_count
    #             c+=1

    #     if c>0:
    #         os.remove(ohlc_count_path)
    #         with open(ohlc_count_path,'w') as f:
    #             f.write(json.dumps(ohlc_count))
    #             f.close()
    #     return ohlc_count

    # def get_last_update(self):
    #     # GET SINCE FILE
    #     last_update_path = os.path.join(os.getenv("KRAKEN_DATA_PATH"),'Database','last_update.json')
    #     with open(last_update_path,'r') as f:
    #         data = f.read()
    #         if "{" in data:
    #             last_update = json.loads(data)
    #         elif type(data)==dict:
    #             last_update = data
    #         else:
    #             last_update = {}
    #         f.close()
    #     return last_update

    # def update_lastUpdate_json(self,ticker,_time_frame,last):
    #     # UPDATE LAST_UPDATE JSON FILE
    #     timeframes_path = os.path.join(os.getenv("KRAKEN_DATA_PATH"),'OHLC')
    #     time_frames = os.listdir(timeframes_path)
    #     last_update_path = os.path.join(os.getenv("KRAKEN_DATA_PATH"),'Database','last_update.json')
    #     last_update = self.get_last_update()

    #     # PRELIMINARY CHECKS
    #     c = 0
    #     # for time_frame in time_frames:
    #     if _time_frame not in last_update:
    #         print(f"!!!New TimeFrame Adding {ticker} {_time_frame}")
    #         last_update[_time_frame] = {}
    #         c+=1

    #     # CHECK IF TICKER IN OHLC_COUNT
    #     if ticker in list(last_update[_time_frame].keys()):
    #         last_update[_time_frame][ticker] = last
    #         c+=1
    #     else:
    #         tickers = list(last_update[_time_frame].keys())
    #         if len(tickers)==0:
    #             if (ticker is not None) and (last is not None):
    #                 last_update[_time_frame] = {ticker:last}
    #                 c+=1
    #         else:
    #             last_update[_time_frame][ticker] = last
    #             c+=1

    #     if c>0:
    #         os.remove(last_update_path)
    #         with open(last_update_path,'w') as f:
    #             f.write(json.dumps(last_update))
    #             f.close()
    #     return last_update 
    """


if __name__=='__main__':
    k_db = KrakenDB()
    print(k_db.getAllAssetPairs(update=True))
    