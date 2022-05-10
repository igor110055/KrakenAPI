import os
import json
import pandas as pd
from datetime import datetime
from tqdm import tqdm


class Kraken_OHLC_Archive:
    """To Handle Karken Archived File Download via:
    https://support.kraken.com/hc/en-us/articles/360047124832-Downloadable-historical-OHLCVT-Open-High-Low-Close-Volume-Trades-data
    """
    def __init__(self) -> None:
        pass
    def read_archived_ohlc(self,ticker,time_frame):
        """
        Archived CSV File to DataFrame
        """
        # CHECK IF FILE EXISTS
        self.TICKER_PATH = os.path.join(self.DATA_PATH,time_frame,ticker+'.csv')
        if os.path.exists(self.TICKER_PATH):
            main_df = pd.read_csv(self.TICKER_PATH,index_col=0,parse_dates=True)
            update_since_json_path = os.path.join(os.getenv("KRAKEN_DATA_PATH"),'Database','last_update.json')
            if os.path.exists(update_since_json_path):
                with open(update_since_json_path,'r') as f:
                    since_dict = json.loads(f.read())
                    f.close()
                    since = since_dict[ticker]
            else:
                since = 0
        else:
            print(f"Path not Found: {ticker} {time_frame}")
            since = 0
            main_df = pd.DataFrame(columns=self.COLS)
        return main_df, since


    def rawToDataFrame(self,csvFilePath,csvFilePath2=None,_df=None):
        """
        Format Raw CSV Data to Proper Formatting - Archived by kraken
        """
        # Columns
        COLS =['Date','Open','High','Low','Close','Volume','Trades']
        # Main
        df = pd.read_csv(csvFilePath,names=COLS,header=None) 
        if csvFilePath2 or _df:
            # Increment
            _df = pd.read_csv(csvFilePath2,names=COLS,header=None) 
            df = df.append(_df,ignore_index=True)
            df = df[~df['Date'].duplicated(keep='last')]
        # Make sure all values are float
        df = df.astype(float)
        # Convert Date Column to DateTime
        df.Date = df.Date.apply(lambda x:datetime.fromtimestamp(x))
        # Sort DataFrame by Date
        df.sort_values('Date',inplace=True)
        # Add VWAP Column
        if 'VWAP' not in df.columns.tolist():
            df['VWAP'] = None
        #Set Date as Index
        df.set_index('Date',inplace=True)
        return df

    def manual_OHLC_dump(self,full_update=True):
        """
        Full Download and Appending Increment as well as Full archive
        """
        if full_update:
            # GET THE ENTIRE ARCHIVED DIR AND FILTER USD PAIRS
            # GET USD PAIRS DATA 
                # APPEND INCREMENT FOLDER DATA FOR THE PAIR
                # MOVE IT TO THE GOOGLE DRIVE
            ohlc_root_path = input("Enter OHLC Root: ") or '/Volumes/My Passport/Crypto/kraken-OHLC'

            full_ohlc_path = os.path.join(ohlc_root_path,'Kraken_OHLCVT')
            incre_ohlc_path = os.path.join(ohlc_root_path,'Kraken_OHLCVT_Q4_2021')

            full_all_usd_path = [os.path.join(full_ohlc_path,t) for t in os.listdir(full_ohlc_path) if 'USD' in t]
            incre_all_usd_path = [os.path.join(incre_ohlc_path,t) for t in os.listdir(incre_ohlc_path) if 'USD' in t]

            all_paths = []
            for p in tqdm(full_all_usd_path):
                t = os.path.basename(p)
                incre_t = os.path.join(incre_ohlc_path,t)
                if incre_t in incre_all_usd_path:
                    all_paths.append([p,incre_t])

            intervals = {self.time_frames[k]:k for k in self.time_frames}
            
            # READ INCREMENTAL FILE
            # READ FULL ARCHIVED FILES
            for ohlc_file_path, ohlc_file_path_incre in all_paths:
                # FILE NAME - TO MATCH WITH THE FILENAME IN GOOGLE DRIVE
                ohlc_filename = os.path.basename(ohlc_file_path).split("_")[0]+'.csv'
                # GET INTERVAL
                interval = int(os.path.basename(ohlc_file_path).split("_")[1].rstrip(".csv"))
                # Cloud Storage OHLC Archive path
                TICKER_PATH = os.path.join(os.getenv("KRAKEN_DATA_PATH"),'OHLC',intervals[interval],ohlc_filename)
                # Convert Data to Right Format
                df = self.rawToDataFrame(ohlc_file_path,ohlc_file_path_incre)
                # Save DF
                df.to_csv(TICKER_PATH)
        else:
            return None
