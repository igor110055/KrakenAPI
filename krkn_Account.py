import pandas as pd
from datetime import datetime
import time



from krkn_Connection import Kraken_Connection

class Kraken_Account(Kraken_Connection):
    def __init__(self,accName='lux'):
        Kraken_Connection.__init__(self,accName)
        self.accName = accName

    def ledger_positions(self):
        resp = self.kraken_request("/0/private/Ledgers",{
            "nonce": str(int(1000*time.time()))
        })
        resp = resp.json()
        ledger = pd.DataFrame(resp['result']['ledger']).T
        ledger.time = ledger.time.apply(lambda x: datetime.fromtimestamp(x))
        ledger[['amount','balance','fee']] = ledger[['amount','balance','fee']].astype(float)
        return ledger

    def trade_histrory(self):
        total = 0
        resp = self.kraken_request('/0/private/TradesHistory',{
            "nonce": str(int(1000*time.time())),
            "trades": True
            })
        total+=1
        print(f'Calls: {total}      ',end='\r')
        data = resp.json()
        trades_df = pd.DataFrame()
        if 'result' in data.keys():
            trades_data = [data['result']['trades'][t] for t in data['result']['trades']]
            trades_df = pd.DataFrame(trades_data)
            count = data['result']['count']
            total_pages = count % 50
            print(f"TOTAL PAGES: {total_pages}")
            # for i in range(1,total_pages+1):
            #     resp = self.kraken_request('/0/private/TradesHistory',{
            #         "nonce": str(int(1000*time.time())),
            #         "trades": True,
            #         'ofs':i
            #         })
            #     total+=1
            #     print(f'Calls: {total}      ',end='\r')
            #     data = resp.json()
            #     if 'result' in data.keys():
            #         print("MORE TRADES FOUND")
            #         trades_data = [data['result']['trades'][t] for t in data['result']['trades']]
            #         trades_df.append(pd.DataFrame(trades_data),ignore_index=True)
            #     time.sleep(0.5)
        return trades_df 
    #**BALANCE**
    def balance(self):
        try:
            resp = self.kraken_request('/0/private/Balance', {
                "nonce": str(int(1000*time.time()))
            })
            data = resp.json()
            assets = data['result']
            return {a:float(assets[a]) for a in assets}
        except:
            print("!!! Balane Request Failed...")
            return {}
    
    #**Total BALANCE - In Quote Currency
    def total_balance(self,asset='ZUSD'):
        resp = self.kraken_request('/0/private/TradeBalance',{
            "nonce": str(int(1000*time.time())),
            "asset": asset.upper()
        })
        return float(resp.json()['result']['eb'])
    
    #**CURRENT POSITIONS - Margin Trading ONLY
    def positions(self):
        resp = self.kraken_request('/0/private/OpenPositions',{
            "nonce": str(int(1000*time.time())),
            "docalcs":True
        })
        return resp.json()
    
if __name__=='__main__':
    acc_client = Kraken_Account()
    print(acc_client.total_balance('ZCAD'))