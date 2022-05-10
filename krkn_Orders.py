from datetime import datetime
import pandas as pd
import time
import os
import sys
import ast

sys.path.append(os.getenv("KRAKEN_PATH"))

from krkn_OHLC import Kraken_OHLC
from krkn_Account import Kraken_Account

class Kraken_Orders(Kraken_Account,Kraken_OHLC):
    def __init__(self,accName="lux"):
        Kraken_Account.__init__(self,accName)
        Kraken_OHLC.__init__(self)
        self.accName = accName
        self.unique_tickers = {
            'DOGEUSD':"XDGUSD",
            'BTCUSD':"XBTUSD"
            }
    
    #**OPEN ORDERS**
    def open_orders(self):
        resp = self.kraken_request('/0/private/OpenOrders', {
            "nonce": str(int(1000*time.time())),
            "trades": True
        })
        resp = resp.json()
        for _id in resp['result']['open']:
            resp['result']['open'][_id]['opentm'] = datetime.fromtimestamp(resp['result']['open'][_id]['opentm'])
        return resp
    
    def get_closed_orders(self):
        """
        Retrieve information about orders that have been closed (filled or cancelled). 50 results are returned at a time, the most recent by default.
        """
        resp = self.kraken_request('/0/private/ClosedOrders', {
            "nonce": str(int(1000*time.time()))
        })
        return resp.json()
    
    def cancel_order(self,ticker,cancel_type='any'):
        """
        # ticker: Trading Pair
        # cancelType: buy, sell, all
        """
        orders_cancelled = 0
        openOrders = self.open_orders()
        if len(openOrders['error'])== 0 and len(openOrders['result']['open'])!=0:
            for _id in openOrders['result']['open']:
                order_id = 0
                order_desc = openOrders['result']['open'][_id]['descr']
                pair = order_desc['pair']
                orderType = order_desc['type']
                if ticker==pair and (cancel_type=='any' or cancel_type==orderType):
                    order_id = _id

                if order_id!=0:
                    fullDescr = openOrders['result']['open'][_id]['descr']['order']
                    print(f"Cancelling: {fullDescr}")
                    resp = self.kraken_request('/0/private/CancelOrder', {
                        "nonce": str(int(1000*time.time())),
                        "txid": order_id})
                    orders_cancelled+=1
            if orders_cancelled>0:
                print(f"Total Orders Cancelled: {orders_cancelled}")
            else:
                print(f'No Open Orders found for {ticker}!')
        else:
            print(f"No Open Orders found!")
        return orders_cancelled
            
    def cancelAllOrders(self):
        resp = self.kraken_request('/0/private/CancelAll', {
            "nonce": str(int(1000*time.time()))
        })
        return resp.json()
        
    def placeLimitOrder(self,ticker,signal,price,amount=None,perc=100):
        """
        ***NOTE: ONLY TRADING ALL CRYPTOS AGAINST USD (actual USD Dollars)***
        2 - requests per call
        ticker - pair XBTUSD
        signal - buy/sell
        price - quote currency USD
        amount - base currency XBT (volume-base)
        volume-quote - put quantity in Quote Currency
        perc - percentage buy/sell (only for sell orders for now)
            buy - get "x" perc of USD and buy XBT
            sell - get "x" perc of XBT and buy USD
        """
        # INIT 
        pairs_df = self.get_TradeablePairsByQuote('ZUSD')
        # TRADING PAIR EXISTS
        ticker_info = pairs_df[pairs_df.altname==ticker]
        if len(ticker_info)==1:
            ticker_info = ticker_info.iloc[0]
            # BASE PRECISION
            price_precision = ticker_info.pair_decimals
            # QUOTE PRECISION
            quantity_precision = ticker_info.lot_decimals
            # MIN QTY
            minOrderSize = ticker_info.ordermin
        else:
            return {"Error":f"{ticker} NOT Tradeable!"}
        
        balances = self.balance()
            
        # SELLING using % value
        if not amount and signal=='sell':
            base_amount = None
            if len(ticker_info)==1:
                ticker_info = ticker_info.iloc[0]

                # For selling the base currency
                if ticker_info['base'] in balances['result']:
                    base_amount = float(balances['result'][ticker_info['base']])
                else:
                    print("Base Currency: 0 (balance)")
            if base_amount and signal=='sell':
                # updating amount
                amount = base_amount*perc/100
                
            if amount is None and signal=='sell':
                return {'Error':"Asset Not Found"} 
            
        # BUYING using % value
        if 'result' in balances.keys() and signal=='buy':
            if 'ZUSD' in balances['result'].keys():
                amount = float(balances['result']['ZUSD'])*perc/100
            else:
                return {"Error":"0 ZUSD (too poor to make this trade!)"}
        else:
            return {"Error":"Balance Call Failed!"}
            
        if amount>0:    
            resp = self.kraken_request('/0/private/AddOrder', {
                        "nonce": str(int(1000*time.time())),
                        "ordertype": "limit",
                        "type": signal,
                        "volume": str(amount),
                        "pair": ticker,
                        "price": str(price)
                    })
            return resp.json()
        else:
            return {'Message':'No amount specified {ticker} {signal}'}
                
    def limitOrder(self,ticker,signal,price,amount,_type='perc',reset=False):
        
        # GET Ticker Price $
        _quote = self.quote(ticker)
        last_price = _quote['Close']
        # INIT 
        pairs_df = self.get_TradeablePairsByQuote('ZUSD')
        if ticker in self.unique_tickers:
            ticker = self.unique_tickers[ticker]
            ticker_info = pairs_df[pairs_df.altname==ticker]
        else:
            # TRADING PAIR EXISTS
            ticker_info = pairs_df[pairs_df.altname==ticker]
        if len(ticker_info)==1:
            ticker_info = ticker_info.iloc[0]
            # BASE PRECISION
            price_precision = ticker_info.pair_decimals
            # QUOTE PRECISION
            quantity_precision = ticker_info.lot_decimals
            # MIN QTY
            minOrderSize = ticker_info.ordermin
        else:
            print("!!!!!!!!")
            return {"Error":f"{ticker} NOT Tradeable!"}

        balances = self.balance()
        if reset:
            self.cancel_order(ticker)

        if signal=='sell':
            if ticker_info.base in balances:
                asset_amount = float(balances[ticker_info.base])
            else:
                asset_amount = 0
                return {f"Error":f"{ticker_info.base} Not Found"}
            if _type=='perc':
                # amount=1 #100%
                qty = asset_amount * amount
            elif _type=='dollar':
                qty = amount/self.quote(ticker)['Close']
                if qty<asset_amount:
                    return {"Error":f"Qty Entered ({qty}) > Asset Available ({asset_amount})"}
            qty = round(qty,quantity_precision)

        if signal=='buy':
            if ticker_info.quote in balances:
                asset_amount = float(balances[ticker_info.quote])
            else:
                asset_amount = 0
                return {"Error":"Too Poor to Make this Trade"}
            if _type=='perc':
                # GET CASH
                if amount==1:
                    amount=0.95
                asset_amount*=amount
            if amount > asset_amount:
                print(f'${amount} Vs. Balance: ${asset_amount}')
                return {"Error":f"Not Enough Funds {ticker_info.quote}"}
            
            if last_price!=0:
                qty = asset_amount/last_price
                qty = round(qty,quantity_precision)
            else:
                print(f"{ticker}>> Price:${last_price}")
                return {'Error':"Issue Getting Last Price"}

        if qty>minOrderSize:    
            fees = ast.literal_eval(ticker_info.fees)[0][1]/100
            # UPDATED QTY
            # qty*=(1-fees)
            resp = self.kraken_request('/0/private/AddOrder', {
                        "nonce": str(int(1000*time.time())),
                        "ordertype": "limit",
                        "type": signal,
                        "volume": str(qty),
                        "pair": ticker,
                        "price": str(round(price,price_precision)),
                        'oflags':'post'
                    })
            if 'result' in resp.json():
                if 'txid' in resp.json()['result']:
                    self.log_trade(ticker,signal,qty,last_price)
            print(resp.json())
            return resp.json()
        else:
            print({'Message':f'No amount specified {ticker} {signal}'})
            return {'Message':f'No amount specified {ticker} {signal}'}


    def marketOrder(self,ticker,signal,amount,_type,reset=False):
        # GET Ticker Price $
        _quote = self.quote(ticker)
        last_price = _quote['Close']
        # INIT 
        pairs_df = self.get_TradeablePairsByQuote('ZUSD')
        # TRADING PAIR EXISTS
        ticker_info = pairs_df[pairs_df.altname==ticker]
        if len(ticker_info)==1:
            ticker_info = ticker_info.iloc[0]
            # BASE PRECISION
            price_precision = ticker_info.pair_decimals
            # QUOTE PRECISION
            quantity_precision = ticker_info.lot_decimals
            # MIN QTY
            minOrderSize = ticker_info.ordermin
        else:
            print("!!!!!!!!")
            return {"Error":f"{ticker} NOT Tradeable!"}

        balances = self.balance()
        if reset:
            self.cancel_order(ticker)

        if signal=='sell':
            if ticker_info.base in balances:
                asset_amount = float(balances[ticker_info.base])
            else:
                asset_amount = 0
                return {f"Error":"{ticker_info.base} Not Found"}
            if _type=='perc':
                # amount=1 #100%
                qty = asset_amount * amount
            elif _type=='dollar':
                qty = amount / last_price
            qty = round(qty,quantity_precision)

        if signal=='buy':
            if _type=='perc' and ticker_info.quote in balances:
                asset_amount = float(balances[ticker_info.quote])
                # UPDATE CASH QUANTITY
                if amount==1:
                    amount=0.95
                asset_amount*=amount
            elif _type=='dollar':
                asset_amount = amount
            else:
                return {"Error":"Too Poor to Make this Trade"}
            
            if amount > asset_amount:
                print(f'${amount} Vs. Balance: ${asset_amount}')
                return {"Error":f"Not Enough Funds {ticker_info.quote}"}
            
            if last_price!=0:
                qty = asset_amount/last_price*0.99 # 98% OF THE CASH BID ASK CUSHION 2%
                qty = round(qty,quantity_precision)
            else:
                print(f"{ticker}>> Price:${last_price}")
                return {'Error':"Issue Getting Last Price"}

        if qty>minOrderSize:    
            fees = ast.literal_eval(ticker_info.fees)[0][1]/100
            # UPDATED QTY
            # qty*=(1-fees)
            resp = self.kraken_request('/0/private/AddOrder', {
                        "nonce": str(int(1000*time.time())),
                        "ordertype": "market",
                        "type": signal,
                        "volume": str(qty),
                        "pair": ticker,
                        'oflags':'fciq'
                    })
            if 'result' in resp.json():
                if 'txid' in resp.json()['result']:
                    self.log_trade(ticker,signal,qty,last_price)
            print(resp.json())
            return resp.json()
        else:
            # print({'Message':f'No amount specified {ticker} {signal}'})
            return {'Message':f'{ticker} Qty < {minOrderSize} {ticker} {signal} | {qty}'}

    def read_tradelog(self):
        _date = datetime.now()
        t = _date.strftime("%Y-%m-%d")
        tradelog_dir = os.path.join(os.getenv("KRAKEN_DATA_PATH"),'Misc','Extrema','Trades',f'{t}.csv')
        if os.path.exists(tradelog_dir):
            tradelog_df = pd.read_csv(tradelog_dir,index_col=0)
            return tradelog_df
        return 'No tradelogs found'

    def log_trade(self,ticker,signal,qty,price):
        """SAVE TICKER,QTY, SIGNAL, PRICE"""
        _date = datetime.now()
        t = _date.strftime("%Y-%m-%d")
        tradelog_dir = os.path.join(os.getenv("KRAKEN_DATA_PATH"),'Misc','Extrema','Trades',f'{t}.csv')
        if os.path.exists(tradelog_dir):
            tradelog_df = pd.read_csv(tradelog_dir,index_col=0)
        else:
            tradelog_df = pd.DataFrame(columns=['Date','Ticker','Signal','Qty','Price'])
            
        _d = {'Date':_date,'Ticker':ticker,'Signal':signal,'Qty':qty,'Price':price}
        tradelog_df = tradelog_df.append(_d,ignore_index=True)
        tradelog_df.to_csv(tradelog_dir)
        return tradelog_df

    def account_manager(self,pair_basket,close_manager=False):
        OG_PAIR_BASKET = pair_basket.copy()
        # GET BALANCE USD (ASSET)
        totalBalance = self.total_balance()*0.95
        # GET ASSETS
        assets = self.balance()
        assets_dollar = {}
        for a in assets:
            if a in self.pairs_df.base.tolist():
                ticker_info = self.pairs_df[self.pairs_df.base==a].iloc[0]
                ticker = ticker_info.altname
                if ticker in pair_basket.index.tolist():
                    current_price = self.quote(ticker)['Close']
                    asset_dollar_value = current_price*assets[a]
                    if asset_dollar_value>10:
                        assets_dollar[ticker] = asset_dollar_value    
        # GET USD TRADING PAIR OF ASSETS
        assets_info = self.pairs_df[self.pairs_df.base.apply(lambda x: x in list(assets.keys()))]
        assets_info['$'] = assets_info.base.apply(lambda x: float(assets[x]))*assets_info.altname.apply(lambda y:self.quote(y)['Close'] if y in pair_basket.index.tolist() else 0)
        asset_dollar = assets_info[assets_info['$']>10]
        
        exclude_tickers = []
        for asset in assets:
            if asset not in asset_dollar.base.tolist():
                exclude_tickers.append(asset)
        for t in exclude_tickers:
            assets.pop(t)

        for asset in assets:
            assets[asset] = asset_dollar[asset_dollar.base==asset]['$'].iloc[0]
            
        total_executions = 0
        if len(pair_basket)>0:
            print(pair_basket.index.tolist())
            base_names = self.pairs_df[self.pairs_df.altname.apply(lambda x: x in pair_basket.index.tolist())].base.tolist()
            ticker_dollar_size = pair_basket['% Allocation']*totalBalance
            print(ticker_dollar_size)


        exclude_tickers = []
        # CLOSE POSITIONS NOT IN PAIR BASKET
        for asset in assets:
            pair_info = self.pairs_df[(self.pairs_df.base==asset)&(self.pairs_df.quote=='ZUSD')]
            if len(pair_info)>0:
                trading_pair = pair_info.altname.iloc[0]
                if len(pair_basket)==0 or close_manager:
                    print(f"Close Asset: {asset} | {trading_pair}")
                    self.marketOrder(trading_pair,'sell',1,'perc',reset=True)
                    total_executions+=1
                else:
                    try:
                        if asset not in base_names:
                            print(f"Close Asset: {asset} | {trading_pair}")
                            self.marketOrder(trading_pair,'sell',1,'perc',reset=True)
                            total_executions+=1
                            exclude_tickers.append(asset)
                    except:
                        print(f"Skipping Closing: {asset}")
        
        if close_manager:
            print(f"Close Account Manager: TotalExecutions ({total_executions})")
            return {f"Close Account Manager: TotalExecutions ({total_executions})"}

        for t in exclude_tickers:
            assets.pop(t)
        # REBALANCE POSITIONS IN PAIR BASKET BUY OR SELL DEPENDING ON DOLLAR $∆
        for asset in assets:
            try:
                pair_info = self.pairs_df[(self.pairs_df.base==asset)&(self.pairs_df.quote=='ZUSD')]
                if len(pair_info)>0:
                    trading_pair = pair_info.altname.iloc[0]
                if trading_pair in ticker_dollar_size.index.tolist():
                    asset_allocation = ticker_dollar_size[trading_pair]
                    # coins = ast.literal_eval(assets[asset])
                    # asset_dollar = coins * self.quote(trading_pair)['Close']
                    asset_dollar = assets[asset]
                    if abs((asset_dollar/asset_allocation)-1) > 0.1:
                        asset_delta = asset_allocation - asset_dollar
                        signal = 'sell' if asset_delta<0 else 'buy'
                        self.marketOrder(trading_pair,signal,abs(asset_delta),_type='dollar',reset=True)
                        total_executions+=1
                    # Remove Trading Pair from Executing as another order
                    ticker_dollar_size.pop(trading_pair)
            except:
                print(f"Skipping Rebalancing Trade: {asset}")
        
        # DOUBLE CHECK ALLOCATION SIZES
        totalBalance = self.total_balance()*0.95
        
        if len(pair_basket)>0:
            print(ticker_dollar_size)

            # _ticker_dollar_size = pair_basket['% Allocation']*totalBalance
            # ticker_dollar_size = ticker_dollar_size[ticker_dollar_size.index==_ticker_dollar_size.index]

            # PLACE ORDERS FOR THE REMAINDER OF THE ASSET BASKET
            for ticker in ticker_dollar_size.index:
                try:
                    print("NEW TRADE:", ticker,'BUY',ticker_dollar_size[ticker])
                    self.marketOrder(ticker,'buy',ticker_dollar_size[ticker],'dollar',True)
                    total_executions+=1
                except:
                    print(f"Skipping Placing New Trade: {ticker}")

        # CREATING LIMIT ORDERS ONCE ALL THE ORDERS ARE PLACED
        # GET ALL ASSETS
        assets = self.balance()
        assets.pop("ZUSD")
        # GET USD TRADING PAIR OF ASSETS
        assets_info = self.pairs_df[self.pairs_df.base.apply(lambda x: x in list(assets.keys()))]
        assets_info['$'] = assets_info.base.apply(lambda x: float(assets[x]))*assets_info.altname.apply(lambda y:self.quote(y)['Close'])
        asset_dollar = assets_info[assets_info['$']>10]

        lmt_pair_basket = OG_PAIR_BASKET.copy()
        # limit price
        lmt_pair_basket['limitPrice'] = OG_PAIR_BASKET['ExtremaClosePrice']*(1+(1.5*OG_PAIR_BASKET['%Swing']/100))

        filtered_pair_basket = lmt_pair_basket[
            (lmt_pair_basket['CurrentPrice']<lmt_pair_basket['limitPrice'])&
            ((lmt_pair_basket['limitPrice']/lmt_pair_basket['CurrentPrice']-1*100)>=1)
        ]
        
        for t in filtered_pair_basket.index:
            if t in asset_dollar.altname.tolist():
                _limitPrice = filtered_pair_basket.loc[t].limitPrice
                self.limitOrder(t,'sell',_limitPrice,1,'perc',reset=True)
                print(f">> TP Bracket: {t} 100% Sell @ {_limitPrice}")
