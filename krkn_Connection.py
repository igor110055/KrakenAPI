import os
import requests
import urllib.parse
import hashlib
import hmac
import base64
import pandas as pd
from datetime import datetime


class Kraken_Connection:
    def __init__(self,accName="lux"):
        '''
        accName must be the exact same name as the file in .t_strj/
        '''
        # self.api_sec = os.getenv("API_SEC_KRAKEN")
        self.api_url = 'https://api.kraken.com'
        self.api_key , self.api_sec = self.stash(accName)
    
    @staticmethod
    def get_kraken_signature(urlpath, data, secret):
        postdata = urllib.parse.urlencode(data)
        encoded = (str(data['nonce']) + postdata).encode()
        message = urlpath.encode() + hashlib.sha256(encoded).digest()
        mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
        sigdigest = base64.b64encode(mac.digest())
        return sigdigest.decode()
        
    # GENERIC CALL
    def kraken_request(self,uri_path, data):
        headers = {}
        headers['API-Key'] = self.api_key
        # get_kraken_signature() as defined in the 'Authentication' section
        headers['API-Sign'] = self.get_kraken_signature(uri_path, data, self.api_sec)             
        req = requests.post((self.api_url + uri_path), headers=headers, data=data)
        return req

    @staticmethod
    def stash(_name):
        _k_p = os.path.join(os.getenv("HOME"),'krakenData','Database','.t_strj')
        accs = [os.path.join(_k_p,x) for x in os.listdir(_k_p) if 'template' not in x]
        for acc in accs:
            ax_name = os.path.basename(acc)
            if ax_name == _name:
                with open(acc,'r') as f:
                    d = f.readlines()
                    d = [x.strip() for x in d]
                    f.close()
                return d

    def serverTime(self):
        resp = requests.get('https://api.kraken.com/0/public/Time')
        resp['result']['unixtime'] = datetime.fromtimestamp(resp['result']['unixtime'])
        return resp.json()


if __name__ == '__main__':
    accountName = input("Enter an account name: ")
    k = Kraken_Connection(accountName)
    k.stash()