# KrakenAPI
Kraken REST API framework to access account, market data and placing trades
___
## **Connection Method**
In order to connect to the kraken exchange and safely store the API key and secret.

Create the following Directory:
```
> cd $HOME
> mkdir krakenData
> cd krakenData
> mkdir Database
> cd Database
> mkdir .t_strj
> cd .t_strj
```
Create a file for the each account.
e.g.

```touch accountname```

where 'accoutname' is the name of the file
Use texteditor (nano/vim) to add your API KEY as follows:
```
>> nano accountname
API_KEY
API_SECRET
```
Paste your API KEY and API SECRET on line1 and line2 respectively

_This 'accountname' is case sensitive as it will be used in the program to access the file for api key and secret_




