from datetime import datetime as dt_datetime, timedelta
import time
import threading
import logging
import pandas as pd
from NorenRestApiPy.NorenApi import NorenApi
import pyotp
import yaml
import math
import concurrent.futures

# Firebase Configuration


class ShoonyaApiPy(NorenApi):
    def __init__(self):
        super().__init__(host='https://api.shoonya.com/NorenWClientTP/', websocket='wss://api.shoonya.com/NorenWSTP/')

# Initialize API
api = ShoonyaApiPy()

# Load credentials and login
with open('cred.yml') as f:
    cred = yaml.safe_load(f)

TOKEN = cred['factor2']
otp = pyotp.TOTP(TOKEN).now()
ret = api.login(
    userid=cred['user'],
    password=cred['pwd'],
    twoFA=otp,
    vendor_code=cred['vc'],
    api_secret=cred['apikey'],
    imei=cred['imei']
)

if ret:
    print("Login Successful")
else:
    print("Login Failed")
    exit()

# Fetch Bank Nifty spot price
try:
    # Use correct symbol format (original code had 'Nifty Bank')
    banknifty = api.get_quotes('NSE', 'Nifty Bank')
    if not banknifty or 'lp' not in banknifty:
        raise ValueError("Invalid response from API")

    bnfltp = float(banknifty['lp'])
    print(f"Bank Nifty LTP: {bnfltp}")
    bnfltp = float(banknifty['lp'])
    mod = int(bnfltp) % 100

    if mod < 50:
        bnfstk = int(math.floor(bnfltp / 100)) * 100
    else:
        bnfstk = int(math.ceil(bnfltp / 100)) * 100

except Exception as e:
    print(f"Error fetching Bank Nifty price: {e}")
    print("Response received:", banknifty)
    exit()

# Fetch option chain (corrected parameters)
chain = api.get_option_chain(exchange='NFO', tradingsymbol='BANKNIFTY27FEB25P49100', strikeprice=bnfstk, count=5)

if not chain or 'values' not in chain or len(chain['values']) == 0:
    print("Error: No option chain data found.")
    exit()

# Concurrently fetch quotes using ThreadPoolExecutor
bnf = []
def fetch_quote(scrip):
    try:
        return api.get_quotes(exchange=scrip['exch'], token=scrip['token'])
    except Exception as e:
        logging.error(f"Error fetching quote: {e}")
        return None

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(fetch_quote, scrip): scrip for scrip in chain['values']}
    for future in concurrent.futures.as_completed(futures):
        result = future.result()
        if result:
            bnf.append(result)

# Create DataFrame
df = pd.DataFrame(bnf, columns=['tsym','token','optt', 'lp', 'ap', 'diff', 'writing', 'bp1', 'oi'])
bn = pd.DataFrame(bnf)
bn=bn.sort_values(by=['strprc'], axis=0, ascending=False, inplace=False,)
bn = pd.DataFrame(bnf, columns=['tsym','token','optt','lp', 'ap','diff','strprc','oi'])
bn[["lp", "ap"]] = bn[["lp", "ap"]].apply(pd.to_numeric)
bn['diff'] = bn['lp'] - bn['ap']
bnce = bn.query('optt == "CE"')
bnpe = bn.query('optt == "PE"')
res2 = pd.concat([bnce, bnpe])
bn2= bnce.sort_values("strprc", axis = 0, ascending = False,inplace = False)
bn3= bnpe.sort_values("strprc", axis = 0, ascending = False,inplace = False)
nfspot= api.get_quotes(exchange='NSE', token='26000')
bnfspot=api.get_quotes(exchange='NSE', token='26009')
frames2 = [bn2,bn3]
con2 = pd.concat(frames2)
csvoi= con2.to_csv("dataframe.csv", index=False)
csvs=pd.read_csv("dataframe.csv")
highoi= csvs.loc[csvs['oi'].idxmin()]
buyopt1 =highoi['tsym']

ret2 = api.place_order(buy_or_sell='B', product_type='C',
                        exchange='NFO', tradingsymbol=buyopt1,
                        quantity=30, discloseqty=0,price_type='MKT', price=0, trigger_price=None,
                        retention='DAY', remarks='my_order_001')


if ret2:
    print("Order Successful")
else:
    print("Order Failed")
    exit()
