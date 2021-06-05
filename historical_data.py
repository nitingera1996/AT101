import os

import pandas as pd
from binance.client import Client

# Account init
TEST_MODE = True

env_variable_prefix = "demo" if TEST_MODE else "prod"

api_key = os.environ.get(env_variable_prefix + '_binance_api')
api_secret = os.environ.get(env_variable_prefix + '_binance_secret')
client = Client(api_key, api_secret)
if TEST_MODE:
    client.API_URL = 'https://testnet.binance.vision/api'


def execute_script():
    # valid intervals - 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M

    # fetch 30 minute klines for the last month of 2017
    # klines = client.get_historical_klines("ETHBTC", Client.KLINE_INTERVAL_30MINUTE, "1 Dec, 2017", "1 Jan, 2018")

    # request historical candle (or klines) data
    klines = client.get_historical_klines('BTCUSDT', Client.KLINE_INTERVAL_1MINUTE, "7 days ago UTC", limit=15000)

    # delete unwanted data
    for kline in klines:
        del kline[6:8]
        del kline[7:]

    # create a Pandas DataFrame and export to CSV
    btc_df = pd.DataFrame(klines, columns=['date', 'open', 'high', 'low', 'close', 'Volume', 'NumberOfTrades'])
    btc_df.set_index('date', inplace=True)
    print(btc_df.head())
    # export DataFrame to csv
    btc_df.to_csv('btc_bars3.csv')
