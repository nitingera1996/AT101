import os
from time import sleep

from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException
from binance.websockets import BinanceSocketManager
from twisted.internet import reactor


# Account init

TEST_MODE = True

env_variable_prefix = "demo" if TEST_MODE else "prod"

api_key = os.environ.get(env_variable_prefix + '_binance_api')
api_secret = os.environ.get(env_variable_prefix + '_binance_secret')
client = Client(api_key, api_secret)
if TEST_MODE:
    client.API_URL = 'https://testnet.binance.vision/api'

price = {'BTCUSDT': None, 'error': False}


def btc_pairs_trade(msg):
    ''' define how to process incoming WebSocket messages '''
    if msg['e'] != 'error':
        price['BTCUSDT'] = float(msg['c'])
    else:
        price['error'] = True


def execute_script():
    # Start Websocket
    bsm = BinanceSocketManager(client)
    conn_key = bsm.start_symbol_ticker_socket('BTCUSDT', btc_pairs_trade)
    bsm.start()

    while not price['BTCUSDT']:
        # wait for WebSocket to start streaming data
        sleep(0.1)

    while True:
        # error check to make sure WebSocket is working
        if price['error']:
            # stop and restart socket
            bsm.stop_socket(conn_key)
            bsm.start()
            price['error'] = False

        else:
            print("BTCUSDT Price: ", price['BTCUSDT'])
            if price['BTCUSDT'] > 40000:
                try:
                    order = "Dummy order"
                    #order = client.order_market_buy(symbol='ETHUSDT', quantity=1)
                    print(order)
                    break
                except BinanceAPIException as e:
                    # error handling goes here
                    print(e)
                except BinanceOrderException as e:
                    # error handling goes here
                    print(e)
        sleep(0.1)

    bsm.stop_socket(conn_key)
    reactor.stop()
