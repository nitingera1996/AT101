import os
import csv
from time import sleep
from datetime import timedelta, datetime
from pytz import timezone

from binance.client import Client
from binance.websockets import BinanceSocketManager
from twisted.internet import reactor

# Configs
TEST_MODE = True
SYMBOL = 'BTCUSDT'
TZ = 'Asia/Kolkata'
TIME_FMT = '%m/%d/%Y %H:%M'
DUMP_PATH = 'data/dumps/'
DUMP_FREQ = {'minutes': 1}      # Only hours and minutes supported

# Account init
env_variable_prefix = "demo" if TEST_MODE else "prod"
api_key = os.environ.get(env_variable_prefix + '_binance_api')
api_secret = os.environ.get(env_variable_prefix + '_binance_secret')
client = Client(api_key, api_secret)
if TEST_MODE:
    client.API_URL = 'https://testnet.binance.vision/api'

# Make Path
if not os.path.exists(DUMP_PATH+SYMBOL):
    os.makedirs(DUMP_PATH+SYMBOL)

# Global Variables
socket_response = {'kline': None, 'error': False}
header_row = ['Duration', 'Date', 'Open', 'High', 'Low', 'PriceChange', 'PriceChange%', 'LastPrice', 'LastQuantity', 'BestBidPrice', 'BestBidQuantity', 'BestAskPrice', 'BestAskQuantity', 'NumberOfTrades', 'Volume']
script_start_time = datetime.now(timezone(TZ))
print("Script start time is -", script_start_time.strftime(TIME_FMT))


def dump_to_csv(filename, klines):
    print(f"Dumping {len(klines)} records to filename {filename}")
    with open(filename, 'w+') as f:
        write = csv.writer(f)
        write.writerow(header_row)
        write.writerows(klines)


def socket_response_handler(msg):
    """ Define how to process incoming WebSocket messages """
    if msg['e'] != 'error':
        socket_response['kline'] = [msg['e'], msg['E'], msg['o'], msg['h'], msg['l'], msg['p'], msg['P'], msg['c'], msg['Q'], msg['b'], msg['B'], msg['a'], msg['A'], msg['n'], msg['v']]
    else:
        socket_response['error'] = True


def execute_script():
    next_dump_time = script_start_time + timedelta(**DUMP_FREQ)
    print(f'Next dump time is - {next_dump_time.strftime(TIME_FMT)}')
    klines = []
    latest_added = []

    # Start Websocket
    bsm = BinanceSocketManager(client)
    conn_key = bsm.start_symbol_ticker_socket(SYMBOL, socket_response_handler)
    bsm.start()

    while not socket_response['kline']:
        # wait for WebSocket to start streaming data
        sleep(0.1)

    while True:
        # error check to make sure WebSocket is working
        if socket_response['error']:
            # stop and restart socket
            bsm.stop_socket(conn_key)
            bsm.start()
            socket_response['error'] = False
        else:
            if not latest_added == socket_response['kline']:
                klines.append(socket_response['kline'])
                latest_added = socket_response['kline']

        current_time = datetime.now(timezone(TZ))
        if current_time.strftime(TIME_FMT) == next_dump_time.strftime(TIME_FMT):
            filename = DUMP_PATH + SYMBOL + '/' + next_dump_time.strftime("%Y%m%d-%H%M") + ".csv"
            dump_to_csv(filename, klines)
            klines = []
            next_dump_time = next_dump_time + timedelta(**DUMP_FREQ)
            print(f'Next dump time is - {next_dump_time.strftime(TIME_FMT)}')

        sleep(0.1)

    bsm.stop_socket(conn_key)
    reactor.stop()


if __name__ == '__main__':
    execute_script()