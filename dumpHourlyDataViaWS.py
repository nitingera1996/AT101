import os
import argparse
import csv
import datetime
from time import sleep
from datetime import timedelta

from binance.client import Client
from binance.websockets import BinanceSocketManager
from twisted.internet import reactor

# Configs
TEST_MODE = False
TIME_FMT = '%m/%d/%Y %H:%M'
DUMP_PATH = 'data/dumps/'
DEFAULT_COIN_SYMBOL = "BTCUSDT"

# Account init
env_variable_prefix = "demo" if TEST_MODE else "prod"
api_key = os.environ.get(env_variable_prefix + '_binance_api')
api_secret = os.environ.get(env_variable_prefix + '_binance_secret')
client = Client(api_key, api_secret)
if TEST_MODE:
    client.API_URL = 'https://testnet.binance.vision/api'


# Global Variables
socket_response = {'kline': None, 'error': False}
header_row = ['EpochDate', 'LocalTime', 'Open', 'High', 'Low', 'PriceChange', 'PriceChange%', 'LastPrice', 'LastQuantity', 'BestBidPrice', 'BestBidQuantity', 'BestAskPrice', 'BestAskQuantity', 'NumberOfTrades']
script_start_time = datetime.datetime.utcnow()
print("Script start time is -", script_start_time)


def dump_to_csv(filename, hourly_klines):
    print(f"Dumping {len(hourly_klines)} records to filename {filename}")
    with open(filename, 'w+') as f:
        write = csv.writer(f)
        write.writerow(header_row)
        write.writerows(hourly_klines)


def socket_response_handler(msg):
    """ Define how to process incoming WebSocket messages """
    if msg['e'] != 'error':
        socket_response['kline'] = [msg['E'], datetime.fromtimestamp(int(msg['E'])/1000.0).strftime('%c'), msg['o'], msg['h'], msg['l'], msg['p'], msg['P'], msg['c'], msg['Q'], msg['b'], msg['B'], msg['a'], msg['A'], msg['n']]
    else:
        socket_response['error'] = True


def dump_coin_data(coin_symbol):
    if not os.path.exists(DUMP_PATH+coin_symbol):
        os.makedirs(DUMP_PATH+coin_symbol)
    next_dump_time = script_start_time + timedelta(hours=1)
    print(f'Next dump time is - {next_dump_time.strftime(TIME_FMT)}')
    hourly_klines = []
    latest_added = []

    # Start Websocket
    bsm = BinanceSocketManager(client)
    conn_key = bsm.start_symbol_ticker_socket(coin_symbol, socket_response_handler)
    bsm.start()
    print(f"Started Websocket for getting ticker for symbol {coin_symbol}")

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
                hourly_klines.append(socket_response['kline'])
                latest_added = socket_response['kline']

        current_time = datetime.datetime.utcnow()
        if current_time.strftime(TIME_FMT) == next_dump_time.strftime(TIME_FMT):
            filename = DUMP_PATH + coin_symbol + "/" + next_dump_time.strftime("%Y%m%d-%H") + ".csv"
            dump_to_csv(filename, hourly_klines)
            hourly_klines = []
            next_dump_time = next_dump_time + timedelta(hours=1)

        sleep(0.1)

    bsm.stop_socket(conn_key)
    reactor.stop()


def parse_args():
    x = argparse.ArgumentParser()
    x.add_argument('--coin', '-c', help="Coin to Parse")
    return x.parse_args()


if __name__ == '__main__':
    # Load arguments then parse settings
    args = parse_args()

    coin_symbol = args.coin if args.coin else DEFAULT_COIN_SYMBOL
    dump_coin_data(coin_symbol)
