import os
import csv
import datetime
from time import sleep
from datetime import timedelta

from binance.client import Client
from binance.websockets import BinanceSocketManager
from twisted.internet import reactor

# Configs
TEST_MODE = False

# Account init
env_variable_prefix = "demo" if TEST_MODE else "prod"
api_key = os.environ.get(env_variable_prefix + '_binance_api')
api_secret = os.environ.get(env_variable_prefix + '_binance_secret')
client = Client(api_key, api_secret)
if TEST_MODE:
    client.API_URL = 'https://testnet.binance.vision/api'


if not os.path.exists('hourly_dump'):
    os.makedirs('hourly_dump')

# Global Variables
socket_response = {'kline': None, 'error': False}
header_row = ['Date', 'Open', 'High', 'Low', 'PriceChange', 'PriceChange%', 'LastPrice', 'LastQuantity', 'BestBidPrice', 'BestBidQuantity', 'BestAskPrice', 'BestAskQuantity', 'NumberOfTrades']
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
        socket_response['kline'] = [msg['E'], msg['o'], msg['h'], msg['l'], msg['p'], msg['P'], msg['c'], msg['Q'], msg['b'], msg['B'], msg['a'], msg['A'], msg['n']]
    else:
        socket_response['error'] = True


def execute_script():
    next_dump_time = script_start_time + timedelta(hours=1)
    print(f'Next dump time is - {next_dump_time.strftime("%m/%d/%Y %H")}:00:00')
    hourly_klines = []
    latest_added = []

    # Start Websocket
    bsm = BinanceSocketManager(client)
    conn_key = bsm.start_symbol_ticker_socket('BTCUSDT', socket_response_handler)
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
                hourly_klines.append(socket_response['kline'])
                latest_added = socket_response['kline']

        current_time = datetime.datetime.utcnow()
        if current_time.strftime("%m/%d/%Y %H") == next_dump_time.strftime("%m/%d/%Y %H"):
            filename = "hourly_dump/" + next_dump_time.strftime("%Y%m%d-%H") + ".csv"
            dump_to_csv(filename, hourly_klines)
            hourly_klines = []
            next_dump_time = next_dump_time + timedelta(hours=1)

        sleep(0.1)

    bsm.stop_socket(conn_key)
    reactor.stop()


if __name__ == '__main__':
    execute_script()
