import os
from common.config import LOCAL_TZ, COMMON_TIME_FMT

SIGNALS_DIRECTORY = "signals/"
FILE_TIME_FMT = '%d-%m-%Y %H:00'


def dump_signal(signal_name, time_dt_utc, signal_id, sentiment=0, coin="NA"):
    file_name = time_dt_utc.astimezone(LOCAL_TZ).strftime(FILE_TIME_FMT)
    signal_line = f'{time_dt_utc.strftime(COMMON_TIME_FMT)} {signal_id} {coin} {sentiment}'
    if not os.path.exists(SIGNALS_DIRECTORY + signal_name):
        os.makedirs(SIGNALS_DIRECTORY + signal_name)
    with open(SIGNALS_DIRECTORY + signal_name + '/' + file_name + ".txt", 'a+') as f:
        print(f'Dumping signal {signal_line} to file {file_name}')
        f.write(signal_line + '\n')
