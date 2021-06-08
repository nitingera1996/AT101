import os
from dateutil import tz

SIGNALS_DIRECTORY = "signals/"
TIME_FMT = '%d-%m-%Y %H:00'
TZ = tz.gettz('Asia/Kolkata')


def dump_signal(signal_name, time_dt_utc, signal_id, sentiment="NA", coin="NA"):
    file_name = time_dt_utc.astimezone(TZ).strftime(TIME_FMT)
    signal_line = f'{time_dt_utc} {signal_id} {coin} {sentiment}'
    if not os.path.exists(SIGNALS_DIRECTORY + signal_name):
        os.makedirs(SIGNALS_DIRECTORY + signal_name)
    with open(SIGNALS_DIRECTORY + signal_name + '/' + file_name + ".txt", 'a+') as f:
        f.write(signal_line + '\n')
