import pandas as pd
from glob import glob
import os

DUMP_PATH = 'data/dumps/'
FILENAMES = {
    'BTCUSDT': [
        {'start': '20210607-0033.csv', 'end': '20210607-1133.csv'},
        {'start': '20210607-2041.csv', 'end': '20210609-0341.csv'},
    ],
    'ETHUSDT': [
        {'start': '20210607-0034.csv', 'end': '20210607-1134.csv'},
        {'start': '20210607-2042.csv', 'end': '20210609-0342.csv'},
    ],
    'DOGEUSDT': [
        {'start': '20210607-0034.csv', 'end': '20210607-1134.csv'},
        {'start': '20210607-2042.csv', 'end': '20210609-0342.csv'},
    ],
}
MULT_FACTOR = 1e6

def accumulate_klines(df):
    data = list(df.to_dict('records'))
    last_base_vol = 0
    last_quote_vol = 0
    size = len(data)
    for ii in range(size):
        if ii > 0 and data[ii-1]['Kline Closed?']:
            last_base_vol += data[ii-1]['Base Volume']
            last_quote_vol += data[ii-1]['Quote Volume']
        data[ii]['Accm Base Volume'] = last_base_vol + data[ii]['Base Volume']
        data[ii]['Accm Quote Volume'] = last_quote_vol + data[ii]['Quote Volume']
        if ii > 0:
            data[ii]['Delta Time'] = data[ii]['EpochDate'] - data[ii-1]['EpochDate']
            data[ii]['Delta Price'] = data[ii]['Close'] - data[ii - 1]['Close']
            data[ii]['Delta Volume'] = data[ii]['Accm Base Volume'] - data[ii - 1]['Accm Base Volume']
            data[ii]['dV/dt'] = data[ii]['Delta Volume'] / data[ii]['Delta Time'] * MULT_FACTOR
            data[ii]['dP/dt'] = data[ii]['Delta Price'] / data[ii]['Delta Time'] * MULT_FACTOR
            if data[ii]['Delta Volume'] != 0:
                data[ii]['dP/dV'] = data[ii]['Delta Price'] / data[ii]['Delta Volume']
            else:
                data[ii]['dP/dV'] = 0
    data = data[1:]
    df = pd.DataFrame(data)
    return df


def combine_and_dump():
    for coin, paths in FILENAMES.items():
        all_files = sorted(glob(os.path.join(DUMP_PATH, coin) + '/*.csv'))
        for path in paths:
            all_csvs = []
            for file in all_files:
                if path['start'] <= os.path.basename(file) <= path['end']:
                    this_csv = pd.read_csv(file)
                    all_csvs.append(this_csv)
            combined_df = pd.concat(all_csvs)
            extended_df = accumulate_klines(combined_df)
            combined_file = 'comb_' + path['start'].split('.')[0] + '_' + path['end'].split('.')[0] + '.csv'
            combined_path = os.path.join(DUMP_PATH, coin, combined_file)
            extended_df.to_csv(combined_path, index=False)
            print(f'Dumped combined file is: {combined_path}')


if '__main__' == __name__:
    combine_and_dump()
