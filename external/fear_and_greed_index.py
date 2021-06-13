import requests
import time
from datetime import timedelta
from utils.math_utils import convert_percentage_to_polarity_range
from utils.datetime_utils import convert_epoch_to_datetime
from utils.dump_utils import dump_signal

FNG_API_ENDPOINT = "https://api.alternative.me/fng/"
FEAR_AND_GREED_INDEX = "FEAR_AND_GREED_INDEX"


def do_work():
    time_until_update = 0

    response = requests.get(FNG_API_ENDPOINT)
    if response.status_code == 200:
        json_response = response.json()
        if json_response.get('metadata') and not json_response.get('metadata').get('error'):
            data = json_response.get('data')[0]
            index_value = data.get('value')
            index_timestamp = data.get('timestamp')
            time_until_update = int(data.get('time_until_update'))
            polarity_value = convert_percentage_to_polarity_range(float(index_value))
            index_dt = convert_epoch_to_datetime(int(index_timestamp))
            dump_signal(signal_name=FEAR_AND_GREED_INDEX, time_dt_utc=index_dt, signal_id='NA', sentiment=polarity_value)

    time_to_sleep = timedelta(seconds=time_until_update)
    print(f'Sleeping for {time_to_sleep} until next {FEAR_AND_GREED_INDEX}  update')
    time.sleep(time_until_update)  # sleep for exactly the amount of time required


if __name__ == "__main__":
    do_work()
