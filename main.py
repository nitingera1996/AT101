import threading
import importlib.util
import time
from utils.config_utils import load_config


# Main script configs
DEFAULT_CONFIG_FILE = 'config/main.yml'


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    parsed_config = load_config(DEFAULT_CONFIG_FILE)
    SIGNALLING_MODULES = parsed_config['trading_options']['SIGNALLING_MODULES']

    my_modules = {}
    # load signalling modules
    try:
        if len(SIGNALLING_MODULES) > 0:
            for module in SIGNALLING_MODULES:
                print(f'Starting {module}')
                my_modules[module] = importlib.import_module(module)
                t = threading.Thread(target=my_modules[module].do_work, args=())
                t.daemon = True
                t.start()
                time.sleep(2)
        else:
            print(f'No modules to load {SIGNALLING_MODULES}')
    except Exception as e:
        print(e)

    while True:
        # Do main work for script here
        pass
