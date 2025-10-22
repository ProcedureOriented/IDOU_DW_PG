import os
if os.path.abspath('.').endswith('utils'):
    os.chdir('..')

import json
config_path = 'utils/db_conn.json'

def get_conn(config_name: str) -> dict:
    with open(config_path) as config_file:
        config: dict = json.load(config_file)
    return config.get(config_name)
