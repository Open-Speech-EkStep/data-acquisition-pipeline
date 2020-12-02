import json
import os


def load_config_file(file_name):
    current_path = os.path.dirname(os.path.realpath(__file__))
    config_file = os.path.join(current_path, file_name)
    with open(config_file, 'r') as f:
        config = json.load(f)
    return config
