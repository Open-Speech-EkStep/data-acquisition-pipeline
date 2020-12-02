import json
import os


def load_config_file(file_name):
    current_path = os.path.dirname(os.path.realpath(__file__))
    config_file = os.path.join(current_path, '..', "configs", file_name)
    try:
        with open(config_file, 'r') as f:
            try:
                return json.load(f)
            except ValueError:
                raise ValueError('{} is not a valid JSON.'.format(file_name))
    except IOError:
        raise IOError('file {} does not exist.'.format(file_name))
