import json
import os


def read_archive():
    archive_file_name = 'archive.txt'
    if os.path.exists(archive_file_name):
        with open(archive_file_name, 'r') as f:
            return f.read().splitlines()
    else:
        return []


def read_config(config_path):
    try:
        with open(config_path, 'r') as f:
            try:
                return json.load(f)
            except ValueError:
                raise ValueError('{} is not a valid JSON.'.format(config_path))
    except IOError:
        raise IOError('file {} does not exist.'.format(config_path))
