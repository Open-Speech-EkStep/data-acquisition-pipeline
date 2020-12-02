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
    with open(config_path, 'r') as f:
        config = json.load(f)
    return config
