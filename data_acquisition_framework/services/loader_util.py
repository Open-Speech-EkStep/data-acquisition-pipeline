import json
import os


def load_storage_config():
    current_path = os.path.dirname(os.path.realpath(__file__))
    storage_config_file = os.path.join(current_path, '..', "configs", "storage_config.json")
    with open(storage_config_file, 'r') as f:
        storage_config = json.load(f)
    return storage_config


def load_youtube_api_config():
    current_path = os.path.dirname(os.path.realpath(__file__))
    api_config_file = os.path.join(current_path, '..', "configs", "youtube_api_config.json")
    with open(api_config_file, 'r') as f:
        yt_api_config = json.load(f)
    return yt_api_config
