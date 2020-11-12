import json
import os

import moviepy.editor
from tinytag import TinyTag

from data_acquisition_framework.configs.paths import download_path


def get_mp3_duration_in_seconds(file):
    tag = TinyTag.get(file)
    return round(tag.duration, 3)


def get_license_info(license_urls):
    for url in license_urls:
        if "creativecommons" in url:
            return "Creative Commons"
    return ', '.join(license_urls)


def get_file_format(file):
    file_format = file.split('.')[-1]
    return file_format


def get_media_info(file, source, language, source_url, license_urls, media_url):
    file_format = get_file_format(file)
    if file_format == 'mp4':
        video = moviepy.editor.VideoFileClip(file)
        duration_in_seconds = int(video.duration)
    else:
        duration_in_seconds = get_mp3_duration_in_seconds(file)
    media_info = {'duration': duration_in_seconds / 60,
                  'raw_file_name': file.replace(download_path, ""),
                  'name': None, 'gender': None,
                  'source_url': media_url,
                  'license': get_license_info(license_urls),
                  "source": source,
                  "language": language,
                  'source_website': source_url}
    return media_info, duration_in_seconds


def load_config_json():
    current_path = os.path.dirname(os.path.realpath(__file__))
    config_file = os.path.join(current_path, '..', "configs", "config.json")
    with open(config_file, 'r') as file:
        config_json = json.load(file)
    return config_json
