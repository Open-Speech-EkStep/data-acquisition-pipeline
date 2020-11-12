import glob
import json
import logging
import os
import subprocess
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

import pandas as pd
from pandas.io.common import EmptyDataError

from data_acquisition_framework.configs.paths import archives_path, channels_path, download_path
from data_acquisition_framework.configs.pipeline_config import batch_num, file_url_name_column, \
    file_speaker_name_column, file_speaker_gender_column, mode
from data_acquisition_framework.services.youtube.youtube_api import YoutubeApiUtils
from data_acquisition_framework.services.youtube.youtube_dl import YoutubeDL

logging.basicConfig(level=logging.DEBUG)


class YoutubeUtil:
    def __init__(self):
        self.youtube_dl_service = YoutubeDL()
        self.youtube_api_service = YoutubeApiUtils()

    def create_channel_file(self, source_channel_dict):
        if not (os.path.exists(channels_path)):
            os.mkdir(channels_path)

        for channel_url in source_channel_dict.keys():
            channel_id = channel_url.split('/')[-1]
            source_channel_dict[channel_url] = str(source_channel_dict[channel_url]).replace(' ', '_')
            source_channel_file = channels_path + channel_id + '__' + source_channel_dict[channel_url] + '.txt'

            videos_list = self.youtube_api_service.get_videos(channel_id)
            with open(source_channel_file, 'w') as channel_file:
                for video_id in videos_list:
                    channel_file.write(video_id + "\n")

    def download_files(self, item, batch_list):
        archive_path = archives_path.replace('<source>', item['channel_name'])
        with ThreadPoolExecutor(max_workers=1) as executor:
            futures = []
            for video_id in batch_list:
                futures.append(executor.submit(self.youtube_dl_service.youtube_download, video_id,
                                               archive_path,
                                               download_path))
            for future in as_completed(futures):
                remove_video_flag, video_id = future.result()
                if remove_video_flag:
                    remove_rejected_video(item['filename'], video_id)

    def get_license_info(self, video_id):
        return self.youtube_api_service.get_license_info(video_id)

    def get_channels(self):
        return self.youtube_api_service.get_channels()

    def get_video_info(self, file, channel_name, filemode_data, channel_id):
        video_id = file.replace(download_path, "").split('file-id')[-1][:-4]
        video_url_prefix = 'https://www.youtube.com/watch?v='
        channel_url_prefix = 'https://www.youtube.com/channel/'
        source_url = video_url_prefix + video_id
        video_duration = int(file.replace(download_path, "").split('file-id')[0]) / 60
        video_info = {'duration': video_duration, 'source': channel_name,
                      'raw_file_name': file.replace(download_path, ""),
                      'name': get_speaker(filemode_data, video_id) if mode == 'file' else None,
                      'gender': get_gender(filemode_data, video_id) if mode == 'file' else None,
                      'source_url': source_url, 'license': self.get_license_info(video_id)}
        # self.t_duration += video_duration
        # logging.info('$$$$$$$    ' + str(self.t_duration // 60) + '   $$$$$$$')
        if mode == "channel":
            video_info['source_website'] = channel_url_prefix + channel_id
        return video_info


def get_video_batch(source, source_file):
    source = source.replace('.txt', '')
    channel_file_name = channels_path + source_file
    archive_file_name = archives_path.replace('<source>', source)
    try:
        channel_videos = pd.read_csv(channel_file_name, header=None)
    except EmptyDataError:
        return []
    try:
        channel_archive = pd.read_csv(archive_file_name, delimiter=' ', header=None, encoding='utf-8')[1]
    except EmptyDataError:
        channel_archive = pd.DataFrame(columns=[1])
    video_batch = channel_videos[
        channel_videos.merge(channel_archive, left_on=0, right_on=1, how='left')[1].isnull()].head(
        batch_num)
    return video_batch[0].tolist()


def check_dataframe_validity(df):
    if file_url_name_column not in df.columns:
        logging.error("Url column entered wrong.")
        exit()
    if file_speaker_name_column not in df.columns:
        logging.error("Speaker name column entered wrong.")
        exit()
    if file_speaker_gender_column not in df.columns:
        logging.error("Speaker gender column entered wrong.")
        exit()


def create_channel_file_for_file_mode(source_file, file_url_column):
    df = pd.read_csv(source_file)
    check_dataframe_validity(df)
    df = df[df[file_url_column].notna()]
    df[file_url_column] = df[file_url_column].apply(
        lambda x: str(x).replace("https://www.youtube.com/watch?v=", ""))
    df[file_url_column] = df[file_url_column].apply(lambda x: str(x).replace("https://youtu.be/", ""))
    if not os.path.exists(channels_path):
        os.system("mkdir " + channels_path)
    df[file_url_column].to_csv(channels_path + source_file.replace(".csv", ".txt"), index=False, header=None)
    return df


def get_channel_videos_count(file_name):
    file_path = channels_path + file_name
    return int(subprocess.check_output(
        "cat {0} | wc -l".format(file_path), shell=True).decode("utf-8").split('\n')[0])


def get_media_paths():
    return glob.glob(download_path + '*.mp4')


def get_speaker(scraped_data, video_id):
    return scraped_data[scraped_data[file_url_name_column] == video_id].iloc[0][file_speaker_name_column]


def get_gender(scraped_data, video_id):
    return str(scraped_data[scraped_data[file_url_name_column] == video_id].iloc[0][file_speaker_gender_column]).lower()


def remove_rejected_video(source, video_id):
    os.system("sed '/{0}/d' {1}>b.txt && mv b.txt {1}".format(video_id, channels_path + source))


def get_meta_filename(file):
    file_format = file.split('.')[-1]
    meta_file_name = file.replace(file_format, "csv")
    return meta_file_name


def load_config_json():
    current_path = os.path.dirname(os.path.realpath(__file__))
    config_file = os.path.join(current_path, '..', "configs", "config.json")
    with open(config_file, 'r') as file:
        config_json = json.load(file)
    return config_json
