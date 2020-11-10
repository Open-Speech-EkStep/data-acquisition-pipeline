import logging
import subprocess
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

import pandas as pd
from pandas.io.common import EmptyDataError

from data_acquisition_framework.gcs_operations import *
from data_acquisition_framework.configs.pipeline_config import *
from data_acquisition_framework.configs.paths import archives_path, channels_path, download_path
from data_acquisition_framework.services.youtube.youtube_dl import YoutubeDL
from data_acquisition_framework.services.youtube.youtube_api import YoutubeApiUtils

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
    video_batch = channel_videos[channel_videos.merge(channel_archive, left_on=0, right_on=1, how='left')[1].isnull()].head(
        batch_num)
    return video_batch[0].tolist()


def check_mode():
    if mode == "file":
        if check_blob(bucket, get_videos_file_path_in_bucket()):
            download_blob(bucket, get_videos_file_path_in_bucket(), source_name + ".csv")
            logging.info(str("Source scraped file has been downloaded from bucket {0} to local path...".format(bucket)))
            scraped_data = create_channel_file_for_file_mode(source_name + ".csv", file_url_name_column)
            return scraped_data
        else:
            logging.error(str("{0} File doesn't exists on the given location: {1}".format(source_name + ".csv",
                                                                                          get_videos_file_path_in_bucket())))
            exit()
    if mode == "channel":
        return None
    else:
        logging.error("Invalid mode")
        exit()


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


def get_channel_videos_count(file):
    return int(subprocess.check_output(
        "cat {0} | wc -l".format(file), shell=True).decode("utf-8").split('\n')[0])


def get_videos_file_path_in_bucket():
    return channel_blob_path + '/' + scraped_data_blob_path + '/' + source_name + '.csv'


def get_speaker(scraped_data, video_id):
    return scraped_data[scraped_data[file_url_name_column] == video_id].iloc[0][file_speaker_name_column]


def get_gender(scraped_data, video_id):
    return str(scraped_data[scraped_data[file_url_name_column] == video_id].iloc[0][file_speaker_gender_column]).lower()


def remove_rejected_video(source, video_id):
    os.system("sed '/{0}/d' {1}>b.txt && mv b.txt {1}".format(video_id, channels_path + source))
