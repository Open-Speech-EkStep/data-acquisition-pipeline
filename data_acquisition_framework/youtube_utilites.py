import logging
import subprocess

import pandas as pd
from pandas.io.common import EmptyDataError

from .gcs_operations import *
from data_acquisition_framework.configs.pipeline_config import *
from data_acquisition_framework.configs.paths import archives_path, playlist_path

logging.basicConfig(level=logging.DEBUG)


def get_video_batch(source, source_file):
    source = source.replace('.txt', '')
    playlist_file_name = playlist_path + source_file
    archive_file_name = archives_path.replace('<source>', source)
    try:
        full_playlist = pd.read_csv(playlist_file_name, header=None)
    except EmptyDataError:
        return 0
    try:
        archive_file = pd.read_csv(archive_file_name, delimiter=' ', header=None, encoding='utf-8')[1]
    except EmptyDataError:
        archive_file = pd.DataFrame(columns=[1])
    video_batch = full_playlist[full_playlist.merge(archive_file, left_on=0, right_on=1, how='left')[1].isnull()].head(
        batch_num)
    return video_batch[0].tolist()


def check_mode():
    if mode == "file":
        if check_blob(bucket, get_videos_file_path_in_bucket()):
            download_blob(bucket, get_videos_file_path_in_bucket(), source_name + ".csv")
            logging.info(str("Source scraped file has been downloaded from bucket {0} to local path...".format(bucket)))
            scraped_data = create_playlist_for_file_mode(source_name + ".csv", file_url_name_column)
            return scraped_data, True
        else:
            logging.error(str("{0} File doesn't exists on the given location: {1}".format(source_name + ".csv",
                                                                                          get_videos_file_path_in_bucket())))
            exit()
    if mode == "channel":
        return None, False
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


def create_playlist_for_file_mode(source_file, file_url_column):
    df = pd.read_csv(source_file)
    check_dataframe_validity(df)
    df = df[df[file_url_column].notna()]
    df[file_url_column] = df[file_url_column].apply(
        lambda x: str(x).replace("https://www.youtube.com/watch?v=", ""))
    df[file_url_column] = df[file_url_column].apply(lambda x: str(x).replace("https://youtu.be/", ""))
    if not os.path.exists(playlist_path):
        os.system("mkdir " + playlist_path)
    df[file_url_column].to_csv(playlist_path + source_file.replace(".csv", ".txt"), index=False, header=None)
    return df


def create_channel_playlist(source_channel_dict, youtube_dl_service):
    if not (os.path.exists(playlist_path)):
        os.mkdir(playlist_path)

    for channel_url in source_channel_dict.keys():
        channel_id = channel_url.split('/')[-1]
        source_channel_dict[channel_url] = str(source_channel_dict[channel_url]).replace(' ', '_')
        source_playlist_file = playlist_path + channel_id + '__' + source_channel_dict[channel_url] + '.txt'

        videos_list = youtube_dl_service.get_videos(channel_url, match_title_string, reject_title_string)
        with open(source_playlist_file, 'w') as playlist_file:
            for video_id in videos_list:
                playlist_file.write(video_id+"\n")


def get_playlist_count(file):
    return int(subprocess.check_output(
        "cat {0} | wc -l".format(file), shell=True).decode("utf-8").split('\n')[0])


def get_videos_file_path_in_bucket():
    return channel_blob_path + '/' + scraped_data_blob_path + '/' + source_name + '.csv'


def get_speaker(scraped_data, video_id):
    return scraped_data[scraped_data[file_url_name_column] == video_id].iloc[0][file_speaker_name_column]


def get_gender(scraped_data, video_id):
    return str(scraped_data[scraped_data[file_url_name_column] == video_id].iloc[0][file_speaker_gender_column]).lower()


def remove_rejected_video(source, video_id):
    os.system("sed '/{0}/d' {1}>b.txt && mv b.txt {1}".format(video_id, playlist_path + source))
