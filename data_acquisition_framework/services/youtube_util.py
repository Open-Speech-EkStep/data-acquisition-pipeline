import glob
import logging
import os
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

import pandas as pd
from pandas.errors import EmptyDataError
from data_acquisition_framework.configs.paths import archives_path, channels_path, download_path
from data_acquisition_framework.configs.youtube_pipeline_config import batch_num, file_url_name_column, \
    file_speaker_name_column, file_speaker_gender_column, mode, source_name, channel_url_dict, license_column, \
    youtube_service_to_use, YoutubeService, only_creative_commons
from data_acquisition_framework.services.storage_util import StorageUtil
from data_acquisition_framework.services.youtube.youtube_api import YoutubeApiUtils
from data_acquisition_framework.services.youtube.youtube_dl_api import YoutubeDL


def remove_rejected_video(file_name, video_id):
    os.system("sed '/{0}/d' {1}>b.txt && mv b.txt {1}".format(video_id, channels_path + file_name))
    logging.info(str("Video Id {0} removed from channel and won't be downloaded".format(video_id)))


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
        raise KeyError(file_url_name_column + ' is not present in given csv file')
    if file_speaker_name_column not in df.columns:
        logging.error("Speaker name column entered wrong.")
        raise KeyError(file_speaker_name_column + ' is not present in given csv file')
    if file_speaker_gender_column not in df.columns:
        logging.error("Speaker gender column entered wrong.")
        raise KeyError(file_speaker_gender_column + ' is not present in given csv file')


def create_channel_file_for_file_mode(source_file, file_url_column):
    df = pd.read_csv(source_file)
    check_dataframe_validity(df)
    df = df[df[file_url_column].notna()]
    df[file_url_column] = df[file_url_column].apply(
        lambda x: str(x).replace("https://www.youtube.com/watch?v=", ""))
    df[file_url_column] = df[file_url_column].apply(lambda x: str(x).replace("https://youtu.be/", ""))
    if not os.path.exists(channels_path):
        os.system("mkdir " + channels_path)
    if 'channel_name' in df.columns:
        channel_value_map = {}
        for ind in df.index:
            row = df.iloc[ind]
            if row['channel_name'] != "":
                if row['channel_name'] in channel_value_map:
                    channel_value_map[row['channel_name']].append(row[file_url_column])
                else:
                    channel_value_map[row['channel_name']] = [row[file_url_column]]
        for key, value in channel_value_map.items():
            source_channel_file = channels_path + key + '.txt'
            with open(source_channel_file, 'w') as channel_file:
                for video_id in value:
                    channel_file.write(video_id + "\n")
    else:
        df[file_url_column].to_csv(channels_path + source_file.replace(".csv", ".txt"), index=False, header=None)
    return df


def get_speaker(scraped_data, video_id):
    matched_video_id_row = scraped_data[scraped_data[file_url_name_column] == video_id]
    if len(matched_video_id_row) == 0:
        return ""
    return str(matched_video_id_row.iloc[0][file_speaker_name_column]).lower()


def get_license(scraped_data, video_id):
    matched_video_id_row = scraped_data[scraped_data[file_url_name_column] == video_id]
    if len(matched_video_id_row) == 0:
        return ""
    if license_column not in matched_video_id_row:
        return ""
    return str(matched_video_id_row.iloc[0][license_column])


def get_gender(scraped_data, video_id):
    matched_video_id_row = scraped_data[scraped_data[file_url_name_column] == video_id]
    if len(matched_video_id_row) == 0:
        return ""
    return str(matched_video_id_row.iloc[0][file_speaker_gender_column]).lower()


def is_channel_from_config():
    return len(channel_url_dict) != 0


def is_youtube_api_mode():
    return (not is_channel_from_config()) and mode == 'channel'


class YoutubeUtil:
    def __init__(self):
        self.t_duration = 0
        self.storage_util = StorageUtil()
        self.youtube_dl_service = YoutubeDL()
        self.youtube_api_service = YoutubeApiUtils()

    def create_channel_file(self, source_channel_dict):
        if not (os.path.exists(channels_path)):
            os.mkdir(channels_path)

        for channel_url in source_channel_dict.keys():
            source_channel_dict[channel_url] = str(source_channel_dict[channel_url]).replace(' ', '_')
            # if "playlist?list=" in channel_url:
            #     pass
            # else:
            channel_id = channel_url.split('/')[-1]
            id_name_join = channel_id + '__' + source_channel_dict[channel_url]
            source_channel_file = channels_path + id_name_join + '.txt'

            is_downloaded = self.storage_util.get_videos_of_channel(id_name_join)

            if not is_downloaded:
                if youtube_service_to_use == YoutubeService.YOUTUBE_DL:
                    if only_creative_commons:
                        tmps_videos_list = self.youtube_dl_service.get_videos(channel_url)
                        videos_list = []
                        for video in tmps_videos_list:
                            if 'Creative Commons' == self.youtube_api_service.get_license_info(video):
                                videos_list.append(video)
                    else:
                        videos_list = self.youtube_dl_service.get_videos(channel_url)
                else:
                    videos_list = self.youtube_api_service.get_videos(channel_id)
                tmp_videos_list = []
                with open(source_channel_file, 'w') as channel_file:
                    for video_id in videos_list:
                        if video_id not in tmp_videos_list:
                            channel_file.write(video_id + "\n")
                            tmp_videos_list.append(video_id)
                self.storage_util.upload(source_channel_file,
                                         self.storage_util.get_channel_file_upload_path(id_name_join))

    def download_files(self, channel_name, file_name, batch_list):
        archive_path = archives_path.replace('<source>', channel_name)
        with ThreadPoolExecutor(max_workers=1) as executor:
            futures = []
            for video_id in batch_list:
                futures.append(executor.submit(self.youtube_dl_service.youtube_download, video_id,
                                               archive_path,
                                               download_path))
            for future in as_completed(futures):
                remove_video_flag, video_id = future.result()
                if remove_video_flag:
                    remove_rejected_video(file_name, video_id)

    def get_license_info(self, video_id):
        # return "Creative Commons"
        return self.youtube_api_service.get_license_info(video_id)

    def get_channels(self):
        if only_creative_commons:
            return self.youtube_api_service.get_cc_video_channels()
        return self.youtube_api_service.get_channels()

    def get_video_info(self, file, channel_name, filemode_data, channel_id):
        video_id = file.replace(download_path, "").split('file-id')[-1][:-4]
        video_url_prefix = 'https://www.youtube.com/watch?v='
        channel_url_prefix = 'https://www.youtube.com/channel/'
        source_url = video_url_prefix + video_id
        video_duration = int(file.replace(download_path, "").split('file-id')[0]) / 60
        if mode == 'file':
            licence = get_license(filemode_data, video_id)
            if licence == "":
                licence = self.get_license_info(video_id)
        else:
            if only_creative_commons:
                licence = "Creative Commons"
            else:
                licence = self.get_license_info(video_id)

        video_info = {'duration': video_duration, 'source': channel_name,
                      'raw_file_name': file.replace(download_path, ""),
                      'name': get_speaker(filemode_data, video_id) if mode == 'file' else None,
                      'gender': get_gender(filemode_data, video_id) if mode == 'file' else None,
                      'source_url': source_url, 'license': licence}
        self.t_duration += video_duration
        logging.info('$$$$$$$    ' + str(self.t_duration // 60) + '   $$$$$$$')
        if mode == "channel":
            video_info['source_website'] = channel_url_prefix + channel_id
        return video_info

    def validate_mode_and_get_result(self):
        scraped_data = None
        if mode == "file":
            videos_file_path = self.storage_util.get_videos_file_path_in_bucket(source_name)
            if self.storage_util.check(videos_file_path):
                self.storage_util.download(videos_file_path, source_name + ".csv")
                logging.info(
                    str("Source scraped file has been downloaded from bucket to local path..."))
                scraped_data = create_channel_file_for_file_mode(source_name + ".csv", file_url_name_column)
            else:
                logging.error(str("{0} File doesn't exists on the given location: {1}".format(source_name + ".csv",
                                                                                              videos_file_path)))
        elif mode == "channel":
            self.get_channels_from_source()
        else:
            logging.error("Invalid mode")

        for channel_file in glob.glob(channels_path + '*.txt'):
            yield mode, channel_file.replace(channels_path, ''), scraped_data

    def get_channels_from_source(self):
        if is_channel_from_config():
            source_channel_dict = channel_url_dict
        else:
            source_channel_dict = self.get_channels()
        self.create_channel_file(source_channel_dict)
