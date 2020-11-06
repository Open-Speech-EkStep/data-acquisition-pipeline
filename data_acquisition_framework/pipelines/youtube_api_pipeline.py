import glob
import logging
import os
import subprocess
from concurrent.futures.thread import ThreadPoolExecutor
import pandas as pd

from data_acquisition_framework.pipelines.data_acquisition_pipeline import DataAcqusitionPipeline
from data_acquisition_framework.configs.pipeline_config import source_name, channel_url_dict, mode
from data_acquisition_framework.token_utilities import get_token_from_bucket, update_token_in_bucket
from data_acquisition_framework.utilites import config_json, create_metadata_for_api, \
    retrieve_archive_from_bucket_for_api, upload_media_and_metadata_to_bucket_for_api, upload_archive_to_bucket_for_api
from data_acquisition_framework.youtube_api import YoutubeApiUtils, YoutubePlaylistCollector
from data_acquisition_framework.youtube_utilites import create_channel_playlist_for_api, get_video_batch, \
    check_and_log_download_output, get_speaker, get_gender, read_website_url, check_mode, get_playlist_count


class YoutubeApiPipeline(DataAcqusitionPipeline):
    FILE_FORMAT = 'mp4'
    ARCHIVE_FILE_NAME = 'archive.txt'
    VIDEO_BATCH_FILE_NAME = 'video_list.txt'
    FULL_PLAYLIST_FILE_NAME = "full_playlist.txt"
    PLAYLIST_PATH = 'playlist'

    def __init__(self):
        logging.info("*************YOUTUBE DOWNLOAD STARTS*************")
        logging.info(str("Downloading videos for source : {0}".format(source_name)))

        self.youtube_api_utils = YoutubeApiUtils()
        self.batch_count = 0
        self.scraped_data = None
        self.config_json = config_json()['downloader']
        self.check_speaker = False
        self.youtube_call = "/app/python/bin/youtube-dl " if "scrapinghub" in os.path.abspath("~") else "youtube-dl "
        self.source_channel_dict = None
        self.source_file = None
        self.t_duration = 0

    def scrape_links(self):
        if len(channel_url_dict) != 0:
            self.source_channel_dict = channel_url_dict
        else:
            get_token_from_bucket()
            self.source_channel_dict = YoutubePlaylistCollector().get_urls()
        create_channel_playlist_for_api(self)
        return self

    def create_download_batch(self):
        return get_video_batch(self)

    def youtube_download(self, video_id, file):
        command = self.youtube_call + '-f "best[ext=mp4][filesize<1024M]" -o "%(duration)sfile-id%(id)s.%(ext)s" ' \
                                      '"https://www.youtube.com/watch?v={0}" --download-archive {1} --proxy "" ' \
                                      '--abort-on-error'.format(video_id, 'archive_' + file)
        downloader_output = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        check_and_log_download_output(self, downloader_output)

    def download_files(self, file):
        with ThreadPoolExecutor(max_workers=1) as executor:
            with open(self.VIDEO_BATCH_FILE_NAME, 'r') as f:
                for video_id in f.readlines():
                    executor.submit(self.youtube_download, video_id, file)
        return self

    def extract_metadata(self, source_file, file, url=None):
        video_info = {}
        file_format = file.split('.')[-1]
        meta_file_name = file.replace(file_format, "csv")
        video_id = file.split('file-id')[-1][:-4]
        source_url = "https://www.youtube.com/watch?v=" + video_id
        video_duration = int(file.split('file-id')[0]) / 60
        video_info['duration'] = video_duration
        self.t_duration += video_duration
        logging.info('$$$$$$$    ' + str(self.t_duration // 60) + '   $$$$$$$')
        video_info['source'] = source_file.replace('.txt', '')
        video_info['raw_file_name'] = file
        if self.check_speaker:
            video_info['name'] = get_speaker(self.scraped_data, video_id)
        else:
            video_info['name'] = None
        if self.check_speaker:
            video_info['gender'] = get_gender(self.scraped_data, video_id)
        else:
            video_info['gender'] = None
        video_info['source_url'] = source_url
        if mode == "channel":
            video_info['source_website'] = read_website_url(video_info['source'])
        video_info['license'] = self.youtube_api_utils.get_license_info(video_id)
        metadata = create_metadata_for_api(video_info, self.config_json)
        metadata_df = pd.DataFrame([metadata])
        metadata_df.to_csv(meta_file_name, index=False)
        return self

    def process_item(self, item, spider):
        self.check_speaker = True if check_mode(self) else False
        for source_file in glob.glob(self.PLAYLIST_PATH + '/*.txt'):
            source_file_name = source_file.replace('playlist/', '')
            logging.info(
                str("Channel {0}".format(source_file_name)))

            self.source_file = source_file_name
            self.batch_count = 0
            retrieve_archive_from_bucket_for_api(source_file_name)
            playlist_count = get_playlist_count(source_file)
            logging.info(
                str("Total playlist count with valid videos is {0}".format(playlist_count)))

            last_video_batch_count = self.create_download_batch()
            while last_video_batch_count > 0:
                logging.info(str("Attempt to download videos with batch size of {0}".format(
                    last_video_batch_count)))
                try:
                    self.download_files(source_file_name)
                except Exception as e:
                    print("error", e)
                finally:
                    audio_paths = glob.glob('*.' + self.FILE_FORMAT)
                    audio_files_count = len(audio_paths)
                    if audio_files_count > 0:
                        self.batch_count += audio_files_count
                        logging.info(
                            str("Uploading {0} files to gcs bucket...".format(audio_files_count)))
                        for file in audio_paths:
                            self.extract_metadata(source_file_name, file)
                            upload_media_and_metadata_to_bucket_for_api(source_file_name, file)
                        upload_archive_to_bucket_for_api(source_file_name)
                        logging.info(
                            str("Uploaded files till now: {0}".format(self.batch_count)))
                last_video_batch_count = self.create_download_batch()
            logging.info(str("Last Batch has no more videos to be downloaded,so finishing downloads..."))
            logging.info(
                str("Total Uploaded files for this run was : {0}".format(self.batch_count)))
        update_token_in_bucket()
        return item
