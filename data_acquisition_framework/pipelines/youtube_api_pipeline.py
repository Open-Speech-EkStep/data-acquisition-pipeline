import glob
import logging
import os
import subprocess
from concurrent.futures.thread import ThreadPoolExecutor
import pandas as pd

from data_acquisition_framework.pipelines.data_acquisition_pipeline import DataAcqusitionPipeline
from data_acquisition_framework.configs.pipeline_config import source_name, channel_url_dict, mode
from data_acquisition_framework.token_utilities import get_token_from_bucket, update_token_in_bucket
from data_acquisition_framework.utilites import config_json, create_metadata, \
    retrieve_archive_from_bucket, upload_media_and_metadata_to_bucket, upload_archive_to_bucket
from data_acquisition_framework.youtube_api import YoutubeApiUtils, YoutubePlaylistCollector
from data_acquisition_framework.youtube_utilites import create_channel_playlist, get_video_batch, \
    check_and_log_download_output, get_speaker, get_gender, check_mode, get_playlist_count
from data_acquisition_framework.configs.paths import download_path, archives_path, playlist_path


class YoutubeApiPipeline(DataAcqusitionPipeline):
    FILE_FORMAT = 'mp4'
    VIDEO_BATCH_FILE_NAME = 'video_list.txt'

    def __init__(self):
        if not os.path.exists(download_path):
            os.system("mkdir " + download_path)
        if os.path.exists(playlist_path):
            os.system('rm -rf ' + playlist_path)
        if os.path.exists('video_list.txt'):
            os.system('rm video_list.txt')
        logging.info("*************YOUTUBE DOWNLOAD STARTS*************")
        # logging.info(str("Downloading videos for source : {0}".format(source_name)))

        self.youtube_api_utils = YoutubeApiUtils()
        self.batch_count = 0
        self.scraped_data = None
        self.config_json = config_json()['downloader']
        self.check_speaker = False
        self.youtube_call = "/app/python/bin/youtube-dl " if "scrapinghub" in os.path.abspath("~") else "youtube-dl "
        self.source_channel_dict = None
        self.source_file = None
        self.source_without_channel_id = None
        self.t_duration = 0

    def scrape_links(self):
        if len(channel_url_dict) != 0:
            self.source_channel_dict = channel_url_dict
        else:
            get_token_from_bucket()
            self.source_channel_dict = YoutubePlaylistCollector().get_urls()
        create_channel_playlist(self)
        return self

    def create_download_batch(self):
        return get_video_batch(self)

    def youtube_download(self, video_id, source_file_name):
        command = self.youtube_call + '-f "best[ext=mp4][filesize<1024M]" -o "{2}%(duration)sfile-id%(id)s.%(ext)s" ' \
                                      '"https://www.youtube.com/watch?v={0}" --download-archive {1} --proxy "" ' \
                                      '--abort-on-error'.format(video_id, archives_path.replace('<source>',
                                                                                                source_file_name.replace(".txt",
                                                                                                                "")),
                                                                download_path)
        downloader_output = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        check_and_log_download_output(self.source_file, downloader_output)

    def download_files(self, source_file_name):
        with ThreadPoolExecutor(max_workers=1) as executor:
            with open(self.VIDEO_BATCH_FILE_NAME, 'r') as f:
                for video_id in f.readlines():
                    executor.submit(self.youtube_download, video_id, source_file_name)
        return self

    def extract_metadata(self, source_file, file, channel_id, url=None):
        video_info = {}
        file_format = file.split('.')[-1]
        meta_file_name = file.replace(file_format, "csv")
        video_id = file.replace(download_path, "").split('file-id')[-1][:-4]
        source_url = "https://www.youtube.com/watch?v=" + video_id
        video_duration = int(file.replace(download_path, "").split('file-id')[0]) / 60
        video_info['duration'] = video_duration
        self.t_duration += video_duration
        logging.info('$$$$$$$    ' + str(self.t_duration // 60) + '   $$$$$$$')
        video_info['source'] = source_file.replace('.txt', '')
        video_info['raw_file_name'] = file.replace(download_path, "")
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
            video_info['source_website'] = "https://www.youtube.com/channel/"+channel_id
        video_info['license'] = self.youtube_api_utils.get_license_info(video_id)
        metadata = create_metadata(video_info, self.config_json)
        metadata_df = pd.DataFrame([metadata])
        metadata_df.to_csv(meta_file_name, index=False)
        return self

    def process_item(self, item, spider):
        self.check_speaker = True if check_mode(self) else False
        for source_file in glob.glob(playlist_path + '*.txt'):
            source_file_name = source_file.replace(playlist_path, '')
            channel_id = source_file_name.split("__")[0]
            source_without_channel_id = source_file_name.replace(channel_id + "__", "")
            logging.info(
                str("Channel {0}".format(source_file_name)))

            self.source_file = source_file_name
            self.source_without_channel_id = source_without_channel_id
            self.batch_count = 0
            retrieve_archive_from_bucket(source_without_channel_id.replace(".txt", ""))
            playlist_count = get_playlist_count(source_file)
            logging.info(
                str("Total playlist count with valid videos is {0}".format(playlist_count)))

            last_video_batch_count = self.create_download_batch()
            while last_video_batch_count > 0:
                logging.info(str("Attempt to download videos with batch size of {0}".format(
                    last_video_batch_count)))
                try:
                    self.download_files(source_without_channel_id)
                except Exception as e:
                    print("error", e)
                finally:
                    media_paths = glob.glob(download_path + '*.' + self.FILE_FORMAT)
                    media_files_count = len(media_paths)
                    if media_files_count > 0:
                        self.batch_count += media_files_count
                        logging.info(
                            str("Uploading {0} files to gcs bucket...".format(media_files_count)))
                        for file in media_paths:
                            self.extract_metadata(source_without_channel_id, file, channel_id)
                            upload_media_and_metadata_to_bucket(source_without_channel_id.replace(".txt", ""), file)
                        upload_archive_to_bucket(source_without_channel_id.replace(".txt", ""))
                        logging.info(
                            str("Uploaded files till now: {0}".format(self.batch_count)))
                last_video_batch_count = self.create_download_batch()
            logging.info(str("Last Batch has no more videos to be downloaded,so finishing downloads..."))
            logging.info(
                str("Total Uploaded files for this run was : {0}".format(self.batch_count)))
        update_token_in_bucket()
        return item
