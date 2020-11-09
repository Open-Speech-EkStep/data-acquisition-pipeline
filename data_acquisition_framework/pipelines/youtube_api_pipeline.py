import glob
import logging
import os
from concurrent.futures.thread import ThreadPoolExecutor
from concurrent.futures import as_completed
import pandas as pd

from data_acquisition_framework.pipelines.data_acquisition_pipeline import DataAcqusitionPipeline
from data_acquisition_framework.configs.pipeline_config import channel_url_dict, mode
from data_acquisition_framework.token_utilities import get_token_from_bucket, update_token_in_bucket
from data_acquisition_framework.utilites import config_json, create_metadata, \
    retrieve_archive_from_bucket, upload_media_and_metadata_to_bucket, upload_archive_to_bucket
from data_acquisition_framework.youtube_api import YoutubeApiUtils, YoutubePlaylistCollector
from data_acquisition_framework.youtube_utilites import create_channel_playlist, get_video_batch, \
    get_speaker, get_gender, check_mode, get_playlist_count, remove_rejected_video
from data_acquisition_framework.configs.paths import download_path, archives_path, playlist_path
from data_acquisition_framework.services.youtube_dl import YoutubeDL


class YoutubeApiPipeline(DataAcqusitionPipeline):
    FILE_FORMAT = 'mp4'

    def __init__(self):
        logging.info("*************YOUTUBE DOWNLOAD STARTS*************")
        # logging.info(str("Downloading videos for source : {0}".format(source_name)))

        self.youtube_api_utils = YoutubeApiUtils()
        self.youtube_dl_service = YoutubeDL()
        self.batch_count = 0
        self.scraped_data = None
        self.config_json = config_json()['downloader']
        self.t_duration = 0

    def scrape_links(self):
        pass

    def create_download_batch(self, item):
        return get_video_batch(item['channel_name'], item['filename'])

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
        return self

    def extract_metadata(self, item, file, url=None):
        video_info = {}
        file_format = file.split('.')[-1]
        meta_file_name = file.replace(file_format, "csv")
        video_id = file.replace(download_path, "").split('file-id')[-1][:-4]
        source_url = "https://www.youtube.com/watch?v=" + video_id
        video_duration = int(file.replace(download_path, "").split('file-id')[0]) / 60
        video_info['duration'] = video_duration
        self.t_duration += video_duration
        logging.info('$$$$$$$    ' + str(self.t_duration // 60) + '   $$$$$$$')
        video_info['source'] = item['channel_name']
        video_info['raw_file_name'] = file.replace(download_path, "")
        video_info['name'] = get_speaker(item['filemode_data'], video_id) if mode == 'file' else None
        video_info['gender'] = get_gender(item['filemode_data'], video_id) if mode == 'file' else None
        video_info['source_url'] = source_url
        if mode == "channel":
            video_info['source_website'] = "https://www.youtube.com/channel/" + item['channel_id']
        video_info['license'] = self.youtube_api_utils.get_license_info(video_id)
        metadata = create_metadata(video_info, self.config_json)
        metadata_df = pd.DataFrame([metadata])
        metadata_df.to_csv(meta_file_name, index=False)
        return self

    def process_item(self, item, spider):
        self.batch_count = 0
        retrieve_archive_from_bucket(item["channel_name"])
        playlist_count = get_playlist_count(playlist_path + item['filename'])
        logging.info(
            str("Total playlist count with valid videos is {0}".format(playlist_count)))
        self.batch_download(item)
        update_token_in_bucket()
        return item

    def batch_download(self, item):
        batch_list = self.create_download_batch(item)
        last_video_batch_count = len(batch_list)
        while last_video_batch_count > 0:
            logging.info(str("Attempt to download videos with batch size of {0}".format(
                last_video_batch_count)))
            try:
                self.download_files(item, batch_list)
            except Exception as e:
                print("error", e)
            finally:
                self.upload_files_to_storage(item)
            batch_list = self.create_download_batch(item)
            last_video_batch_count = len(batch_list)
        logging.info(str("Last Batch has no more videos to be downloaded,so finishing downloads..."))
        logging.info(
            str("Total Uploaded files for this run was : {0}".format(self.batch_count)))

    def upload_files_to_storage(self, item):
        channel_name = item['channel_name']
        media_paths = glob.glob(download_path + '*.' + self.FILE_FORMAT)
        media_files_count = len(media_paths)
        if media_files_count > 0:
            self.batch_count += media_files_count
            logging.info(
                str("Uploading {0} files to gcs bucket...".format(media_files_count)))
            for file in media_paths:
                self.extract_metadata(item, file)
                upload_media_and_metadata_to_bucket(channel_name, file)
            upload_archive_to_bucket(channel_name)
            logging.info(
                str("Uploaded files till now: {0}".format(self.batch_count)))
