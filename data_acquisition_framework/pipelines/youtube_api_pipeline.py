import glob
import logging

import pandas as pd

from data_acquisition_framework.configs.paths import download_path, channels_path
from data_acquisition_framework.metadata.metadata import MediaMetadata
from data_acquisition_framework.pipelines.data_acquisition_pipeline import DataAcquisitionPipeline
from data_acquisition_framework.services.storage_util import StorageUtil
from data_acquisition_framework.services.youtube_util import YoutubeUtil, get_video_batch, get_channel_videos_count, \
    get_meta_filename


class YoutubeApiPipeline(DataAcquisitionPipeline):
    FILE_FORMAT = 'mp4'

    def __init__(self):
        self.storage_util = StorageUtil()
        logging.info("*************YOUTUBE DOWNLOAD STARTS*************")
        self.youtube_util = YoutubeUtil()
        self.metadata_creator = MediaMetadata()
        self.batch_count = 0
        self.scraped_data = None
        self.t_duration = 0

    def create_download_batch(self, item):
        return get_video_batch(item['channel_name'], item['filename'])

    def download_files(self, item, batch_list):
        self.youtube_util.download_files(item, batch_list)

    def extract_metadata(self, item, file, url=None):
        meta_file_name = get_meta_filename(file)
        video_info = self.youtube_util.get_video_info(file, item)
        metadata = self.metadata_creator.create_metadata(video_info)
        metadata_df = pd.DataFrame([metadata])
        metadata_df.to_csv(meta_file_name, index=False)

    def process_item(self, item, spider):
        self.batch_count = 0
        self.storage_util.retrieve_archive_from_bucket(item["channel_name"])
        channel_videos_count = get_channel_videos_count(channels_path + item['filename'])
        logging.info(
            str("Total channel count with valid videos is {0}".format(channel_videos_count)))
        self.batch_download(item)
        # update_token_in_bucket()
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
                self.storage_util.upload_media_and_metadata_to_bucket(channel_name, file)
            self.storage_util.upload_archive_to_bucket(channel_name)
            logging.info(
                str("Uploaded files till now: {0}".format(self.batch_count)))
