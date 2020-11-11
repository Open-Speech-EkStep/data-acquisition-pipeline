import glob
import logging
import os

import scrapy

from data_acquisition_framework.services.youtube_util import YoutubeUtil, create_channel_file_for_file_mode
from ..configs.paths import channels_path, download_path, archives_base_path
from ..configs.pipeline_config import channel_url_dict, mode, source_name, file_url_name_column, channel_blob_path, \
    scraped_data_blob_path
from ..items import YoutubeItem
from ..services.storage_util import StorageUtil


class DatacollectorYoutubeSpider(scrapy.Spider):
    name = 'datacollector_youtube'
    allowed_domains = ['youtube.com']
    count = 0

    custom_settings = {
        'CONCURRENT_ITEMS': '1',
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.storage_util = StorageUtil()
        self.storage_util.set_gcs_creds(str(kwargs["my_setting"]).replace("\'", ""))
        os.environ["youtube_api_key"] = str(kwargs["youtube_api_key"])

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(
            *args,
            my_setting=crawler.settings.get("GCS_CREDS") if "scrapinghub" in os.path.abspath("~") else open(
                "./credentials.json").read(),
            youtube_api_key=crawler.settings.get("YOUTUBE_API_KEY") if "scrapinghub" in os.path.abspath("~") else open(
                "./.youtube_api_key").read(),
            **kwargs
        )
        spider._set_crawler(crawler)
        return spider

    def start_requests(self):
        self.clear_required_directories()
        scraped_data = self.check_mode()
        is_file_mode = True
        if scraped_data is None:
            is_file_mode = False
            self.scrape_links()
        for source_file in glob.glob(channels_path + '*.txt'):
            source_file_name = source_file.replace(channels_path, '')
            channel_id = source_file_name.split("__")[0]
            channel_name = source_file_name.replace(channel_id + "__", "").replace('.txt', '')
            if is_file_mode:
                channel_id = None
            logging.info(
                str("Channel {0}".format(source_file_name)))
            yield YoutubeItem(
                channel_name=channel_name,
                channel_id=channel_id,
                filename=source_file_name,
                filemode_data=scraped_data)

    def clear_required_directories(self):
        if os.path.exists(download_path):
            os.system('rm -rf ' + download_path)
        else:
            os.system("mkdir " + download_path)
        if os.path.exists(channels_path):
            os.system('rm -rf ' + channels_path)
        if os.path.exists(archives_base_path):
            os.system('rm -rf ' + archives_base_path)

    def scrape_links(self):
        youtube_util = YoutubeUtil()
        if len(channel_url_dict) != 0:
            source_channel_dict = channel_url_dict
        else:
            with open('token.txt', 'w') as f:
                pass
            # get_token_from_bucket()
            source_channel_dict = youtube_util.get_channels()
        youtube_util.create_channel_file(source_channel_dict)

    def check_mode(self):
        if mode == "file":
            videos_file_path = self.get_videos_file_path_in_bucket()
            if self.storage_util.check(videos_file_path):
                self.storage_util.download(videos_file_path, source_name + ".csv")
                logging.info(
                    str("Source scraped file has been downloaded from bucket to local path..."))
                scraped_data = create_channel_file_for_file_mode(source_name + ".csv", file_url_name_column)
                return scraped_data
            else:
                logging.error(str("{0} File doesn't exists on the given location: {1}".format(source_name + ".csv",
                                                                                              videos_file_path)))
                exit()
        if mode == "channel":
            return None
        else:
            logging.error("Invalid mode")
            exit()

    def get_videos_file_path_in_bucket(self):
        return channel_blob_path + '/' + scraped_data_blob_path + '/' + source_name + '.csv'
