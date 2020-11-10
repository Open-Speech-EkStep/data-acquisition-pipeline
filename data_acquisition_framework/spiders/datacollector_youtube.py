import glob
import os

import scrapy

from ..configs.paths import playlist_path
from ..items import YoutubeItem
from ..utilites import *
from ..youtube_api import YoutubePlaylistCollector
from ..youtube_util import YoutubeUtil, check_mode


class DatacollectorYoutubeSpider(scrapy.Spider):
    name = 'datacollector_youtube'
    allowed_domains = ['youtube.com']
    start_urls = ['http://youtube.com/']
    count = 0

    custom_settings = {
        'CONCURRENT_ITEMS': '1',
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        set_gcs_creds(str(kwargs["my_setting"]).replace("\'", ""))
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

    def parse(self, response, **kwargs):
        if not os.path.exists(download_path):
            os.system("mkdir " + download_path)
        if os.path.exists(playlist_path):
            os.system('rm -rf ' + playlist_path)
        scraped_data = check_mode()
        is_file_mode = True
        if scraped_data is None:
            is_file_mode = False
            self.scrape_links()
        for source_file in glob.glob(playlist_path + '*.txt'):
            source_file_name = source_file.replace(playlist_path, '')
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

    def scrape_links(self):
        if len(channel_url_dict) != 0:
            source_channel_dict = channel_url_dict
        else:
            with open('token.txt', 'w') as f:
                pass
            # get_token_from_bucket()
            source_channel_dict = YoutubePlaylistCollector().get_urls()
        YoutubeUtil().create_channel_playlist(source_channel_dict)
