import json

import scrapy
from ..utilites import *
import os

class DatacollectorYoutubeSpider(scrapy.Spider):
    name = 'datacollector_youtube'
    allowed_domains = ['youtube.com']
    start_urls = ['http://youtube.com/']
    count = 0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        set_gcs_creds(str(kwargs["my_setting"]).replace("\'", ""))
        os.environ["youtube_api_key"] = str(kwargs["youtube_api_key"])

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(
            *args,
            my_setting=crawler.settings.get("GCS_CREDS") if "scrapinghub" in os.path.abspath("~") else open("./credentials.json").read(),
            youtube_api_key=crawler.settings.get("YOUTUBE_API_KEY") if "scrapinghub" in os.path.abspath("~") else open(
                "./.youtube_api_key").read(),
            **kwargs
        )
        spider._set_crawler(crawler)
        return spider


    def parse(self, response):
        return {"Channel": "DUMMY"}
