import json

import scrapy
from ..utilites import *

class DatacollectorYoutubeSpider(scrapy.Spider):
    name = 'datacollector_youtube'
    allowed_domains = ['youtube.com']
    start_urls = ['http://youtube.com/']
    count = 0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        set_gcs_creds(str(kwargs["my_setting"]).replace("\'", ""))

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(
            *args,
            my_setting=crawler.settings.get("GCS_CREDS") if "scrapinghub" in os.path.abspath("~") else open("./credentials.json").read(),
            **kwargs
        )
        spider._set_crawler(crawler)
        return spider


    def parse(self, response):
        return {"Channel": "https://www.youtube.com/channel/UCZ7HuQxnGJL76G8gQup_E7w"}
