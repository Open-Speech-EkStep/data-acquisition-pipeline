# -*- coding: utf-8 -*-
# from scrapy.http.request import Request
import json

import scrapy
import scrapy.settings

from ..utilites import *


class Songs(scrapy.Item):
    """docstring for Songs"""
    # def __init__(self, arg):
    # super(Songs, self).__init__()
    title = scrapy.Field()
    file_urls = scrapy.Field()
    files = scrapy.Field()


class MusicSpider(scrapy.Spider):
    name = 'datacollector_music'
    allowed_domains = ["musicforprogramming.net"]
    start_urls = ['http://musicforprogramming.net/']

    count = 0
    custom_settings = {
        'DOWNLOAD_MAXSIZE': '0',
        'DOWNLOAD_WARNSIZE': '999999999'
    }

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
        # gcs_credentials = json.loads(self.settings['GCS_CREDS'])["Credentials"]
        # set_gcs_credentials(gcs_credentials)
        # The name alone contains u'musicForProgramming("47: Abe Mangger");'
        # So we strip out to get the name of the song only
        title = response.css('title::text').extract_first()[len('musicForProgramming("'):-3]
        src = response.xpath('//audio[@id="player"]/@src').extract_first()

        self.count += 1
        if self.count >= 10:
            return

        yield Songs(title=title, file_urls=[src])

        next_url = response.xpath('//div[@id="episodes"]/a/@href').extract()[self.count]

        if next_url is not None:
            yield scrapy.Request(response.urljoin('http://musicforprogramming.net/' + next_url))


