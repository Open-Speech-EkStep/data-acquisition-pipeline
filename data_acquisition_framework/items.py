# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class DataAcquisitionFrameworkItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class Media(scrapy.Item):
    title = scrapy.Field()
    file_urls = scrapy.Field()
    files = scrapy.Field()
    source = scrapy.Field()
    source_url = scrapy.Field()
    license_urls = scrapy.Field()
    language = scrapy.Field()
    duration = scrapy.Field()


class YoutubeItem(scrapy.Item):
    channel_name = scrapy.Field()
    channel_id = scrapy.Field()
    filename = scrapy.Field()
    filemode_data = scrapy.Field()


class LicenseItem(scrapy.Item):
    key_name = scrapy.Field()
    source = scrapy.Field()
    file_urls = scrapy.Field()
    files = scrapy.Field()
    content = scrapy.Field()
    language = scrapy.Field()