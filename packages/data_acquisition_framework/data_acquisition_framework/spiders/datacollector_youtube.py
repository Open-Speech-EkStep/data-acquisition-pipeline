import json

import scrapy


class DatacollectorYoutubeSpider(scrapy.Spider):
    name = 'datacollector_youtube'
    allowed_domains = ['youtube.com']
    start_urls = ['http://youtube.com/']

    def parse(self, response):
        gcs_credentials = json.loads(self.settings['GCS_CREDS'])["Credentials"]
        return {"Channel": "https://www.youtube.com/channel/UCZ7HuQxnGJL76G8gQup_E7w",
                "Gcs_Credentials": gcs_credentials}
