import scrapy
import json

class ExampleSpider(scrapy.Spider):
    name = 'YT_test'
    allowed_domains = ['youtube.com']
    start_urls = ['https://youtube.com/']

    def parse(self, response):
        gcs_credentials = json.loads(self.settings['GCS_CREDS'])["Credentials"]
        return {"Channel": "https://www.youtube.com/channel/UCZ7HuQxnGJL76G8gQup_E7w",
                "Gcs_Credentials": gcs_credentials}
