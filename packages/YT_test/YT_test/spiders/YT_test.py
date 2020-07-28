import scrapy


class ExampleSpider(scrapy.Spider):
    name = 'YT_test'
    allowed_domains = ['youtube.com']
    start_urls = ['https://youtube.com/']

    def parse(self, response):
        return {"Channel": "https://www.youtube.com/channel/UCZ7HuQxnGJL76G8gQup_E7w"}
