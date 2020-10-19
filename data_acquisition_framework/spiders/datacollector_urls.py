import scrapy
from datetime import datetime
import json
import scrapy.settings
import concurrent.futures

from ..utilites import *
from ..items import Media
from scrapy.exceptions import CloseSpider
from urllib.parse import urlparse


class UrlSearchSpider(scrapy.Spider):
    name = "url_search"

    custom_settings = {
        "DOWNLOAD_MAXSIZE": "0",
        "DOWNLOAD_WARNSIZE": "999999999",
        "RETRY_TIMES": "0",
        "ROBOTSTXT_OBEY": "False",
        "ITEM_PIPELINES": '{"data_acquisition_framework.pipelines.AudioPipeline": 1}',
        "MEDIA_ALLOW_REDIRECTS": "True",
        "REACTOR_THREADPOOL_MAXSIZE": "20",
        "DOWNLOAD_DELAY":'2.0',
        "AUTOTHROTTLE_ENABLED": 'True',
        # "CONCURRENT_REQUESTS": "32",
        "SCHEDULER_PRIORITY_QUEUE": "scrapy.pqueues.DownloaderAwarePriorityQueue",
        "COOKIES_ENABLED": "False",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        set_gcs_creds(str(kwargs["my_setting"]).replace("'", ""))
        self.total_duration_in_seconds = 0

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(
            *args,
            my_setting=crawler.settings.get("GCS_CREDS")
            if "scrapinghub" in os.path.abspath("~")
            else open("./credentials.json").read(),
            **kwargs
        )
        crawler.signals.connect(spider.item_scraped, signal=scrapy.signals.item_scraped)
        spider._set_crawler(crawler)
        return spider

    def item_scraped(self, item, response, spider):
        self.total_duration_in_seconds += item["duration"]
        hours = self.total_duration_in_seconds / 3600
        print(spider.name+" has downloaded %s hours"%str(hours))
        # if self.total_duration_in_seconds >= self.max_seconds:
        #     # raise CloseSpider("Stopped since %s hours of data scraped"% hours)
        #     self.crawler.engine.close_spider(spider,"Scraped estimated hours")

    def start_requests(self):
        config_path = (
            os.path.dirname(os.path.realpath(__file__)) + "/../web_crawl_config.json"
        )
        with open(config_path, "r") as f:
            config = json.load(f)
            self.language = config["language"]
            self.max_seconds = config["max_hours"] * 3600
            self.word_to_ignore = config["word_to_ignore"]
            self.extensions_to_include = config["extensions_to_include"]
            self.extensions_to_ignore = config["extensions_to_ignore"]
            self.enable_hours_restriction = config["enable_hours_restriction"].lower() == "yes"
            self.depth = config["depth"]
        urls_path = os.path.dirname(os.path.realpath(__file__)) + "/../urls.txt"
        with open(urls_path, "r") as f:
            with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
                future_to_url = {
                    executor.submit(self.parse_results_url, url): url
                    for url in f.readlines()
                }
                for future in concurrent.futures.as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        data = future.result()
                        if data is not None:
                            yield data
                    except Exception as exc:
                        print("%r generated an exception: %s" % (url, exc))

    def parse_results_url(self, url):
        return scrapy.Request(url, callback=self.parse, cb_kwargs=dict(depth=1))

    def extract_license_urls(self, urls):
        license_urls = []
        for url in urls:
            url = url.rstrip().lstrip()
            if url.startswith("https://creativecommons.org/publicdomain/mark") or url.startswith("https://creativecommons.org/publicdomain/zero") or url.startswith("https://creativecommons.org/licenses/by"):
                license_urls.append(url)
        return license_urls
    
    def is_unwanted_words_present(self, url):
        for word in self.word_to_ignore:
            if word in url.lower():
                return True
        return False
    
    def is_unwanted_extension_present(self, url):
        for extension in self.extensions_to_ignore:
            if url.lower().endswith(extension):
                return True
        return False

    def is_extension_present(self, url):
        for extension in self.extensions_to_include:
            if url.lower().endswith(extension.lower()):
                return True
        return False            


    def parse(self, response, depth):

        if self.enable_hours_restriction and (self.total_duration_in_seconds >= self.max_seconds):
            return

        base_url = response.url
        a_urls = response.css("a::attr(href)").getall()
        source_urls = response.css("source::attr(src)").getall()
        urls = a_urls + source_urls

        source_domain = base_url[base_url.index("//") + 2 :].split("/")[0]

        license_urls = self.extract_license_urls(a_urls)

        for url in urls:
            
            if self.enable_hours_restriction and (self.total_duration_in_seconds >= self.max_seconds):
                return

            url = response.urljoin(url)

            try:
                urlparse(url)
            except:
                continue

            if self.is_unwanted_words_present(url):
                continue

            if self.is_extension_present(url):
                url_parts = url.split("/")
                yield Media(
                    title=url_parts[len(url_parts) - 1],
                    file_urls=[url],
                    source=source_domain,
                    license_urls=license_urls,
                    language=self.language,
                    source_url=base_url
                )
                continue

            if self.is_unwanted_extension_present(url) or depth >= self.depth:
                continue

            # if not matched any of above, traverse to next
            yield scrapy.Request(
                url, callback=self.parse, cb_kwargs=dict(depth=(depth + 1))
            )
