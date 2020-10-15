import scrapy
from datetime import datetime
import json
import scrapy.settings
import concurrent.futures

from ..utilites import *


class Media(scrapy.Item):
    title = scrapy.Field()
    file_urls = scrapy.Field()
    files = scrapy.Field()
    source = scrapy.Field()
    license_urls = scrapy.Field()


class UrlSearchSpider(scrapy.Spider):
    name = "url_search"

    custom_settings = {
        "DOWNLOAD_MAXSIZE": "0",
        "DOWNLOAD_WARNSIZE": "999999999",
        "RETRY_TIMES": "0",
        # 'DEPTH_PRIORITY':'1',
        "ROBOTSTXT_OBEY": "False",
        # 'SCHEDULER_DISK_QUEUE':'scrapy.squeues.PickleFifoDiskQueue',
        # 'SCHEDULER_MEMORY_QUEUE' : 'scrapy.squeues.FifoMemoryQueue',
        "ITEM_PIPELINES": '{"data_acquisition_framework.pipelines.AudioPipeline": 1}',
        "MEDIA_ALLOW_REDIRECTS": "True",
        "LOG_LEVEL": "INFO",
        "REACTOR_THREADPOOL_MAXSIZE": "20",
        "CONCURRENT_REQUESTS": "32",
        "SCHEDULER_PRIORITY_QUEUE": "scrapy.pqueues.DownloaderAwarePriorityQueue",
        "COOKIES_ENABLED": "False",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        set_gcs_creds(str(kwargs["my_setting"]).replace("'", ""))

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(
            *args,
            my_setting=crawler.settings.get("GCS_CREDS")
            if "scrapinghub" in os.path.abspath("~")
            else open("./credentials.json").read(),
            **kwargs
        )
        spider._set_crawler(crawler)
        return spider

    def start_requests(self):
        bing_config_path = (
            os.path.dirname(os.path.realpath(__file__)) + "/../bing_config.json"
        )
        with open(bing_config_path, "r") as f:
            config = json.load(f)
            self.word_to_ignore = config["word_to_ignore"]
            self.extensions_to_include = config["extensions_to_include"]
            self.extensions_to_ignore = config["extensions_to_ignore"]
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

    def parse(self, response, depth):
        base_url = response.url
        a_urls = response.css("a::attr(href)").getall()
        source_urls = response.css("source::attr(src)").getall()
        urls = []
        if len(a_urls) != 0:
            urls += a_urls
        if len(source_urls) != 0:
            urls += source_urls
        source = base_url[base_url.index("//") + 2 :].split("/")[0]
        license_urls = []
        for url in a_urls:
            url = url.rstrip().lstrip()
            if url.startswith("https://creativecommons.org/publicdomain/mark") or url.startswith("https://creativecommons.org/publicdomain/zero") or url.startswith("https://creativecommons.org/licenses/by"):
                license_urls.append(url)
        for url in urls:
            url = response.urljoin(url)
            flag = False

            # iterate for search of unwanted words
            for word in self.word_to_ignore:
                if word in url.lower():
                    flag = True
                    break
            if flag:
                continue

            # download urls from drive
            if "drive.google.com" in url and "export=download" in url:
                # url_parts = url.split("=")
                # yield Media(
                #     title=url_parts[-1],
                #     file_urls=[url],
                #     source=source,
                #     license_urls=license_urls,
                # )
                continue

            # iterate for search of wanted files
            for extension in self.extensions_to_include:
                if url.lower().endswith(extension.lower()):
                    url_parts = url.split("/")
                    yield Media(
                        title=url_parts[len(url_parts) - 1],
                        file_urls=[url],
                        source=source,
                        license_urls=license_urls,
                    )
                    flag = True
                    break

            if flag:
                continue

            # iterate for search of unwanted extension files
            for extension in self.extensions_to_ignore:
                if url.lower().endswith(extension):
                    flag = True
                    break

            if flag:
                continue

            if (depth + 1) >= self.depth:
                continue

            # if not matched any of above, traverse to next
            yield scrapy.Request(
                url, callback=self.parse, cb_kwargs=dict(depth=(depth + 1))
            )
