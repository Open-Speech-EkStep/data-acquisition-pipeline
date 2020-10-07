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

class BingSearchSpider(scrapy.Spider):
    name = "bing_search"

    custom_settings = {
        'DOWNLOAD_MAXSIZE': '0',
        'DOWNLOAD_WARNSIZE': '999999999',
        'RETRY_TIMES': '0',
        # 'DEPTH_PRIORITY':'1',
        'ROBOTSTXT_OBEY':'False',
        # 'SCHEDULER_DISK_QUEUE':'scrapy.squeues.PickleFifoDiskQueue',
        # 'SCHEDULER_MEMORY_QUEUE' : 'scrapy.squeues.FifoMemoryQueue',
        'ITEM_PIPELINES':'{"data_acquisition_framework.pipelines.AudioPipeline": 1}',
        'MEDIA_ALLOW_REDIRECTS':'True',
        'LOG_LEVEL':'INFO',
        'REACTOR_THREADPOOL_MAXSIZE':'20',
        'CONCURRENT_REQUESTS':'32',
        'SCHEDULER_PRIORITY_QUEUE':'scrapy.pqueues.DownloaderAwarePriorityQueue',
        'COOKIES_ENABLED':'False',
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


    def start_requests(self):
        bing_config_path = os.path.dirname(os.path.realpath(__file__)) + "/../bing_config.json"
        with open(bing_config_path,'r') as f:
            config = json.load(f)
            self.depth = config["depth"]
            self.pages = config["pages"]
            self.word_to_ignore = config["word_to_ignore"]
            self.extensions_to_ignore = config["extensions_to_ignore"]
            self.extensions_to_include = config["extensions_to_include"]
            isNew = False
            if config["continue"].lower() == "yes":
                self.page = config["last_visited"]
            else:
                isNew = True
                self.page = 0
            for keyword in config["keywords"]:
                keyword = keyword.replace(" ","+")
                url="http://www.bing.com/search?q={0}&first={1}".format(keyword, self.page)
                yield scrapy.Request(url=url, callback=self.bing_parse, cb_kwargs=dict(count=1,keyword=keyword))
        config["last_visited"] = (self.pages * 10) + 0 if isNew else config["last_visited"]
        with open(bing_config_path,'w') as jsonfile:
            json.dump(config, jsonfile)  

    def bing_parse(self, response, count,keyword):
        urls = response.css('a::attr(href)').getall()
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            future_to_url = f
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    data = future.result()
                    if data is not None:
                        yield data
                except Exception as exc:
                    print('%r generated an exception: %s' % (url, exc))
        # for url in urls:
        #     if url.startswith("http:") or url.startswith("https:"):
        #         if url.startswith("https://www.youtube.com") or "go.microsoft.com/fwlink" in url or "www.microsofttranslator.com" in url or "www.bing.com" in url or "www.microsoft.com" in url:
        #             continue
        #         if url.endswith(".pdf") or url.endswith(".jpg") or url.endswith(".png"):
        #             continue
        #         yield scrapy.Request(url, callback=self.parse, cb_kwargs=dict(depth=1))
        self.page = self.page + 10
        formattedUrl="http://www.bing.com/search?q={0}&first={1}".format(keyword, self.page)
        c = count+1
        if c > self.pages:
            return
        #next_url = response.urljoin(formattedUrl)
        yield scrapy.Request(formattedUrl, callback=self.bing_parse, cb_kwargs=dict(count=c,keyword=keyword))

    def parse_results_url(self, url):
        if url.startswith("http:") or url.startswith("https:"):
            if url.startswith("https://www.youtube.com") or "go.microsoft.com/fwlink" in url or "www.microsofttranslator.com" in url or "www.bing.com" in url or "www.microsoft.com" in url:
                return None
            if url.endswith(".pdf") or url.endswith(".jpg") or url.endswith(".png"):
                return None
            return scrapy.Request(url, callback=self.parse, cb_kwargs=dict(depth=1))


    def parse(self, response, depth):
        base_url = response.url
        a_urls = response.css('a::attr(href)').getall()
        source_urls = response.css('source::attr(src)').getall()
        urls = []
        if len(a_urls) != 0:
            urls += a_urls
        if len(source_urls) != 0:
            urls += source_urls
        source = base_url[base_url.index("//")+2:].split('/')[0]
        # with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        #     future_to_url = {executor.submit(self.process_url, url, source, depth, response): url for url in urls}
        #     for future in concurrent.futures.as_completed(future_to_url):
        #         url = future_to_url[future]
        #         try:
        #             data = future.result()
        #             if data is not None:
        #                 yield data
        #         except Exception as exc:
        #             print('%r generated an exception: %s' % (url, exc))
        for url in urls:
            url = response.urljoin(url)
            flag = False

            # iterate for search of unwanted words
            for word in self.word_to_ignore:
                if word in url.lower():
                    flag = True
                    break
            if flag: continue

            # download urls from drive
            if "drive.google.com" in url and "export=download" in url:
                url_parts = url.split("/")
                yield Media(title=url_parts[len(url_parts)-1], file_urls=[url], source=source)
                continue

            # iterate for search of wanted files
            for extension in self.extensions_to_include:
                if url.lower().endswith(extension.lower()):
                    url_parts = url.split("/")
                    yield Media(title=url_parts[len(url_parts)-1], file_urls=[url], source=source)
                    flag = True
                    break
            
            if flag: continue

            # iterate for search of unwanted extension files
            for extension in self.extensions_to_ignore:
                if url.lower().endswith(extension):
                    flag = True
                    break
            
            if flag: continue

            if (depth+1) >= self.depth:
                continue

            # if not matched any of above, traverse to next
            yield scrapy.Request(url, callback=self.parse, cb_kwargs=dict(depth=(depth+1)))
    
    # def process_url(self, url, source, depth, response):
    #     url = response.urljoin(url)

    #     # iterate for search of unwanted words
    #     for word in self.word_to_ignore:
    #         if word in url.lower():
    #             return None

    #     # download urls from drive
    #     if "drive.google.com" in url and "export=download" in url:
    #         url_parts = url.split("/")
    #         return Media(title=url_parts[len(url_parts)-1], file_urls=[url], source=source)

    #     # iterate for search of wanted files
    #     for extension in self.extensions_to_include:
    #         if url.lower().endswith(extension.lower()):
    #             url_parts = url.split("/")
    #             return Media(title=url_parts[len(url_parts)-1], file_urls=[url], source=source)

    #     # iterate for search of unwanted extension files
    #     for extension in self.extensions_to_ignore:
    #         if url.lower().endswith(extension):
    #             return None

    #     if (depth+1) >= self.depth:
    #         return None

    #     # if not matched any of above, traverse to next
    #     return scrapy.Request(url, callback=self.parse, cb_kwargs=dict(depth=(depth+1)))

