import scrapy
from datetime import datetime
import json
import logging
import scrapy.settings
import concurrent.futures
from scrapy import signals
from ..utilites import *
from ..items import Media
from urllib.parse import urlparse


class BingSearchSpider(scrapy.Spider):
    name = "data_collector_bing"

    custom_settings = {
        'DOWNLOAD_MAXSIZE': '0',
        'DOWNLOAD_WARNSIZE': '999999999',
        'RETRY_TIMES': '0',
        'ROBOTSTXT_OBEY':'False',
        'ITEM_PIPELINES':'{"data_acquisition_framework.pipelines.AudioPipeline": 1}',
        'MEDIA_ALLOW_REDIRECTS':'True',
        'REACTOR_THREADPOOL_MAXSIZE':'20',
        # 'DOWNLOAD_DELAY':'2.0',
        'AUTOTHROTTLE_ENABLED': 'True',
        #'HTTPCACHE_ENABLED':'True',
        #"USER_AGENT":'Ekstep-Crawler (bot@mycompany.com)',
        # 'CONCURRENT_REQUESTS':'32',
        #'CONCURRENT_ITEMS':'200',
        'SCHEDULER_PRIORITY_QUEUE':'scrapy.pqueues.DownloaderAwarePriorityQueue',
        'COOKIES_ENABLED':'False',
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        set_gcs_creds(str(kwargs["my_setting"]).replace("\'", ""))
        self.total_duration_in_seconds = 0


    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(
            *args,
            my_setting=crawler.settings.get("GCS_CREDS") if "scrapinghub" in os.path.abspath("~") else open("./credentials.json").read(),
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
        self.web_crawl_config = os.path.dirname(os.path.realpath(__file__)) + "/../web_crawl_config.json"
        with open(self.web_crawl_config,'r') as f:
            config = json.load(f)
            self.language = config["language"]
            self.max_seconds = config["max_hours"] * 3600
            self.depth = config["depth"]
            self.pages = config["pages"]
            self.max_hours = config["max_hours"]
            self.extensions_to_include = config["extensions_to_include"]
            self.word_to_ignore = config["word_to_ignore"]
            self.extensions_to_ignore = config["extensions_to_ignore"]
            self.is_continued = config["continue_page"].lower() == "yes"
            self.enable_hours_restriction = config["enable_hours_restriction"].lower() == "yes"
            page = 0
            if self.is_continued:
                page = config["last_visited"]
            for keyword in config["keywords"]:
                keyword = self.language+"+"+keyword.replace(" ","+")
                if page == 0:
                    url="https://www.bing.com/search?q={0}".format(keyword)
                else:
                    url="https://www.bing.com/search?q={0}&first={1}".format(keyword, page)
                yield scrapy.Request(url=url, callback=self.bing_parse, cb_kwargs=dict(page_number=1,keyword=keyword))
            config["last_visited"] = (self.pages * 10) + page 
            with open(self.web_crawl_config,'w') as jsonfile:
                json.dump(config, jsonfile, indent=4)

    def bing_parse(self, response, page_number, keyword):
        self.write("base_url.txt", response.url)
        urls = response.css('a::attr(href)').getall()
        print("bing search page urls count=>", len(urls))
        search_result_urls = []
        self.write("results.txt","%s results = [\n"%response.url)
        for url in urls:
            if url.startswith("http:") or url.startswith("https:"):
                if "go.microsoft.com" not in url and not "microsofttranslator.com" in url:
                    search_result_urls.append(url)
                    self.write("results.txt",url+"\n")
        self.write("results.txt","\n]\n")
        print("filtered search page urls count=>", len(search_result_urls))
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
           future_to_url = {executor.submit(self.parse_results_url, url): url for url in search_result_urls}
           for future in concurrent.futures.as_completed(future_to_url):
               url = future_to_url[future]
               try:
                   data = future.result()
                   if data is not None:
                       yield data
               except Exception as exc:
                   print('%r generated an exception: %s' % (url, exc))
        page = page_number * 10
        formattedUrl="https://www.bing.com/search?q={0}&first={1}".format(keyword, page)
        page_number += 1
        if page_number <= self.pages:
            yield scrapy.Request(formattedUrl, callback=self.bing_parse, cb_kwargs=dict(page_number=page_number,keyword=keyword))

    def parse_results_url(self, url):
        return scrapy.Request(url, callback=self.parse, cb_kwargs=dict(depth=1))

    def parse(self, response, depth):
        if self.enable_hours_restriction and (self.total_duration_in_seconds >= self.max_seconds):
            return
        base_url = response.url
        a_urls = response.css('a::attr(href)').getall()
        source_urls = response.css('source::attr(src)').getall()
        urls = a_urls + source_urls
        source_domain = base_url[base_url.index("//")+2:].split('/')[0]

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
            
            # iterate for search of wanted files
            if self.is_extension_present(url):
                url_parts = url.split("/")
                yield Media(title=url_parts[len(url_parts)-1], file_urls=[url],source_url=base_url,source=source_domain, license_urls=license_urls,language=self.language)
                continue
            
            if self.is_unwanted_extension_present(url) or depth >= self.depth:
                continue

            # if not matched any of above, traverse to next
            yield scrapy.Request(url, callback=self.parse, cb_kwargs=dict(depth=(depth+1)))

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
    
    def write(self, filename, content):
        with open(filename,'a') as f:
           f.write(content+"\n")


