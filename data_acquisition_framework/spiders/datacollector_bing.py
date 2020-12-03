import concurrent.futures
import html
import json
import os
import re
from urllib.parse import urlparse
import logging

import scrapy
import scrapy.settings
from scrapy import signals

from ..items import Media, LicenseItem
from ..services.storage_util import StorageUtil
from ..utilities import extract_license_urls, is_unwanted_words_present, is_unwanted_extension_present, \
    is_extension_present, is_unwanted_wiki, write

from data_acquisition_framework.services.loader_util import load_web_crawl_config

class BingSearchSpider(scrapy.Spider):
    name = "datacollector_bing"

    custom_settings = {
        'DOWNLOAD_MAXSIZE': '0',
        'DOWNLOAD_WARNSIZE': '999999999',
        'RETRY_TIMES': '0',
        'ROBOTSTXT_OBEY': 'False',
        'ITEM_PIPELINES': '{"data_acquisition_framework.pipelines.audio_pipeline.AudioPipeline": 1}',
        'MEDIA_ALLOW_REDIRECTS': 'True',
        'REACTOR_THREADPOOL_MAXSIZE': '20',
        # 'DOWNLOAD_DELAY':'2.0',
        'AUTOTHROTTLE_ENABLED': 'True',
        # 'HTTPCACHE_ENABLED':'True',
        # "USER_AGENT":'Ekstep-Crawler (bot@mycompany.com)',
        # 'CONCURRENT_REQUESTS':'32',
        # 'CONCURRENT_ITEMS':'200',
        'SCHEDULER_PRIORITY_QUEUE': 'scrapy.pqueues.DownloaderAwarePriorityQueue',
        'COOKIES_ENABLED': 'False',
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        StorageUtil().set_gcs_creds(str(kwargs["my_setting"]).replace("\'", ""))
        self.total_duration_in_seconds = 0
        self.web_crawl_config = os.path.dirname(os.path.realpath(__file__)) + "/../configs/web_crawl_config.json"
        config = load_web_crawl_config()
        self.config = config
        self.language = config["language"]
        self.language_code = config["language_code"]
        self.max_seconds = config["max_hours"] * 3600
        self.depth = config["depth"]
        self.pages = config["pages"]
        self.max_hours = config["max_hours"]
        self.extensions_to_include = config["extensions_to_include"]
        self.word_to_ignore = config["word_to_ignore"]
        self.extensions_to_ignore = config["extensions_to_ignore"]
        self.is_continued = config["continue_page"].lower() == "yes"
        self.enable_hours_restriction = config["enable_hours_restriction"].lower() == "yes"

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(
            *args,
            my_setting=crawler.settings.get("GCS_CREDS") if "scrapinghub" in os.path.abspath("~") else open(
                "./credentials.json").read(),
            **kwargs
        )
        crawler.signals.connect(spider.item_scraped, signal=scrapy.signals.item_scraped)
        spider._set_crawler(crawler)
        return spider

    def item_scraped(self, item, response, spider):
        if item is not None and "duration" in item:
            self.total_duration_in_seconds += item["duration"]
        hours = self.total_duration_in_seconds / 3600
        logging.info(spider.name + " has downloaded %s hours" % str(hours))

    def start_requests(self):
        start_page = 0
        if self.is_continued:
            start_page = self.config["last_visited"]
        for keyword in self.config["keywords"]:
            keyword = self.language + "+" + keyword.replace(" ", "+")
            if start_page == 0:
                url = "https://www.bing.com/search?q={0}".format(keyword)
            else:
                url = "https://www.bing.com/search?q={0}&first={1}".format(keyword, start_page)
            yield scrapy.Request(url=url, callback=self.parse_search_page,
                                 cb_kwargs=dict(page_number=1, keyword=keyword))
        self.config["last_visited"] = (self.pages * 10) + start_page
        with open(self.web_crawl_config, 'w') as web_crawl_config_file:
            json.dump(self.config, web_crawl_config_file, indent=4)

    def parse_search_page(self, response, page_number, keyword):
        urls = response.css('a::attr(href)').getall()
        search_result_urls = self.filter_unwanted_urls(response, urls)
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            future_to_url = {executor.submit(self.get_request_for_search_result, url): url for url in
                             search_result_urls}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    data = future.result()
                    if data is not None:
                        yield data
                except Exception as exc:
                    logging.error('%r generated an exception: %s' % (url, exc))
        page = page_number * 10
        formatted_url = "https://www.bing.com/search?q={0}&first={1}".format(keyword, page)
        page_number += 1
        if page_number <= self.pages:
            yield scrapy.Request(formatted_url, callback=self.parse_search_page,
                                 cb_kwargs=dict(page_number=page_number, keyword=keyword))

    def get_request_for_search_result(self, url):
        return scrapy.Request(url, callback=self.parse, cb_kwargs=dict(depth=1))

    def filter_unwanted_urls(self, response, urls):
        write("base_url.txt", response.url)
        search_result_urls = []
        write("results.txt", "%s results = [\n" % response.url)
        for url in urls:
            if url.startswith("http:") or url.startswith("https:"):
                if "go.microsoft.com" not in url and "microsofttranslator.com" not in url:
                    search_result_urls.append(url)
                    write("results.txt", url + "\n")
        write("results.txt", "\n]\n")
        return search_result_urls

    def parse(self, response, depth):
        if self.enable_hours_restriction and (self.total_duration_in_seconds >= self.max_seconds):
            return
        base_url = response.url
        a_urls = response.css('a::attr(href)').getall()
        source_urls = response.css('source::attr(src)').getall()
        audio_tag_urls = response.css("audio::attr(src)").getall()

        urls = a_urls + source_urls + audio_tag_urls
        source_domain = base_url[base_url.index("//") + 2:].split('/')[0]
        if source_domain.startswith("www."):
            source_domain = source_domain.replace("www.", "")
        all_a_tags = response.xpath('//a')
        license_urls = extract_license_urls(a_urls, all_a_tags, response)
        license_extracted = False

        for url in urls:
            if self.enable_hours_restriction and (self.total_duration_in_seconds >= self.max_seconds):
                return

            url = response.urljoin(url)

            try:
                urlparse(url)
            except:
                continue

            if is_unwanted_words_present(self.word_to_ignore, url) or is_unwanted_wiki(self.language_code, url):
                continue

            if is_extension_present(self.extensions_to_include, url):
                if not license_extracted:
                    for license_item in self.extract_license(license_urls, source_domain):
                        logging.info("requesting license " + str(type(license_item)))
                        yield license_item
                    license_extracted = True
                url_parts = url.split("/")
                yield Media(
                    title=url_parts[-1],
                    file_urls=[url],
                    source_url=base_url,
                    source=source_domain,
                    license_urls=license_urls,
                    language=self.language)
                continue

            if is_unwanted_extension_present(self.extensions_to_ignore, url) or depth >= self.depth:
                continue

            # if not matched any of above, traverse to next
            yield scrapy.Request(url, callback=self.parse, cb_kwargs=dict(depth=(depth + 1)))

    def extract_license(self, license_urls, source_domain):
        for license_url in license_urls:
            if "creativecommons" in license_url:
                yield LicenseItem(file_urls=[license_url], key_name="creativecommons", source=source_domain,
                                  language=self.language)
            elif license_url.endswith(".pdf") or license_url.endswith(".epub") or license_url.endswith(
                    ".docx") or license_url.endswith(".doc"):
                name = "document"
                yield LicenseItem(file_urls=[license_url], key_name=name, source=source_domain,
                                  language=self.language)
            else:
                yield scrapy.Request(license_url, callback=self.parse_license, cb_kwargs=dict(source=source_domain))

    def parse_license(self, response, source):
        texts = response.xpath(
            "//body//text()[not (ancestor-or-self::script or ancestor-or-self::noscript or ancestor-or-self::style)]").extract()
        content = ""
        is_creative_commons = False
        for text in texts:
            text = html.unescape(text)
            text = text.rstrip().lstrip()
            text = text.replace("\r\n", "")
            text = re.sub(' +', ' ', text)
            if len(content) == 0:
                content = text
            else:
                content = content + "\n" + text
            if "creativecommons" in text:
                is_creative_commons = True
                break
        if is_creative_commons:
            yield LicenseItem(file_urls=[response.url], key_name="creativecommons", source=source,
                              language=self.language)
        else:
            yield LicenseItem(file_urls=[], key_name="html_page", source=source,
                              content=content,
                              language=self.language)
