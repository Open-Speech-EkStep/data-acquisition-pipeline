import concurrent.futures
from concurrent.futures import as_completed
import html
import logging
import os
import re
from urllib.parse import urlparse

import scrapy
import scrapy.settings
from scrapy import signals

from data_acquisition_framework.services.loader_util import load_config_file
from ..items import Media, LicenseItem
from ..services.storage_util import StorageUtil
from ..utilities import is_unwanted_words_present, is_unwanted_wiki, write, is_extension_present, \
    is_unwanted_extension_present, extract_license_urls


class UrlSearchSpider(scrapy.Spider):
    name = "datacollector_urls"

    custom_settings = {
        "DOWNLOAD_MAXSIZE": "0",
        "DOWNLOAD_WARNSIZE": "999999999",
        "RETRY_TIMES": "0",
        "ROBOTSTXT_OBEY": "False",
        "ITEM_PIPELINES": '{"data_acquisition_framework.pipelines.audio_pipeline.AudioPipeline": 1}',
        "MEDIA_ALLOW_REDIRECTS": "True",
        "REACTOR_THREADPOOL_MAXSIZE": "20",
        # "DOWNLOAD_DELAY":'2.0',
        "AUTOTHROTTLE_ENABLED": 'True',
        # "CONCURRENT_REQUESTS": "32",
        "SCHEDULER_PRIORITY_QUEUE": "scrapy.pqueues.DownloaderAwarePriorityQueue",
        "COOKIES_ENABLED": "False",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        StorageUtil().set_gcs_creds(str(kwargs["my_setting"]).replace("'", ""))
        self.total_duration_in_seconds = 0
        config = load_config_file('web_crawl_config.json')
        self.language = config["language"]
        self.language_code = config["language_code"]
        self.max_seconds = config["max_hours"] * 3600
        self.word_to_ignore = config["word_to_ignore"]
        self.extensions_to_include = config["extensions_to_include"]
        self.extensions_to_ignore = config["extensions_to_ignore"]
        self.enable_hours_restriction = config["enable_hours_restriction"].lower() == "yes"
        self.depth = config["depth"]

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
        if item is not None and "duration" in item:
            self.total_duration_in_seconds += item["duration"]
        hours = self.total_duration_in_seconds / 3600
        logging.info(spider.name + " has downloaded %s hours" % str(hours))

    def start_requests(self):
        urls_path = os.path.dirname(os.path.realpath(__file__)) + "/../urls.txt"
        with open(urls_path, "r") as f:
            urls = f.read().splitlines()
            with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
                future_to_url = {
                    executor.submit(self.parse_results_url, url): url
                    for url in urls
                }
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        data = future.result()
                        if data is not None:
                            yield data
                    except Exception as exc:
                        logging.info("%r generated an exception: %s" % (url, exc))

    def parse_results_url(self, url):
        return scrapy.Request(url, callback=self.parse, cb_kwargs=dict(depth=1))

    def parse(self, response, depth):
        if self.enable_hours_restriction and (self.total_duration_in_seconds >= self.max_seconds):
            return

        base_url = response.url
        all_a_tags = response.xpath('//a')
        a_urls, urls = self.extract_media_urls(response)
        source_domain = self.extract_source_domain(base_url)

        license_urls = extract_license_urls(a_urls, all_a_tags, response)
        license_extracted = False

        for url in urls:

            if self.enable_hours_restriction and (self.total_duration_in_seconds >= self.max_seconds):
                return

            url = response.urljoin(url)

            try:
                url_scheme = urlparse(url).scheme
                if url_scheme != "http" and url_scheme != "https":
                    continue
            except Exception as exc:
                print(exc)
                continue

            if is_unwanted_words_present(self.word_to_ignore, url) or is_unwanted_wiki(self.language_code, url):
                continue

            if is_extension_present(self.extensions_to_include, url):
                if not license_extracted:
                    for license_item in self.extract_license(license_urls, source_domain):
                        yield license_item
                    license_extracted = True
                url_parts = url.split("/")
                yield Media(
                    title=url_parts[-1],
                    file_urls=[url],
                    source=source_domain,
                    license_urls=license_urls,
                    language=self.language,
                    source_url=base_url
                )
                write("log.txt", url)
                continue

            if is_unwanted_extension_present(self.extensions_to_ignore, url) or depth >= self.depth:
                continue

            write("log.txt", url)
            # if not matched any of above, traverse to next
            yield scrapy.Request(
                url, callback=self.parse, cb_kwargs=dict(depth=(depth + 1))
            )

    def extract_source_domain(self, base_url):
        source_domain = base_url[base_url.index("//") + 2:].split("/")[0]
        if source_domain.startswith("www."):
            source_domain = source_domain.replace("www.", "")
        return source_domain

    def extract_media_urls(self, response):
        a_urls = response.css("a::attr(href)").getall()
        source_urls = response.css("source::attr(src)").getall()
        audio_tag_urls = response.css("audio::attr(src)").getall()
        urls = a_urls + source_urls + audio_tag_urls
        return a_urls, urls

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
