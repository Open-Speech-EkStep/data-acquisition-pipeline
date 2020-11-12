import concurrent.futures
import html
import json
import os
import re
from urllib.parse import urlparse
import logging
import scrapy
import scrapy.settings

from ..items import Media, LicenseItem
from ..services.storage_util import StorageUtil


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
        config_path = (
                os.path.dirname(os.path.realpath(__file__)) + "/../configs/web_crawl_config.json"
        )
        with open(config_path, "r") as f:
            config = json.load(f)
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
                        logging.info("%r generated an exception: %s" % (url, exc))

    def parse_results_url(self, url):
        return scrapy.Request(url, callback=self.parse, cb_kwargs=dict(depth=1))

    def extract_license_urls(self, urls, all_a_tags, response):
        license_urls = set()
        for url in urls:
            url = url.rstrip().lstrip()
            if url.startswith("https://creativecommons.org/publicdomain/mark") or url.startswith(
                    "https://creativecommons.org/publicdomain/zero") or url.startswith(
                "https://creativecommons.org/licenses/by"):
                license_urls.add(url)
        if len(license_urls) == 0:
            for a_tag in all_a_tags:
                texts = a_tag.xpath('text()').extract()
                for text in texts:
                    text = text.lower()
                    if "terms" in text or "license" in text or "copyright" in text or "usage policy" in text or "conditions" in text or "website policies" in text or "website policy" in text:
                        for link in a_tag.xpath('@href').extract():
                            license_urls.add(response.urljoin(link))
        return list(license_urls)

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

    def sanitize(self, word):
        return word.rstrip().lstrip().lower()

    def is_unwanted_wiki(self, url):
        url = self.sanitize(url)
        if "wikipedia.org" in url or "wikimedia.org" in url:
            url = url.replace("https://", "").replace("http://", "")
            if not url.startswith("en") or not url.startswith(self.language_code) or not url.startswith("wiki"):
                return True
        return False

    def write(self, content):
        with open("log.txt", 'a') as f:
            f.write(content + "\n")

    def parse(self, response, depth):

        if self.enable_hours_restriction and (self.total_duration_in_seconds >= self.max_seconds):
            return

        base_url = response.url
        all_a_tags = response.xpath('//a')
        a_urls = response.css("a::attr(href)").getall()
        source_urls = response.css("source::attr(src)").getall()
        audio_tag_urls = response.css("audio::attr(src)").getall()

        urls = a_urls + source_urls + audio_tag_urls

        source_domain = base_url[base_url.index("//") + 2:].split("/")[0]

        if source_domain.startswith("www."):
            source_domain = source_domain.replace("www.", "")

        license_urls = self.extract_license_urls(a_urls, all_a_tags, response)
        license_extracted = False

        for url in urls:

            if self.enable_hours_restriction and (self.total_duration_in_seconds >= self.max_seconds):
                return

            url = response.urljoin(url)

            try:
                urlparse(url)
            except:
                continue

            if self.is_unwanted_words_present(url) or self.is_unwanted_wiki(url):
                continue

            if self.is_extension_present(url):
                if not license_extracted:
                    for license_item in self.extract_license(license_urls, source_domain):
                        yield license_item
                    license_extracted = True
                url_parts = url.split("/")
                yield Media(
                    title=url_parts[len(url_parts) - 1],
                    file_urls=[url],
                    source=source_domain,
                    license_urls=license_urls,
                    language=self.language,
                    source_url=base_url
                )
                self.write(url)
                continue

            if self.is_unwanted_extension_present(url) or depth >= self.depth:
                continue

            self.write(url)
            # if not matched any of above, traverse to next
            yield scrapy.Request(
                url, callback=self.parse, cb_kwargs=dict(depth=(depth + 1))
            )

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
