# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import json
import os

from scrapy import signals
# useful for handling different item types with a single interface
from scrapy.exceptions import IgnoreRequest
from scrapy.linkextractors import IGNORED_EXTENSIONS


class DataAcquisitionFrameworkSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class DataAcquisitionFrameworkDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class CrawlerDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_closed, signal=signals.spider_closed)
        return s

    def spider_closed(self, spider):
        spider.logger.info('Spider closed: {0} with {1} not crawled'.format(spider.name, str(len(self.visited_urls))))

    def __init__(self):
        self.visited_urls = []
        web_crawl_config_path = os.path.dirname(os.path.realpath(__file__)) + "/web_crawl_config.json"
        self.bing_archive_path = os.path.dirname(os.path.realpath(__file__)) + "/bing_archive.txt"
        with open(web_crawl_config_path, 'r') as f:
            config = json.load(f)
            self.word_to_ignore = config["word_to_ignore"]
            self.extensions_to_ignore = set(config["extensions_to_ignore"] + IGNORED_EXTENSIONS)
        # self.bing_archive = []
        # if os.path.exists(self.bing_archive_path):
        #     with open(self.bing_archive_path,'r') as f:
        #         self.bing_archive = set(f.read().splitlines())
        # else:
        #     print("Not present")

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called

        # if request.url.rstrip() in self.bing_archive:
        #     raise IgnoreRequest()

        for word in self.word_to_ignore:
            if word.lower() in request.url.lower():
                raise IgnoreRequest()

        for ext in self.extensions_to_ignore:
            if request.url.lower().endswith(ext):
                raise IgnoreRequest()
        # with open(self.bing_archive_path,'a') as f:
        #     f.write(request.url+"\n")
        self.visited_urls.append(request.url)
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        try:
            self.visited_urls.remove(response.url)
        except ValueError:
            spider.logger.info("%s url not found" % response.url)
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass
