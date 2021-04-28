import os

import scrapy.settings

from ..items import Media
from ..services.storage_util import StorageUtil


class DirectDownloadSpider(scrapy.Spider):
    name = "datacollector_direct_download"
    start_urls = ['https://www.youtube.com']

    custom_settings = {
        "CONCURRENT_ITEMS": "1",
        "MEDIA_ALLOW_REDIRECTS": "True",
        "ROBOTSTXT_OBEY": "False",
        "ITEM_PIPELINES": '{"data_acquisition_framework.pipelines.audio_pipeline.AudioPipeline": 1}'
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        StorageUtil().set_gcs_creds(str(kwargs["my_setting"]).replace("'", ""))
        self.language = "Bodo"
        self.language_code = "brx"

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

    def parse(self, response, **kwargs):
        urls_path = os.path.dirname(os.path.realpath(__file__)) + "/../download_urls.txt"
        with open(urls_path, "r") as f:
            urls = f.read().splitlines()
            for url in urls:
                source_domain = self.extract_source_domain(url)
                url_parts = url.split("/")
                yield Media(
                    title=url_parts[-1],
                    file_urls=[url],
                    source=source_domain,
                    license_urls=[],
                    language=self.language,
                    source_url=url
                )

    def extract_source_domain(self, base_url):
        source_domain = base_url[base_url.index("//") + 2:].split("/")[0]
        if source_domain.startswith("www."):
            source_domain = source_domain.replace("www.", "")
        return source_domain
