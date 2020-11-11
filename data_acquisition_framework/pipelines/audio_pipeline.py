import logging
import os
from contextlib import suppress

import pandas as pd
from itemadapter import ItemAdapter
from scrapy import Request
from scrapy.pipelines.files import FilesPipeline

from data_acquisition_framework.configs.paths import download_path, archives_path
from data_acquisition_framework.metadata.metadata import MediaMetadata
from data_acquisition_framework.services.storage_util import StorageUtil
from data_acquisition_framework.utilities import get_file_format, get_media_info


class AudioPipeline(FilesPipeline):

    def __init__(self, store_uri, download_func=None, settings=None):
        super().__init__(store_uri, download_func, settings)
        if not os.path.exists(download_path):
            os.system("mkdir " + download_path)
        self.archive_list = {}
        self.metadata_creator = MediaMetadata()
        self.storage_util = StorageUtil()

    def file_path(self, request, response=None, info=None, **kwargs):
        file_name: str = request.url.split("/")[-1]
        file_name = file_name.replace("%", "_").replace(",", "_")
        return file_name

    def item_completed(self, results, item, info):
        duration_in_seconds = 0
        with suppress(KeyError):
            ItemAdapter(item)[self.files_result_field] = [x for ok, x in results if ok]
        if self.is_download_success(item):
            file_stats = item['files'][0]
            file = file_stats['path']
            url = file_stats['url']
            media_file_path = download_path + file
            if os.path.isfile(media_file_path):
                logging.info(str("***File {0} downloaded ***".format(file)))
                duration_in_seconds = self.upload_file_to_storage(file, item, media_file_path, url)
            else:
                logging.info(str("***File {0} not downloaded ***".format(item["title"])))
        item["duration"] = duration_in_seconds
        return item

    def upload_file_to_storage(self, file, item, media_file_path, url):
        duration_in_seconds = 0
        self.storage_util.populate_local_archive(item["source"], url)
        try:
            duration_in_seconds = self.extract_metadata(media_file_path, url, item)
            self.storage_util.upload_media_and_metadata_to_bucket(item["source"], media_file_path,
                                                                  item["language"])
            self.storage_util.upload_archive_to_bucket(item["source"], item["language"])
            logging.info(str("***File {0} uploaded ***".format(file)))
        except Exception as exception:
            logging.error(exception)
            os.remove(file)
        return duration_in_seconds

    def get_media_requests(self, item, info):
        urls = ItemAdapter(item).get(self.files_urls_field, [])
        if item["source"] not in self.archive_list:
            self.archive_list[item["source"]] = []
        if not os.path.isdir(archives_path.split('/')[0] + '/' + item["source"]):
            self.storage_util.retrieve_archive_from_bucket(item["source"], item["language"])
            self.archive_list[item["source"]] = self.storage_util.retrieve_archive_from_local(item["source"])
        return [Request(u) for u in urls if u not in self.archive_list[item["source"]]]

    def extract_metadata(self, file, url, item):
        file_format = get_file_format(file)
        meta_file_name = file.replace(file_format, "csv")
        media_info, duration_in_seconds = get_media_info(file, item['source'], item['language'], item['source_url'], item['license_urls'], url)
        metadata = self.metadata_creator.create_metadata(media_info)
        metadata_df = pd.DataFrame([metadata])
        metadata_df.to_csv(meta_file_name, index=False)
        return duration_in_seconds

    def is_download_success(self, item):
        return len(item['files']) > 0
