import logging
import os
from contextlib import suppress

import moviepy.editor
import pandas as pd
from itemadapter import ItemAdapter
from scrapy import Request
from scrapy.pipelines.files import FilesPipeline

from data_acquisition_framework.utilites import config_json, populate_local_archive, \
    upload_media_and_metadata_to_bucket, upload_archive_to_bucket, retrieve_archive_from_bucket, \
    retrieve_archive_from_local, get_mp3_duration_in_seconds, create_metadata_for_audio


class AudioPipeline(FilesPipeline):

    def __init__(self, store_uri, download_func=None, settings=None):
        super().__init__(store_uri, download_func, settings)
        if not os.path.exists("downloads"):
            os.system("mkdir downloads")
        self.archive_list = {}
        self.config_json = config_json()['downloader']

    def file_path(self, request, response=None, info=None):
        file_name: str = request.url.split("/")[-1]
        file_name = file_name.replace("%", "_").replace(",", "_")
        return file_name

    def item_completed(self, results, item, info):
        duration_in_seconds = 0
        with suppress(KeyError):
            ItemAdapter(item)[self.files_result_field] = [x for ok, x in results if ok]
        if len(item['files']) > 0:
            file_stats = item['files'][0]
            file = file_stats['path']
            url = file_stats['url']
            if os.path.isfile("downloads/"+file):
                logging.info(str("***File {0} downloaded ***".format(file)))
                populate_local_archive(item["source"], url)
                try:
                    duration_in_seconds = self.extract_metadata("downloads/"+file, url, item)
                    upload_media_and_metadata_to_bucket(item["source"], "downloads/"+file, item["language"])
                    upload_archive_to_bucket(item["source"], item["language"])
                    logging.info(str("***File {0} uploaded ***".format(file)))
                except Exception as exception:
                    logging.error(exception)
                    os.remove(file)
            else:
                logging.info(str("***File {0} not downloaded ***".format(item["title"])))
        item["duration"] = duration_in_seconds
        return item

    def get_media_requests(self, item, info):
        urls = ItemAdapter(item).get(self.files_urls_field, [])
        if item["source"] not in self.archive_list:
            self.archive_list[item["source"]] = []
        if not os.path.isdir("archives/" + item["source"]):
            retrieve_archive_from_bucket(item["source"], item["language"])
            self.archive_list[item["source"]] = retrieve_archive_from_local(item["source"])
        return [Request(u) for u in urls if u not in self.archive_list[item["source"]]]

    def get_license_info(self, license_urls):
        for url in license_urls:
            if "creativecommons" in url:
                return "Creative Commons"
        return ', '.join(license_urls)

    def extract_metadata(self, file, url, item):
        video_info = {}
        file_format = file.split('.')[-1]
        meta_file_name = file.replace(file_format, "csv")
        source_url = url
        if file_format == 'mp4':
            video = moviepy.editor.VideoFileClip(file)
            duration_in_seconds = int(video.duration)
        else:
            duration_in_seconds = get_mp3_duration_in_seconds(file)
        video_info['duration'] = duration_in_seconds / 60
        video_info['raw_file_name'] = file.replace("downloads/", "")
        video_info['name'] = None
        video_info['gender'] = None
        video_info['source_url'] = source_url
        # have to rephrase to check if creative commons is present otherwise give comma separated license page links
        video_info['license'] = self.get_license_info(item["license_urls"])
        metadata = create_metadata_for_audio(video_info, self.config_json, item)
        metadata_df = pd.DataFrame([metadata])
        metadata_df.to_csv(meta_file_name, index=False)
        return duration_in_seconds
