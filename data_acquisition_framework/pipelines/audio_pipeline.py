import logging
import os
from contextlib import suppress

import moviepy.editor
import pandas as pd
from itemadapter import ItemAdapter
from scrapy import Request
from scrapy.pipelines.files import FilesPipeline

from data_acquisition_framework.utilites import config_yaml, populate_archive_to_source, \
    upload_audio_and_metadata_to_bucket, upload_archive_to_bucket_by_source, retrive_archive_from_bucket_by_source, \
    retrieve_archive_from_local_by_source, get_mp3_duration_in_seconds, create_metadata_for_audio


class AudioPipeline(FilesPipeline):

    def __init__(self, store_uri, download_func=None, settings=None):
        super().__init__(store_uri, download_func, settings)
        self.archive_list = {}
        self.yml_config = config_yaml()['downloader']

    def file_path(self, request, response=None, info=None):
        file_name: str = request.url.split("/")[-1]
        file_name = file_name.replace("%","_").replace(",","_")
        return file_name

    def item_completed(self, results, item, info):
        duration = 0
        with suppress(KeyError):
            ItemAdapter(item)[self.files_result_field] = [x for ok, x in results if ok]
        if len(item['files']) > 0:
            file_stats = item['files'][0]
            file = file_stats['path']
            url = file_stats['url']
            if os.path.isfile(file):
                logging.info(str("***File {0} downloaded ***".format(file)))
                populate_archive_to_source(item["source"],url)
                try:
                    duration = self.extract_metadata(file, url, item)
                    upload_audio_and_metadata_to_bucket(file, item)
                    upload_archive_to_bucket_by_source(item)
                    logging.info(str("***File {0} uploaded ***".format(file)))
                except Exception as exception:
                    logging.error(exception)
                    os.remove(file)
            else:
                logging.info(str("***File {0} not downloaded ***".format(item["title"])))
        item["duration"] = duration
        return item

    def get_media_requests(self, item, info):
        urls = ItemAdapter(item).get(self.files_urls_field, [])
        if item["source"] not in self.archive_list:
            self.archive_list[item["source"]] = []
        if not os.path.isdir("archives/"+item["source"]):
            retrive_archive_from_bucket_by_source(item)
            self.archive_list[item["source"]] = retrieve_archive_from_local_by_source(item["source"])
        return [Request(u) for u in urls if u not in self.archive_list[item["source"]]]

    def get_license_info(self, license_urls):
        for url in license_urls:
            if "creativecommons" in url:
                return "Creative Commons"
        return ', '.join(license_urls)

    def extract_metadata(self, file, url, item):
        video_info = {}
        duration_in_seconds = 0
        file_format = file.split('.')[-1]
        meta_file_name = file.replace(file_format, "csv")
        source_url = url
        if file_format == 'mp4':
            video = moviepy.editor.VideoFileClip(file)
            duration_in_seconds = int(video.duration)
        else:
            duration_in_seconds = get_mp3_duration_in_seconds(file)
        video_info['duration'] = duration_in_seconds / 60
        video_info['raw_file_name'] = file
        video_info['name'] = None
        video_info['gender'] = None
        video_info['source_url'] = source_url
        # have to rephrase to check if creative commons is present otherwise give comma separated license page links
        video_info['license'] = self.get_license_info(item["license_urls"])
        metadata = create_metadata_for_audio(video_info, self.yml_config, item)
        metadata_df = pd.DataFrame([metadata])
        metadata_df.to_csv(meta_file_name, index=False)
        item["duration"] = duration_in_seconds
        return duration_in_seconds