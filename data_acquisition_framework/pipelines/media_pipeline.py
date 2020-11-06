import logging
import os
from contextlib import suppress

import moviepy.editor
import pandas as pd
from itemadapter import ItemAdapter
from scrapy import Request
from scrapy.pipelines.files import FilesPipeline

from data_acquisition_framework.utilites import retrieve_archive_from_bucket, retrieve_archive_from_local, config_json, \
    populate_archive, upload_media_and_metadata_to_bucket, upload_archive_to_bucket, get_mp3_duration, create_metadata


class MediaPipeline(FilesPipeline):

    def __init__(self, store_uri, download_func=None, settings=None):
        super().__init__(store_uri, download_func, settings)
        retrieve_archive_from_bucket()
        self.archive_list = retrieve_archive_from_local()
        self.config_json = config_json()['downloader']

    def file_path(self, request, response=None, info=None):
        file_name: str = request.url.split("/")[-1]
        file_name = file_name.replace("%", "_").replace(",", "_")
        return file_name

    def item_completed(self, results, item, info):
        with suppress(KeyError):
            ItemAdapter(item)[self.files_result_field] = [x for ok, x in results if ok]
        if len(item['files']) > 0:
            file_stats = item['files'][0]
            file = file_stats['path']
            url = file_stats['url']
            if os.path.isfile(file):
                logging.info(str("***File {0} downloaded ***".format(file)))
                populate_archive(url)
                self.extract_metadata(file, url)
                upload_media_and_metadata_to_bucket(file)
                upload_archive_to_bucket()
                logging.info(str("***File {0} uploaded ***".format(file)))
        return item

    def get_media_requests(self, item, info):
        urls = ItemAdapter(item).get(self.files_urls_field, [])
        return [Request(u) for u in urls if u not in self.archive_list]

    def extract_metadata(self, file, url):
        video_info = {}
        video_duration = 0
        file_format = file.split('.')[-1]
        meta_file_name = file.replace(file_format, "csv")
        source_url = url
        if file_format == 'mp4':
            video = moviepy.editor.VideoFileClip(file)
            video_duration = int(video.duration) / 60
        elif file_format == 'mp3':
            video_duration = get_mp3_duration(file)
        video_info['duration'] = video_duration
        video_info['raw_file_name'] = file
        video_info['name'] = None
        video_info['gender'] = None
        video_info['source_url'] = source_url
        metadata = create_metadata(video_info, self.config_json)
        metadata_df = pd.DataFrame([metadata])
        metadata_df.to_csv(meta_file_name, index=False)
