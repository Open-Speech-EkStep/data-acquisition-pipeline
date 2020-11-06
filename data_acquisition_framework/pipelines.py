# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


from contextlib import suppress

import moviepy.editor
from itemadapter import ItemAdapter
# useful for handling different item types with a single interface
from scrapy.http import Request
from scrapy.pipelines.files import FilesPipeline

from .utilites import *
from .youtube_utilites import *
from .token_utilities import *


class MediaPipeline(FilesPipeline):

    def __init__(self, store_uri, download_func=None, settings=None):
        super().__init__(store_uri, download_func, settings)
        retrive_archive_from_bucket()
        self.archive_list = retrieve_archive_from_local()
        self.yml_config = config_yaml()['downloader']

    def file_path(self, request, response=None, info=None):
        file_name: str = request.url.split("/")[-1]
        file_name = file_name.replace("%","_").replace(",","_")
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
        FILE_FORMAT = file.split('.')[-1]
        meta_file_name = file.replace(FILE_FORMAT, "csv")
        source_url = url
        if FILE_FORMAT == 'mp4':
            video = moviepy.editor.VideoFileClip(file)
            video_duration = int(video.duration) / 60
        elif FILE_FORMAT == 'mp3':
            video_duration = get_mp3_duration(file)
        video_info['duration'] = video_duration
        video_info['raw_file_name'] = file
        video_info['name'] = None
        video_info['gender'] = None
        video_info['source_url'] = source_url
        metadata = create_metadata(video_info, self.yml_config)
        metadata_df = pd.DataFrame([metadata])
        metadata_df.to_csv(meta_file_name, index=False)

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
        FILE_FORMAT = file.split('.')[-1]
        meta_file_name = file.replace(FILE_FORMAT, "csv")
        source_url = url
        if FILE_FORMAT == 'mp4':
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
