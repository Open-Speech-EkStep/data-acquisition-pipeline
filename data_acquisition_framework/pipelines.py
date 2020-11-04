# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


import glob
from contextlib import suppress

import moviepy.editor
from itemadapter import ItemAdapter
# useful for handling different item types with a single interface
from scrapy.http import Request
from scrapy.pipelines.files import FilesPipeline

from .data_acquisition_pipeline import DataAcqusitionPipeline
from .utilites import *
from .youtube_utilites import *
from .youtube_api import YoutubePlaylistCollector, YoutubeApiUtils
from .token_utilities import *
from concurrent.futures.thread import ThreadPoolExecutor


class YoutubePipeline(DataAcqusitionPipeline):
    FILE_FORMAT = 'mp4'
    ARCHIVE_FILE_NAME = 'archive.txt'
    VIDEO_BATCH_FILE_NAME = 'video_list.txt'
    FULL_PLAYLIST_FILE_NAME = "full_playlist.txt"

    def __init__(self):
        logging.info("*************YOUTUBE DOWNLOAD STARTS*************")
        logging.info(str("Downloading videos for source : {0}".format(source_name)))

        self.youtube_api_utils = YoutubeApiUtils()
        self.batch_count = 0
        self.scraped_data = None
        self.yml_config = config_yaml()['downloader']
        self.check_speaker = False
        self.youtube_call = "/app/python/bin/youtube-dl " if "scrapinghub" in os.path.abspath("~") else "youtube-dl "
        retrive_archive_from_bucket()

    def scrape_links(self):
        create_channel_playlist(self, channel_url)
        return self

    def create_download_batch(self):
        return get_video_batch(self)

    def download_files(self):
        downloader_output = subprocess.run(
            self.youtube_call + '-f "best[ext=mp4]" -o "%(duration)sfile-id%(id)s.%(ext)s" --batch-file {0}  '
                                '--restrict-filenames --download-archive {1} --proxy "" --abort-on-error '
            .format(self.VIDEO_BATCH_FILE_NAME, self.ARCHIVE_FILE_NAME), shell=True, capture_output=True)
        check_and_log_download_output(self, downloader_output)
        return self

    def extract_metadata(self, file, url=None):
        video_info = {}
        file_format = file.split('.')[-1]
        meta_file_name = file.replace(file_format, "csv")
        video_id = file.split('file-id')[-1][:-4]
        source_url = "https://www.youtube.com/watch?v=" + video_id
        # video = moviepy.editor.VideoFileClip(file)
        video_duration = int(file.split('file-id')[0]) / 60
        video_info['duration'] = video_duration
        video_info['raw_file_name'] = file
        if self.check_speaker:
            video_info['name'] = get_speaker(self.scraped_data, video_id)
        else:
            video_info['name'] = None
        if self.check_speaker:
            video_info['gender'] = get_gender(self.scraped_data, video_id)
        else:
            video_info['gender'] = None
        video_info['source_url'] = source_url
        video_info['license'] = self.youtube_api_utils.get_license_info(video_id)
        metadata = create_metadata(video_info, self.yml_config)
        metadata_df = pd.DataFrame([metadata])
        metadata_df.to_csv(meta_file_name, index=False)
        return self

    def process_item(self, item, spider):
        self.check_speaker = True if check_mode(self) else False
        playlist_count = get_playlist_count(self.FULL_PLAYLIST_FILE_NAME)
        logging.info(str("Total playlist count with valid videos is {0}".format(playlist_count)))
        last_video_batch_count = self.create_download_batch()
        while last_video_batch_count > 0:
            logging.info(str("Attempt to download videos with batch size of {0}".format(last_video_batch_count)))
            try:
                self.download_files()
            finally:
                audio_paths = glob.glob('*.' + self.FILE_FORMAT)
                audio_files_count = len(audio_paths)
                if audio_files_count > 0:
                    self.batch_count += audio_files_count
                    logging.info(str("Uploading {0} files to gcs bucket...".format(audio_files_count)))
                    for file in audio_paths:
                        self.extract_metadata(file)
                        upload_media_and_metadata_to_bucket(file)
                    upload_archive_to_bucket()
                    logging.info(str("Uploaded files till now: {0}".format(self.batch_count)))
            last_video_batch_count = self.create_download_batch()
        logging.info("Last Batch has no more videos to be downloaded,so finishing downloads...")
        logging.info(str("Total Uploaded files for this run was : {0}".format(self.batch_count)))
        return item


class YoutubeApiPipeline(DataAcqusitionPipeline):
    FILE_FORMAT = 'mp4'
    ARCHIVE_FILE_NAME = 'archive.txt'
    VIDEO_BATCH_FILE_NAME = 'video_list.txt'
    FULL_PLAYLIST_FILE_NAME = "full_playlist.txt"
    PLAYLIST_PATH = 'playlist'

    def __init__(self):
        logging.info("*************YOUTUBE DOWNLOAD STARTS*************")
        logging.info(str("Downloading videos for source : {0}".format(source_name)))

        self.youtube_api_utils = YoutubeApiUtils()
        self.batch_count = 0
        self.scraped_data = None
        self.yml_config = config_yaml()['downloader']
        self.check_speaker = False
        self.youtube_call = "/app/python/bin/youtube-dl " if "scrapinghub" in os.path.abspath("~") else "youtube-dl "
        self.source_channel_dict = None
        self.source_file = None
        self.t_duration = 0

    def scrape_links(self):
        if len(channel_url_dict) != 0:
            self.source_channel_dict = channel_url_dict
        else:
            get_token_from_bucket()
            self.source_channel_dict = YoutubePlaylistCollector().get_urls()
        create_channel_playlist_for_api(self)
        return self

    def create_download_batch(self):
        return get_video_batch(self)

    def youtube_download(self, video_id, file):
        command = self.youtube_call + '-f "best[ext=mp4][filesize<1024M]" -o "%(duration)sfile-id%(id)s.%(ext)s" ' \
                                      '"https://www.youtube.com/watch?v={0}" --download-archive {1} --proxy "" ' \
                                      '--abort-on-error'.format(video_id, 'archive_' + file)
        downloader_output = subprocess.run(
            command, shell=True, capture_output=True)
        check_and_log_download_output(self, downloader_output)

    def download_files(self, file):
        with ThreadPoolExecutor(max_workers=1) as executor:
            with open(self.VIDEO_BATCH_FILE_NAME, 'r') as f:
                for video_id in f.readlines():
                    executor.submit(self.youtube_download, video_id, file)
        return self

    def extract_metadata(self, source_file, file, url=None):
        video_info = {}
        file_format = file.split('.')[-1]
        meta_file_name = file.replace(file_format, "csv")
        video_id = file.split('file-id')[-1][:-4]
        source_url = "https://www.youtube.com/watch?v=" + video_id
        video_duration = int(file.split('file-id')[0]) / 60
        video_info['duration'] = video_duration
        self.t_duration += video_duration
        logging.info('$$$$$$$    ' + str(self.t_duration // 60) + '   $$$$$$$')
        video_info['source'] = source_file.replace('.txt', '')
        video_info['raw_file_name'] = file
        if self.check_speaker:
            video_info['name'] = get_speaker(self.scraped_data, video_id)
        else:
            video_info['name'] = None
        if self.check_speaker:
            video_info['gender'] = get_gender(self.scraped_data, video_id)
        else:
            video_info['gender'] = None
        video_info['source_url'] = source_url
        video_info['source_website'] = read_website_url(video_info['source'])
        video_info['license'] = self.youtube_api_utils.get_license_info(video_id)
        metadata = create_metadata_for_api(video_info, self.yml_config)
        metadata_df = pd.DataFrame([metadata])
        metadata_df.to_csv(meta_file_name, index=False)
        return self

    def process_item(self, item, spider):
        self.check_speaker = True if check_mode(self) else False
        for source_file in glob.glob(self.PLAYLIST_PATH + '/*.txt'):
            source_file_name = source_file.replace('playlist/', '')
            logging.info(
                str("Channel {0}".format(source_file_name)))

            self.source_file = source_file_name
            self.batch_count = 0
            retrive_archive_from_bucket_for_api(source_file_name)
            playlist_count = get_playlist_count(source_file)
            logging.info(
                str("Total playlist count with valid videos is {0}".format(playlist_count)))

            last_video_batch_count = self.create_download_batch()
            while last_video_batch_count > 0:
                logging.info(str("Attempt to download videos with batch size of {0}".format(
                    last_video_batch_count)))
                try:
                    self.download_files(source_file_name)
                except Exception as e:
                    print("error", e)
                finally:
                    audio_paths = glob.glob('*.' + self.FILE_FORMAT)
                    audio_files_count = len(audio_paths)
                    if audio_files_count > 0:
                        self.batch_count += audio_files_count
                        logging.info(
                            str("Uploading {0} files to gcs bucket...".format(audio_files_count)))
                        for file in audio_paths:
                            self.extract_metadata(source_file_name, file)
                            upload_media_and_metadata_to_bucket_for_api(source_file_name, file)
                        upload_archive_to_bucket_for_api(source_file_name)
                        logging.info(
                            str("Uploaded files till now: {0}".format(self.batch_count)))
                last_video_batch_count = self.create_download_batch()
            logging.info(str("Last Batch has no more videos to be downloaded,so finishing downloads..."))
            logging.info(
                str("Total Uploaded files for this run was : {0}".format(self.batch_count)))
        update_token_in_bucket()
        return item


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
