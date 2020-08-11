# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from io import BytesIO
from scrapy.utils.project import get_project_settings
from scrapy.http import Request
from .utilites import *
from .youtube_utilites import *
from .pipeline_config import *
from .data_acquisition_pipeline import DataAcqusitionPipeline
import glob
import os
import moviepy.editor
import audioread
import pandas as pd
from contextlib import suppress
from itemadapter import ItemAdapter
from scrapy.pipelines.files import FilesPipeline


class YoutubePipeline(DataAcqusitionPipeline):
    FILE_FORMAT = 'mp4'
    ARCHIVE_FILE_NAME = 'archive.txt'
    VIDEO_BATCH_FILE_NAME = 'video_list.txt'
    FULL_PLAYLIST_FILE_NAME = "full_playlist.txt"

    def __init__(self):
        logging.info("*************YOUTUBE DOWNLOAD STARTS*************")
        logging.info(str("Downloading videos for source : {0}".format(source_name)))

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
            self.youtube_call + '-f "best[ext=mp4]" -o "%(duration)sfile-id%(id)s.%(ext)s" --batch-file {0}  --restrict-filenames --download-archive {1} --proxy "" --abort-on-error '.format(
                self.VIDEO_BATCH_FILE_NAME, self.ARCHIVE_FILE_NAME), shell=True, capture_output=True)
        check_and_log_download_output(self, downloader_output)
        return self

    def extract_metadata(self, file, url=None):
        video_info = {}
        FILE_FORMAT = file.split('.')[-1]
        meta_file_name = file.replace(FILE_FORMAT, "csv")
        video_id = file.split('file-id')[-1][:-4]
        source_url = ("https://www.youtube.com/watch?v=") + video_id
        # video = moviepy.editor.VideoFileClip(file)
        video_duration = int(file.split('file-id')[0]) / 60
        video_info['duration'] = video_duration
        video_info['raw_file_name'] = file
        if self.check_speaker:
            video_info['name'] = get_speaker(self.scraped_data, video_id)
        else:
            video_info['name'] = None
        video_info['gender'] = None
        video_info['source_url'] = source_url
        metadata = create_metadata(video_info, self.yml_config)
        metadata_df = pd.DataFrame([metadata])
        metadata_df.to_csv(meta_file_name, index=False)
        return self

    # def start(self):
    #     self \
    #         .scrape_links() \
    #         .create_download_batch() \
    #         .download_files() \
    #         .extract_metadata() \
    #         .upload_to_bucket()

    def process_item(self, item, spider):
        self.check_speaker = True if check_mode(self) else False
        playlist_count = get_playlist_count(self)
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


# class GCSFilesStoreJSON(GCSFilesStore):
#     CREDENTIALS = {
#         "type": "service_account",
#         "project_id": "ekstepspeechrecognition",
#         "private_key_id": "76e44673ef49ac12b7cb2aa094920e6df876bcab",
#         "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQC7joxDM8uVFf+L\nMbdMANKvhff2QQEiwfWxhfnXrNq/s44d/fR/dbAX35ajTrpOXJFIEuY29gTADzrd\nMpixn+lMOWAaHeWM2AkUKswHcoVvsAqvAhSZUahg5PaGcWiIWJP3PyNPca0Qx6+o\nrnse2S7Omhv1vEloAxhUWtxIgi6WJfR8KwmLhqVH6mUGkqVJf0l9wzQUVc+jDaFI\nzOwKl+qOlHzt9ynLqmqrBF3C5RozAfVTqdqusahD463hVKQy+LOKE3I5iWRUQUcE\nonrqMFrOILF0NHNvqBglPJ9iRqjc/m93j/5KniyikNzedXHJ1NSbn07oDdzis9hh\n23tsiD7fAgMBAAECggEARQGbAKS/bBBmb5+wmXGaEsNfKobbNJ8ZVyH8fQpXh324\nNbe4q+awjfARO++c43TybQqrEiCtOb7AwR67CGtWCln3zlQen5XirT1byQetKZ0j\nKSXCT3C4W0ISo/943uV8N1VPGA0yiEB4FD9yBDUTICeaTuziMzckTfEKKFFhc5NN\nJKK3jyjTuz8UiLtK3KTRzExNs3QfibwhIdV/eDOAGc+VT61ec4Fdh+ejYC3vK+Mn\nO68Jnnah/TuyaBLVMW3YsyXQg+oVsPxBP6T7UzF6sbbBkcg6/U/URv2/5Vc1k/T4\n6cFInJIgLtHQvxRLILtMNAx9agQ1PVubl6fHD44zTQKBgQDcK6QFJzKqo8kZ5IqW\nGaLAwZlLeT9VmJ6Z950bPW2a/sqH0XFTjNPVV7FFKEm/DfJY4S5IyzKo9zGsMPEL\n8Zc9zOz4dOkIwNzGZ2ycCkvrTn8b7wRRQAsiAWM0/A4MRj4v5bRLmZSg4wKmTMti\nRxdXqVG02ZQJmoZjRbk/xM3lnQKBgQDaFDbbhKWn8gBvv0/kbDorpRDIorZs+8Ud\n/A8g1xI3xBHiuxWT5tugPfo+395D7BT6CBZ+aBk2qwu2baH7bNG/iXjuGa7Ay+mH\nmIVHsCUX+eFITvbeT8t9Ncv/P03ER0RbFL8eZW/n8i1KcIy9ZN7N+e9EBb3/wAiL\nA9wMNyCrqwKBgQCPdOgEa4v534pTErSyJLYFPp/xq2j3DuCYldyKOTZHfajdYjyj\nIemM4vyggSW8FQxJmT+dMrkpmxeEiMcm7x2KqRHmudZ1W6T+qbj820Coa5cqzkxT\n3JTkbV8E0Q8eNE6kytj1QXa0dfXuAa+rs4KkHbEdU3+/2i2iVXXk9QjriQKBgQCS\nVigti8hBd0HVurHYnMs4CE7H42+4mAXAxig8qDVgWGCMHXAwTCSqVYx77mtOdrfo\nw86cSixJI+P7KXwdo/rnpU8Rrwg19V8ijzU4UrnBaftDM0GzEiaBQb0+7XK4t/3l\nhHlu4zCBm1/K6NV4LZzY6NMmeRfy6yCQcCmTxNZWewKBgQDCa1XMJs6i534ye2TC\nQ/skkl6X3HMJrJ6B3pg4zon6FV/C6E/1+PumAK69aXDAtSN4lbd2/wUmGZOazqWc\nKMXa2+KTBId4YvrYoKyYGufpNecaGU+hn2hDNNWPM07U7zX94tX6ATaiMJFUs6ns\nLgLG84ts++dTaMFip9Rh8uc44g==\n-----END PRIVATE KEY-----\n",
#         "client_email": "servacct-speechrecognition-crw@ekstepspeechrecognition.iam.gserviceaccount.com",
#         "client_id": "115727090172355048355",
#         "auth_uri": "https://accounts.google.com/o/oauth2/auth",
#         "token_uri": "https://oauth2.googleapis.com/token",
#         "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
#         "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/servacct-speechrecognition-crw%40ekstepspeechrecognition.iam.gserviceaccount.com"
#     }
#
#     def __init__(self, uri):
#         from google.cloud import storage
#         client = storage.Client.from_service_account_info(self.CREDENTIALS)
#         bucket, prefix = uri[5:].split('/', 1)
#         self.bucket = client.bucket(bucket)
#         self.prefix = prefix
#


class MediaPipeline(FilesPipeline):

    # def __init__(self, store_uri, download_func=None, settings=None):
    #     super(GCSFilePipeline, self).__init__(store_uri, download_func, settings)

    # def process_item(self, item, spider):
    #     # print("**********Setting Bucket Credentials**********")
    #     # set_gcs_credentials(item["Gcs_Credentials"])
    #     # print("**********Bucket Credentials Set**********")
    #     print("Inside process item")
    #     return item

    def __init__(self, store_uri, download_func=None, settings=None):
        super().__init__(store_uri, download_func, settings)
        retrive_archive_from_bucket()
        self.archive_list = retrieve_archive_from_local()
        self.yml_config = config_yaml()['downloader']

    def file_path(self, request, response=None, info=None):
        file_name: str = request.url.split("/")[-1]
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
        FILE_FORMAT = file.split('.')[-1]
        meta_file_name = file.replace(FILE_FORMAT, "csv")
        source_url = url
        if FILE_FORMAT == 'mp4':
            video = moviepy.editor.VideoFileClip(file)
            video_duration = int(video.duration) / 60
        elif FILE_FORMAT == 'mp3':
            video_duration = get_mp3_duration(file)  # TODO To use a third party module
        video_info['duration'] = video_duration
        video_info['raw_file_name'] = file
        video_info['name'] = None
        video_info['gender'] = None
        video_info['source_url'] = source_url
        metadata = create_metadata(video_info, self.yml_config)
        metadata_df = pd.DataFrame([metadata])
        metadata_df.to_csv(meta_file_name, index=False)
