# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


import glob

import moviepy.editor

from .data_acquisition_pipeline import DataAcqusitionPipeline
from .utilites import *
from .youtube_utilites import *


class YoutubePipeline(DataAcqusitionPipeline):
    FILE_FORMAT = 'mp4'
    ARCHIVE_FILE_NAME = 'archive.txt'
    VIDEO_BATCH_FILE_NAME = 'video_list.txt'
    FULL_PLAYLIST_FILE_NAME = "full_playlist.txt"

    def __init__(self):
        self.batch_count = 0
        self.scraped_data = None
        self.yml_config = config_yaml()['downloader']
        self.check_speaker = False
        self.youtube_call = "/app/python/bin/youtube-dl " if "scrapinghub" in os.path.abspath("~") else "youtube-dl "

    def scrape_links(self):
        create_channel_playlist(self, channel_url)
        return self

    def create_download_batch(self):
        return get_video_batch(self)

    def download_files(self):
        downloader_output = subprocess.run(
            self.youtube_call + '-f bestvideo[ext=mp4] -ciw -o "file-id%(id)s.%(ext)s" --batch-file {0}  --restrict-filenames --download-archive {1} --proxy "" --abort-on-error '.format(
                self.VIDEO_BATCH_FILE_NAME, self.ARCHIVE_FILE_NAME), shell=True, capture_output=True)
        check_and_log_download_output(self, downloader_output)
        return self

    def extract_metadata(self, file):
        video_info = {}
        meta_file_name = file.replace('mp4', 'csv')
        video_id = file.split('file-id')[-1][:-4]
        source_url = ("https://www.youtube.com/watch?v=") + video_id
        video = moviepy.editor.VideoFileClip(file)
        video_duration = int(video.duration) / 60
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

    def upload_to_bucket(self, file):
        meta_file_name = file.replace("mp4", "csv")
        upload_blob(bucket, file, channel_blob_path + '/' + source_name + '/' + file)
        os.remove(file)
        upload_blob(bucket, meta_file_name, channel_blob_path + '/' + source_name + '/' + meta_file_name)
        os.remove(meta_file_name)
        return self

    # def start(self):
    #     self \
    #         .scrape_links() \
    #         .create_download_batch() \
    #         .download_files() \
    #         .extract_metadata() \
    #         .upload_to_bucket()

    def process_item(self, item, spider):
        logging.info("**********Setting Bucket Credentials**********")
        set_gcs_credentials(item["Gcs_Credentials"])
        logging.info("**********Bucket Credentials Set**********")
        self.check_speaker = True if check_mode(self) else False
        logging.info("*************YOUTUBE DOWNLOAD STARTS*************")
        logging.info(str("Downloading videos for source : {0}".format(source_name)))
        try:
            get_archive(self)
        finally:
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
                            self.upload_to_bucket(file)
                        upload_blob(bucket, self.ARCHIVE_FILE_NAME, get_archive_file_path(self))
                        logging.info(str("Uploaded files till now: {0}".format(self.batch_count)))
                last_video_batch_count = self.create_download_batch()
            logging.info("Last Batch has no more videos to be downloaded,so finishing downloads...")
            logging.info(str("Total Uploaded files for this run was : {0}".format(self.batch_count)))
