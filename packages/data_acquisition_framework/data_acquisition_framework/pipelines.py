# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from .gcs_operations import *
from .common_utility import *
from .pipeline_config import *
import glob
import os
import subprocess
import moviepy.editor
import pandas as pd

class YTPipeline:
    FILE_FORMAT = 'mp4'
    ARCHIVE_FILE_NAME = 'archive.txt'
    VIDEO_BATCH_FILE_NAME = 'video_list.txt'
    FULL_PLAYLIST_FILE_NAME = "full_playlist.txt"

    def __init__(self):
        self.batch_count = 0
        self.scraped_data = None
        self.yml_config = config_yaml()['downloader']
        self.check_speaker = True

    def scrape(self):
        return self

    def batch(self):
        return self

    def download(self):
        os.system(
            '/app/python/bin/youtube-dl -f bestvideo[ext=mp4] -ciw -o "file-id%(id)s.%(ext)s" -v --batch-file {0}  --restrict-filenames --download-archive {1} --proxy "" --abort-on-error '.format(
                self.VIDEO_BATCH_FILE_NAME, self.ARCHIVE_FILE_NAME))
        return self

    def metadata(self, file):
        video_info = {}
        meta_file_name = file.replace('mp4', 'csv')
        video_id = file.split('file-id')[-1][:-4]
        source_url = ("https://www.youtube.com/watch?v=") + video_id
        video = moviepy.editor.VideoFileClip(file)
        video_duration = int(video.duration) / 60
        video_info['duration'] = video_duration
        video_info['raw_file_name'] = file
        if self.check_speaker:
            video_info['name'] = self.get_speaker(self.scraped_data, video_id)
        else:
            video_info['name'] = None
        video_info['gender'] = None
        video_info['source_url'] = source_url
        metadata = create_metadata(video_info, self.yml_config)
        metadata_df = pd.DataFrame([metadata])
        metadata_df.to_csv(meta_file_name, index=False)
        return self

    def upload(self, file):
        meta_file_name = file.replace("mp4", "csv")
        upload_blob(bucket, file, channel_blob_path + '/' + source_name + '/' + file)
        os.remove(file)
        upload_blob(bucket, meta_file_name, channel_blob_path + '/' + source_name + '/' + meta_file_name)
        os.remove(meta_file_name)
        return self

    # def start(self):
    #     self \
    #         .scrape() \
    #         .batch() \
    #         .download() \
    #         .metadata() \
    #         .upload()

    def check_mode(self):
        if mode == "file":
            if check_blob(bucket, self.get_scraped_file_path()):
                download_blob(bucket, self.get_scraped_file_path(), source_name + ".csv")
                print("Source scraped file has been downloaded from bucket {0} to local path...".format(bucket))
                self.scraped_data = self.create_playlist(source_name + ".csv", file_url_name_column)
                self.check_speaker = True
        elif mode == "channel":
            self.create_channel_playlist(channel_url)
        else:
            raise Exception("Invalid mode")

    def get_archive(self):
        if check_blob(bucket, self.get_archive_file_path()):
            download_blob(bucket, self.get_archive_file_path(), self.ARCHIVE_FILE_NAME)
            print("Archive file has been downloaded from bucket {0} to local path...".format(bucket))
            num_downloaded = sum(1 for line in open(self.ARCHIVE_FILE_NAME))
            print("Count of Previously downloaded files are : ", num_downloaded)
        else:
            os.system('touch {0}'.format(self.ARCHIVE_FILE_NAME))
            print("No Archive file has been found on bucket...Downloading all files...")

    def create_playlist(self, source_file, file_url_name_column):
        df = pd.read_csv(source_file)
        df[file_url_name_column] = df[file_url_name_column].apply(
            lambda x: str(x).replace("https://www.youtube.com/watch?v=", ""))
        df[file_url_name_column].to_csv(self.FULL_PLAYLIST_FILE_NAME, index=False, header=None)
        return df

    def get_video_batch(self):
        os.system(
            "cat {0} {1} | sed 's/youtube //g' | sort |uniq -c |awk '$1==1' | cut -c9- | head -{2} > {3} ".format(
                self.FULL_PLAYLIST_FILE_NAME, self.ARCHIVE_FILE_NAME, batch_num, self.VIDEO_BATCH_FILE_NAME))
        return int(subprocess.check_output(
            "cat {0} | wc -l".format(self.VIDEO_BATCH_FILE_NAME), shell=True).decode("utf-8").split('\n')[
                       0])

    def create_channel_playlist(self, channel_url):
        os.system(
            '/app/python/bin/youtube-dl {0} --flat-playlist --get-id --match-title "{1}" --reject-title "{2}" > {3} '.format(
                channel_url, match_title_string, reject_title_string, self.FULL_PLAYLIST_FILE_NAME))

    def get_playlist_count(self):
        return int(subprocess.check_output(
            "cat {0} | wc -l".format(self.FULL_PLAYLIST_FILE_NAME), shell=True).decode("utf-8").split('\n')[0])

    def get_archive_file_path(self):
        return channel_blob_path + '/' + archive_blob_path + '/' + source_name + '/' + self.ARCHIVE_FILE_NAME

    def get_scraped_file_path(self):
        return channel_blob_path + '/' + scraped_data_blob_path + '/' + source_name + '.csv'

    def get_speaker(self, scraped_data, video_id):
        return scraped_data[scraped_data[file_url_name_column] == video_id].iloc[0][file_speaker_name_column]

    def process_item(self, item, spider):
        print("**********Setting Bucket Credentials**********")
        set_gcs_credentials(item["Gcs_Credentials"])
        print("**********Bucket Credentials Set**********")
        self.check_mode()
        print("*************YOUTUBE DOWNLOAD STARTS*************")
        print("Downloading videos for source : ", source_name)
        try:
            self.get_archive()
        finally:
            playlist_count = self.get_playlist_count()
            print("Total playlist count with valid videos is ", playlist_count)
            last_video_batch_count = self.get_video_batch()
            while last_video_batch_count > 0:
                print("Attempt to download videos with batch size of ", last_video_batch_count)
                try:
                    self.download()
                finally:
                    audio_paths = glob.glob('*.' + self.FILE_FORMAT)
                    audio_files_count = len(audio_paths)
                    if audio_files_count > 0:
                        self.batch_count += audio_files_count
                        print("Uploading {0} files to gcs bucket...".format(audio_files_count))
                        for file in audio_paths:
                            self.metadata(file)
                            self.upload(file)
                        upload_blob(bucket, self.ARCHIVE_FILE_NAME, self.get_archive_file_path())
                        print("Uploaded files till now: ", self.batch_count)
                last_video_batch_count = self.get_video_batch()
            print("Last Batch has no more videos to be downloaded,so finishing downloads...")
            print("Total Uploaded files for this run was : ", self.batch_count)
