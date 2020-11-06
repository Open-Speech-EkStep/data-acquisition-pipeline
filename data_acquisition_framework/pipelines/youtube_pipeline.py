from data_acquisition_framework.pipelines.data_acquisition_pipeline import DataAcqusitionPipeline
import glob
from data_acquisition_framework.utilites import *
from data_acquisition_framework.youtube_utilites import *
from data_acquisition_framework.youtube_api import YoutubeApiUtils
import subprocess

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
            .format(self.VIDEO_BATCH_FILE_NAME, self.ARCHIVE_FILE_NAME), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
