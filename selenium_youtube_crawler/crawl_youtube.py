import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum

# downloads driver for firefox if not present
import geckodriver_autoinstaller
from selenium.common.exceptions import TimeoutException

from browser_utils import BrowserUtils
from downloader import Downloader
from gcs import set_gcs_credentials
from gcs_helper import GCSHelper
from utilities import read_playlist_from_file, read_playlist_from_youtube_api

geckodriver_autoinstaller.install()


# Crawls youtube videos using youtube api and downloads using en.savefrom.net website
class YoutubeCrawler:
    def __init__(
            self, bucket_name, bucket_path, language, playlist_workers=10,download_workers=10
    ):
        self.bucket_path = bucket_path
        self.bucket_name = bucket_name
        current_path = os.path.dirname(os.path.realpath(__file__))
        credentials_path = os.path.join(current_path, "..", "credentials.json")
        set_gcs_credentials(credentials_path)
        self.playlist_executor = ThreadPoolExecutor(max_workers=playlist_workers)
        self.download_executor = ThreadPoolExecutor(max_workers=playlist_workers)
        self.thread_local = threading.local()
        self.language = language

    def start_download(self, download_url, video_id, source):
        downloader = Downloader(self.thread_local, self.bucket_name, self.bucket_path, self.language)
        downloader.download(download_url, video_id, source)

    def initiate_video_id_crawl(self, browser_utils, video_id, playlist_name):
        video_id = video_id.rstrip().lstrip()
        redirect_url = "https://www.ssyoutube.com/watch?v=" + video_id

        browser_utils.get(redirect_url)

        try:
            # wait until result box is rendered by ajax request
            browser_utils.wait_by_class_name("subname")
        except TimeoutException:
            print("Load of {0} failed".format(video_id))
            return

        result_div = browser_utils.find_element_by_id("sf_result")
        a_tags = result_div.find_elements_by_tag_name("a")
        for a_tag in a_tags:
            if a_tag.get_attribute("title") == "video format: 360":
                download_url = a_tag.get_attribute("href")
                print("Starting Download for {0}".format(video_id))

                self.download_executor.submit(
                    self.start_download,
                    download_url,
                    video_id,
                    playlist_name,
                )

                break

    def initiate_playlist_crawl(self, playlists, playlist_name):
        browser_utils = BrowserUtils()

        # download archive here
        archive_video_ids = GCSHelper(
            self.bucket_name, self.bucket_path
        ).validate_and_download_archive(playlist_name)

        video_ids = playlists[playlist_name]

        print("Total Files in {0} is {1}".format(playlist_name, str(len(video_ids))))

        # remove video ids that are in archive
        for _id in archive_video_ids:
            _id = _id.split(" ")[1].rstrip()
            if _id in video_ids:
                video_ids.remove(_id)

        print("Files to be downloaded => %s" % str(len(video_ids)))

        for video_id in video_ids:
            try:
                print(video_id)
                self.initiate_video_id_crawl(browser_utils, video_id, playlist_name)
            except Exception as exc:
                print("%s generated an exception: %s" % (video_id, exc))
                continue

        browser_utils.quit()

    def crawl(self, input_type):
        gcs_helper = GCSHelper(self.bucket_name, self.bucket_path)
        if input_type is CrawlInput.FILE:
            playlists = read_playlist_from_file("playlists")
        else:
            gcs_helper.download_token_from_bucket()
            playlists = read_playlist_from_youtube_api()
        print(playlists)
        future_to_playlist = {
            self.playlist_executor.submit(
                self.initiate_playlist_crawl, playlists, playlist
            ): playlist
            for playlist in playlists
        }

        for future in as_completed(future_to_playlist):
            playlist = future_to_playlist[future]
            try:
                future.result()
            except Exception as exc:
                print("%r generated an exception: %s" % (playlist, exc))

        if input_type is CrawlInput.YOUTUBE_API:
            gcs_helper.upload_token_to_bucket()


class CrawlInput(Enum):
    FILE = "FILE",
    YOUTUBE_API = "YOUTUBE_API"


if __name__ == "__main__":
    with open('config.json', 'r') as f:
        config = json.load(f)
        bucket_name_string = config["bucket_name"]
        bucket_path_string = config["bucket_path"]
        input_type_string = config["input_type"]
    try:
        type_string = CrawlInput[input_type_string.upper()]
        bucket_path_string = bucket_path_string.replace("<language>", config["language"])
        youtube_crawler = YoutubeCrawler(bucket_name_string, bucket_path_string, config["language"])
        youtube_crawler.crawl(type_string)
    except Exception as exception:
        print("generated an exception: %s" % (exception))
        print("Invalid Input type. Should be FILE or YOUTUBE_API")
