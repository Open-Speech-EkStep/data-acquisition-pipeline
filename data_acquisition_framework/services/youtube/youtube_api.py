import json
import os

from googleapiclient.discovery import build

from data_acquisition_framework.services.storage_util import StorageUtil


class YoutubeApiBuilder:

    def __init__(self):
        self.youtube_api_key = os.environ["youtube_api_key"]

    def get_youtube_object(self):
        return build('youtube', 'v3', developerKey=self.youtube_api_key, cache_discovery=False)


class YoutubeApiUtils:

    def __init__(self):
        self.youtube = YoutubeApiBuilder().get_youtube_object()
        self.channel_collector = YoutubeChannelCollector(self.youtube)

    def get_license_info(self, video_id):
        result = self.youtube.videos().list(part='status', id=video_id).execute()
        license_value = result['items'][0]['status']['license']
        if license_value == 'creativeCommon':
            return 'Creative Commons'
        else:
            return 'Standard Youtube'

    def __next_page(self, token, channel_id):
        res = self.youtube.search().list(part='id', type='video', channelId=channel_id, videoLicense='any', maxResults=50,
                                         pageToken=token).execute()
        if 'nextPageToken' in res.keys():
            next_page_token = res['nextPageToken']
        else:
            next_page_token = ''
        return next_page_token, res

    def get_videos(self, channel_id):
        token = ''
        complete_video_ids = []
        while True:
            token, result = self.__next_page(token, channel_id)
            for item in result['items']:
                complete_video_ids.append(item['id']['videoId'])
            if token == '':
                break
        return complete_video_ids

    def __get_channels_with_videos(self):
        channel_collection = {}
        channels = self.get_channels()
        for channel_url, channel_name in channels.items():
            video_ids = self.get_videos(channel_url.split('/')[-1])
            channel_name = channel_name.replace(" ", "_")
            channel_collection[channel_name] = video_ids
        return channel_collection

    def generate_channel_files(self, folder):
        channel_collection = self.__get_channels_with_videos()
        for channel_name, video_ids in channel_collection.items():
            with open(folder + "/" + channel_name + ".txt", 'w') as f:
                for video_id in video_ids:
                    f.write(video_id + "\n")

    def get_channels(self):
        return self.channel_collector.get_urls()


class YoutubeChannelCollector:

    def __init__(self, youtube):
        self.storage_util = StorageUtil()
        current_path = os.path.dirname(os.path.realpath(__file__))
        api_config_file = os.path.join(current_path, '..', '..', "configs", "youtube_api_config.json")
        with open(api_config_file, 'r') as f:
            config = json.load(f)

        self.TYPE = "channel"
        self.youtube = youtube
        self.MAX_PAGE_RESULT = 50
        self.TOKEN_FILE_NAME = 'token.txt'
        num_pages, num_results = self.calculate_pages(config["max_results"])

        self.MAX_RESULTS = num_results
        self.PAGES = num_pages

        self.REL_LANGUAGE = config["language_code"]
        words_to_include = "|".join(config["keywords"""])
        self.KEYWORDS = ['in', config["language"], words_to_include, '-song']

    def calculate_pages(self, max_results):
        if max_results <= self.MAX_PAGE_RESULT:
            num_results = max_results
            num_pages = 1
        else:
            num_pages = int(max_results // self.MAX_PAGE_RESULT)
            num_results = self.MAX_PAGE_RESULT
            if not max_results % self.MAX_PAGE_RESULT == 0:
                num_pages += 1
        return num_pages, num_results

    def __get_page_channels(self):
        token = self.storage_util.get_token_from_local()
        results = self.youtube.search().list(part="id,snippet", type=self.TYPE, q=' '.join(
            self.KEYWORDS), maxResults=self.MAX_RESULTS, relevanceLanguage=self.REL_LANGUAGE, pageToken=token).execute()
        next_token = results['nextPageToken']
        self.storage_util.set_token_in_local(next_token)
        page_channels = {}
        for item in results['items']:
            page_channels['https://www.youtube.com/channel/' +
                          item['snippet']['channelId']] = item['snippet']['channelTitle']
        return page_channels

    def get_urls(self):
        complete_channels = {}
        for _ in range(self.PAGES):
            page_channels = self.__get_page_channels()
            complete_channels.update(page_channels)
        return complete_channels
