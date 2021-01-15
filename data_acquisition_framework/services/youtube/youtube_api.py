import os

from googleapiclient.discovery import build

from data_acquisition_framework.services.loader_util import load_config_file
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

    def __youtube_call_for_video_info(self, video_id):
        return self.youtube.videos().list(part='status', id=video_id).execute()

    def get_license_info(self, video_id):
        result = self.__youtube_call_for_video_info(video_id)
        license_value = result['items'][0]['status']['license']
        if license_value == 'creativeCommon':
            return 'Creative Commons'
        else:
            return 'Standard Youtube'

    def get_cc_video_channels(self):
        return self.channel_collector.get_cc_video_channels()

    def __youtube_call_for_video_ids(self, channel_id, token):
        return self.youtube.search().list(part='id', type='video', channelId=channel_id, videoLicense='any',
                                          maxResults=50,
                                          eventType="completed",
                                          pageToken=token).execute()

    def __next_page(self, token, channel_id):
        res = self.__youtube_call_for_video_ids(channel_id, token)
        if 'nextPageToken' in res.keys():
            next_page_token = res['nextPageToken']
        else:
            next_page_token = ''
        return next_page_token, res

    def get_videos(self, channel_id):
        token = ''
        complete_video_ids = []
        # res = self.__youtube_call_for_video_ids(channel_id, token)
        # for item in res['items']:
        #     complete_video_ids.append(item['id']['videoId'])
        while True:
            token, result = self.__next_page(token, channel_id)
            for item in result['items']:
                complete_video_ids.append(item['id']['videoId'])
            if token == '':
                break
        return complete_video_ids

    def get_channels(self):
        return self.channel_collector.get_urls()


class YoutubeChannelCollector:

    def __init__(self, youtube):
        self.youtube = youtube
        self.storage_util = StorageUtil()

        self.TYPE = "channel"
        self.MAX_PAGE_RESULT = 50

        config = load_config_file("youtube_api_config.json")
        num_pages, num_results = self.__calculate_pages(config["max_results"])
        self.max_results = num_results
        self.pages = num_pages
        self.rel_language = config["language_code"]
        self.__set_keywords(config)
        self.pages_exhausted = False

    def __set_keywords(self, config):
        words_to_include = "|".join(config["keywords"])
        words_to_ignore = " ".join(["-" + word_to_ignore for word_to_ignore in config["words_to_ignore"]])
        self.keywords = ['in', config["language"], words_to_include, words_to_ignore]

    def __calculate_pages(self, max_results):
        if max_results <= self.MAX_PAGE_RESULT:
            num_results = max_results
            num_pages = 1
        else:
            num_pages = int(max_results // self.MAX_PAGE_RESULT)
            num_results = self.MAX_PAGE_RESULT
            if not max_results % self.MAX_PAGE_RESULT == 0:
                num_pages += 1
        return num_pages, num_results

    def __youtube_api_call_for_channel_search(self, token):
        return self.youtube.search().list(part="id,snippet", type=self.TYPE, q=' '.join(
            self.keywords), maxResults=self.max_results, relevanceLanguage=self.rel_language, pageToken=token).execute()

    def __get_page_channels(self):
        token = self.storage_util.get_token_from_local()
        results = self.__youtube_api_call_for_channel_search(token)
        page_channels = {}
        for item in results['items']:
            title = item['snippet']['channelTitle']
            title = title.replace("'", "") \
                .replace(" ", "_") \
                .replace(',', '_') \
                .replace('/', '_') \
                .replace('\\', '_') \
                .replace('.', '_') \
                .replace('$', '_')
            page_channels['https://www.youtube.com/channel/' +
                          item['snippet']['channelId']] = title
        if 'nextPageToken' in results:
            next_token = results['nextPageToken']
            self.storage_util.set_token_in_local(next_token)
        else:
            self.pages_exhausted = True
        return page_channels

    def get_urls(self):
        complete_channels = {}
        for _ in range(self.pages):
            if self.pages_exhausted:
                break
            page_channels = self.__get_page_channels()
            complete_channels.update(page_channels)
        return complete_channels

    def youtube_api_call_for_cc_video_search(self, token):
        return self.youtube.search().list(part="id,snippet", type='video', q=' '.join(
            self.keywords), maxResults=self.max_results, relevanceLanguage=self.rel_language,
                                          videoLicense='creativeCommon',
                                          pageToken=token).execute()

    def __get_page_cc_videos(self, token):
        results = self.youtube_api_call_for_cc_video_search(token)
        page_channels = {}
        next_token = None
        for item in results['items']:
            title = item['snippet']['channelTitle']
            title = title.replace("'", "") \
                .replace(" ", "_") \
                .replace(',', '_') \
                .replace('/', '_') \
                .replace('\\', '_') \
                .replace('.', '_') \
                .replace('$', '_')
            channel_id = item['snippet']['channelId']
            if channel_id == 'UCmyKnNRH0wH-r8I-ceP-dsg' or channel_id =='UCcTKQnC3lRA4aira95_a1pw':
                continue
            page_channels['https://www.youtube.com/channel/' +
                          channel_id] = title
        if 'nextPageToken' in results:
            next_token = results['nextPageToken']
            self.storage_util.set_token_in_local(next_token)
        else:
            self.pages_exhausted = True
        return page_channels, next_token

    def get_cc_video_channels(self):
        complete_channels = {}
        token = self.storage_util.get_token_from_local()
        for _ in range(self.pages):
            if self.pages_exhausted:
                break
            page_channels, token = self.__get_page_cc_videos(token)
            complete_channels.update(page_channels)
        return complete_channels


if __name__ == '__main__':
    os.environ["youtube_api_key"] = '<api key here>'
    youtube = YoutubeApiBuilder().get_youtube_object()
    channel_collector = YoutubeChannelCollector(youtube)
    result = channel_collector.get_cc_video_channels()
    print(result)
