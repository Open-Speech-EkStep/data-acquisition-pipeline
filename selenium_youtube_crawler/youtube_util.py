import os
import re

import pandas as pd
import tamil
from googleapiclient.discovery import build

from yt_loader_util import load_config_file


class YoutubeApiBuilder:

    def __init__(self):
        self.youtube_api_key = self.load_api_key()

    def load_api_key(self):
        with open('.youtube_api_key', 'r') as f:
            return f.read().rstrip()

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

    def __youtube_call_for_video_ids(self, channel_id, token):
        return self.youtube.search().list(part='id', type='video', channelId=channel_id, videoLicense='any',
                                          maxResults=50,
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
        while True:
            token, result = self.__next_page(token, channel_id)
            for item in result['items']:
                complete_video_ids.append(item['id']['videoId'])
            if token == '':
                break
        return complete_video_ids

    def get_channels(self):
        return self.channel_collector.get_urls()

    def get_playlist_collection(self):
        playlist_collection = {}
        urls = self.get_channels()
        for channel, name in urls.items():
            channel_id = channel.split('/')[-1]
            video_ids = self.get_videos(channel_id)
            name = name.replace(" ", "_")
            print(name, len(video_ids))
            playlist_collection[name] = video_ids
        return playlist_collection

    def generate_playlist_files(self, folder):
        if not os.path.exists(folder):
            os.system('mkdir ' + folder)
        playlist_collection = self.get_playlist_collection()
        for playlist, video_ids in playlist_collection.items():
            with open(folder + "/" + playlist + ".txt", 'w') as f:
                for video_id in video_ids:
                    f.write(video_id + "\n")

    def get_channel_name_and_urls(self):
        urls = self.get_channels()
        pairs = []
        for url, name in urls.items():
            pairs.append((name, url))
        df = pd.DataFrame(pairs, columns=['Channel Name', 'Url'])
        df.to_csv('channels.csv')

    def extract_video_info_for_channels(self):
        urls = self.get_channels()
        for url, name in urls.items():
            channel_id = url.split('/')[-1]
            videos = self.get_videos(channel_id)
            data = []
            for video_id in videos:
                title, licence = self.get_video_info(video_id)
                video_url = "https://www.youtube.com/watch?v=" + video_id
                is_tamil_present = self.is_tamil_present(title)
                data.append([title, video_url, is_tamil_present, licence])
            df = pd.DataFrame(data, columns=['Video Title', 'Video url', 'is_tamil_present', 'is_creative_common'])
            df.to_csv(name.replace(" ", "_") + "_" + channel_id + '.csv')

    def get_video_info(self, video_id):
        data = self.youtube.videos().list(part='snippet, status', id=video_id).execute()
        title = data['items'][0]['snippet']['title']
        licence = data['items'][0]['status']['license']
        return title, licence

    def is_tamil_present(self, text):
        words = tamil.utf8.get_tamil_words(
            tamil.utf8.get_letters(text))
        return "tamil" in text.lower() or len(words) != 0

    def is_kannada_present(self, text):
        for word in text.split():
            result = self.find_kannada(word)
            if result:
                return result
        return "kannada" in text.lower() or False

    def find_kannada(self, word):
        range_start = u'\u0C80'
        range_end = u'\u0CFF'
        pattern = range_start + '-' + range_end
        if re.match('^[' + pattern + ']+$', word) is not None:
            return True
        else:
            return False


class YoutubeChannelCollector:

    def __init__(self, youtube):
        self.youtube = youtube

        self.TYPE = "channel"
        self.MAX_PAGE_RESULT = 50

        config = load_config_file("config.json")
        num_pages, num_results = self.__calculate_pages(config["max_results"])
        self.max_results = num_results
        self.pages = num_pages
        self.rel_language = config["language_code"]
        self.__set_keywords(config)
        self.pages_exhausted = False

    def __set_keywords(self, config):
        words_to_include = "|".join(config["keywords"])
        words_to_ignore = "|".join(["-" + word_to_ignore for word_to_ignore in config["words_to_ignore"]])
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
        token = self.get_token()
        results = self.__youtube_api_call_for_channel_search(token)
        page_channels = {}
        for item in results['items']:
            page_channels['https://www.youtube.com/channel/' +
                          item['snippet']['channelId']] = item['snippet']['channelTitle']
        if 'nextPageToken' in results:
            next_token = results['nextPageToken']
            self.set_next_token(next_token)
        else:
            self.pages_exhausted = True
        return page_channels

    def get_token(self):
        if os.path.exists("token.txt"):
            with open('token.txt', 'r') as file:
                token = file.read()
                return token

    def set_next_token(self, token):
        with open('token.txt', 'w') as file:
            file.write(token)

    def get_urls(self):
        complete_channels = {}
        for _ in range(self.pages):
            if self.pages_exhausted:
                break
            page_channels = self.__get_page_channels()
            complete_channels.update(page_channels)
        return complete_channels


if __name__ == "__main__":
    if not os.path.exists("playlists"):
        os.system("mkdir playlists")
    YoutubeApiUtils().generate_playlist_files('playlists')
