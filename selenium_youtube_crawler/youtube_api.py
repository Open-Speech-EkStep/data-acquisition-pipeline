import json
import os

import pandas as pd
from googleapiclient.discovery import build


class YoutubeApiBuilder:

    def __init__(self):
        self.youtube_api_key = self.load_api_key()

    def load_api_key(self):
        with open('.youtube_api_key', 'r') as f:
            return f.read()

    def get_youtube_object(self):
        return build('youtube', 'v3', developerKey=self.youtube_api_key, cache_discovery=False)


class YoutubeApiUtils:

    def __init__(self):
        self.youtube = YoutubeApiBuilder().get_youtube_object()

    def get_license_info(self, video_id):
        result = self.youtube.videos().list(part='status', id=video_id).execute()
        license_info = result['items'][0]['status']['license']
        if license_info == 'creativeCommon':
            return 'Creative Commons'
        else:
            return 'Standard Youtube'


class YoutubePlaylistCollector:

    def __init__(self, config):

        self.TYPE = "channel"
        self.youtube = YoutubeApiBuilder().get_youtube_object()

        num_pages, num_results = self.calculate_pages(config["max_results"])

        self.MAX_RESULTS = num_results
        self.PAGES = num_pages

        self.REL_LANGUAGE = config["language_code"]
        words_to_include = "|".join(config["keywords"])
        self.KEYWORDS = ['in', config["language"], words_to_include, '-song']

    def calculate_pages(self, max_results):
        if max_results <= 50:
            num_results = max_results
            num_pages = 1
        else:
            num_pages = int(max_results // 50)
            num_results = 50
            if not max_results % 50 == 0:
                num_pages += 1
        return num_pages, num_results

    def youtube_extract(self):
        token = self.getToken()
        results = self.youtube.search().list(part="id,snippet", type=self.TYPE, q=(' ').join(
            self.KEYWORDS), maxResults=self.MAX_RESULTS, relevanceLanguage=self.REL_LANGUAGE, pageToken=token).execute()
        nextToken = results['nextPageToken']
        self.setNextToken(nextToken)
        page_channels = {}
        for item in results['items']:
            page_channels['https://www.youtube.com/channel/' +
                          item['snippet']['channelId']] = item['snippet']['channelTitle']
        return page_channels

    def getToken(self):
        if os.path.exists("token.txt"):
            with open('token.txt', 'r') as file:
                token = file.read()
                return token

    def setNextToken(self, token):
        with open('token.txt', 'w') as file:
            file.write(token)

    def getUrls(self):
        complete_channels = {}
        for _ in range(self.PAGES):
            try:
                page_channels = self.youtube_extract()
            except:
                break
            complete_channels.update(page_channels)
        return complete_channels

    def get_playlist_collection(self):
        playlist_collection = {}
        urls = self.getUrls()
        for channel, name in urls.items():
            channel_id = channel.split('/')[-1]
            video_ids = self.get_videos(channel_id)
            name = name.replace(" ", "_")
            print(name, len(video_ids))
            playlist_collection[name] = video_ids
        return playlist_collection

    def generate_playlist_files(self, folder):
        playlist_collection = self.get_playlist_collection()
        for playlist, video_ids in playlist_collection.items():
            with open(folder + "/" + playlist + ".txt", 'w') as f:
                for video_id in video_ids:
                    f.write(video_id + "\n")

    def get_channel_name_and_urls(self):
        playlist_collection = {}
        urls = self.getUrls()
        pairs = []
        for url, name in urls.items():
            pairs.append((name, url))
        df = pd.DataFrame(pairs, columns=['Channel Name', 'Url'])
        df.to_csv('channels1.csv')

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


if __name__ == "__main__":
    with open('config.json', 'r') as f:
        config = json.load(f)
        # YoutubePlaylistCollector(config).get_channel_name_and_urls()
        YoutubePlaylistCollector(config).generate_playlist_files("playlists")
