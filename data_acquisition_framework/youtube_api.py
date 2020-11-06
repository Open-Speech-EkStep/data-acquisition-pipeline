import os
import subprocess
import json
from googleapiclient.discovery import build


class YoutubeApiBuilder:

    def __init__(self):
        self.load_api_key()
    
    def load_api_key(self):
        self.youtube_api_key = os.environ["youtube_api_key"]
    
    def get_youtube_object(self):
        return build('youtube', 'v3', developerKey=self.youtube_api_key)


class YoutubeApiUtils:

    def __init__(self):
        self.youtube = YoutubeApiBuilder().get_youtube_object()
    
    def get_license_info(self, video_id):
        result = self.youtube.videos().list(part='status', id=video_id).execute()
        license = result['items'][0]['status']['license']
        if license == 'creativeCommon':
            return 'Creative Commons'
        else:
            return 'Standard Youtube'


class YoutubePlaylistCollector:

    def __init__(self):
        print(os.getcwd())
        with open('./data_acquisition_framework/configs/youtube_api_config.json', 'r') as f:
            config = json.load(f)

        self.TYPE = "channel"
        self.youtube = YoutubeApiBuilder().get_youtube_object()

        num_pages, num_results = self.calculate_pages(config["max_results"])

        self.MAX_RESULTS = num_results
        self.PAGES = num_pages

        self.REL_LANGUAGE = config["language_code"]
        words_to_include = "|".join(config["keywords"""])
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
        token = self.get_token()
        results = self.youtube.search().list(part="id,snippet", type=self.TYPE, q=(' ').join(
            self.KEYWORDS), maxResults=self.MAX_RESULTS, relevanceLanguage=self.REL_LANGUAGE, pageToken=token).execute()
        next_token = results['nextPageToken']
        self.set_next_token(next_token)
        page_channels = {}
        for item in results['items']:
            page_channels['https://www.youtube.com/channel/' +
                        item['snippet']['channelId']] = item['snippet']['channelTitle']
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
        for _ in range(self.PAGES):
            page_channels = self.youtube_extract()
            complete_channels.update(page_channels)
        return complete_channels
    
    def get_playlist_collection(self):
        playlist_collection = {}
        urls = self.getUrls()
        for channel, name in urls.items():
            try:
                video_ids = subprocess.check_output('youtube-dl {0} --flat-playlist --get-id'.format(channel), shell=True)
            except:
                continue
            video_ids = video_ids.decode("utf-8").rstrip().lstrip().split("\n")
            name = name.replace(" ","_")
            print(name, len(video_ids))
            playlist_collection[name] = video_ids
        return playlist_collection

    def generate_playlist_files(self, folder):
        playlist_collection = self.get_playlist_collection()
        for playlist, video_ids in playlist_collection.items():
            with open(folder+"/"+playlist+".txt", 'w') as f:
                for video_id in video_ids:
                    f.write(video_id+"\n")
            
        
if __name__ == "__main__":
    with open('configs/youtube_api_config.json', 'r') as f:
        config = json.load(f)
        YoutubePlaylistCollector(config).generate_playlist_files("playlists")
