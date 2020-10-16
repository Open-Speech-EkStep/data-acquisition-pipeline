import os
import subprocess
import youtube_dl 
import json
from googleapiclient.discovery import build

class YoutubeApi:

    def __init__(self, config):
        self.load_api_key()
        self.youtube = build('youtube', 'v3', developerKey=self.youTubeApiKey)
        self.TYPE = "channel"

        num_pages, num_results = self.calculate_pages(config["max_results"])

        self.MAX_RESULTS = num_results
        self.PAGES = num_pages

        self.REL_LANGUAGE = config["language_code"]
        words_to_include = "|".join(config["keywords"""])
        self.KEYWORDS = ['in', config["language"], words_to_include, '-song']

    def load_api_key(self):
        with open('.env', 'r') as f:
            self.youTubeApiKey = f.read().split("=")[-1]

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
        
    def youtubeExtract(self):
        token = self.getToken()
        results = self.youtube.search().list(part="id,snippet", type=self.TYPE, q=(' ').join(
            self.KEYWORDS), maxResults=self.MAX_RESULTS, relevanceLanguage=self.REL_LANGUAGE, pageToken='').execute()
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
        for i in range(self.PAGES):
            page_channels = self.youtubeExtract()
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
        for playlist in playlist_collection:
            with open(folder+"/"+playlist+".txt", 'w') as f:
                for video_id in video_ids:
                    f.write(video_id+"\n")
            
        
if __name__ == "__main__":
    with open('config.json','r') as f:
        config = json.load(f)
        YoutubeApi(config).generate_playlist_files("playlists")








