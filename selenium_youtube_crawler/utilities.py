import os
from youtube_api import YoutubeApi
from gcs import upload_blob, download_blob, check_blob
import requests
import threading

def read_playlist_from_file(folder_name):
    if not os.path.exists(folder_name):
        return {}
    data_collection = {}
    for playlist in os.listdir(folder_name):
        playlist_name = playlist.replace(".txt","")
        with open(folder_name+'/'+playlist) as f:
            video_ids = f.read().splitlines()
            data_collection[playlist_name] = video_ids
    return data_collection

def read_playlist_from_youtube_api(config):
    youtube_api = YoutubeApi(config)
    return youtube_api.get_playlist_collection()

def populate_local_archive(source, video_id):        
    with open("archive/"+source+"/archive.txt", 'a') as f:
        f.write("youtube "+video_id+"\n")


