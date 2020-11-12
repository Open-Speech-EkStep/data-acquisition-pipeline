import os

from .youtube_api import YoutubePlaylistCollector


def read_playlist_from_file(folder_name):
    if not os.path.exists(folder_name):
        return {}
    data_collection = {}
    for playlist in os.listdir(folder_name):
        playlist_name = playlist.replace(".txt", "")
        with open(folder_name + '/' + playlist) as f:
            video_ids = f.read().splitlines()
            data_collection[playlist_name] = video_ids
    return data_collection


def read_playlist_from_youtube_api(config):
    youtube_playlist_collector = YoutubePlaylistCollector(config)
    return youtube_playlist_collector.__get_channels_with_videos()


def populate_local_archive(source, video_id):
    with open("archive/" + source + "/archive.txt", 'a') as f:
        f.write("youtube " + video_id + "\n")
