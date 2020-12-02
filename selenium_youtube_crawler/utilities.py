import os

from youtube_util import YoutubeApiUtils


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


def read_playlist_from_youtube_api():
    youtube_api_utils = YoutubeApiUtils()
    return youtube_api_utils.get_channels()


def populate_local_archive(source, video_id):
    with open("archive/" + source + "/archive.txt", 'a') as f:
        f.write("youtube " + video_id + "\n")


def create_required_dirs_for_archive_if_not_present(source):
    local_archive_source_folder = "archive/" + source
    if not os.path.exists("archive"):
        os.system("mkdir archive")
    if not os.path.exists(local_archive_source_folder):
        os.system("mkdir {0}".format(local_archive_source_folder))
