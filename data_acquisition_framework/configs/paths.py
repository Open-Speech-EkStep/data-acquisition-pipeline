import os


def get_path(value):
    current_path = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(current_path, '..', "..", value)


archives_base_path = get_path("archives/")
download_path = get_path("downloads/")
archives_path = get_path(archives_base_path + "<source>/archive.txt")
channels_path = get_path("channels/")
