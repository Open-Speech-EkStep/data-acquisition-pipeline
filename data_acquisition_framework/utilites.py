from tinytag import TinyTag


def get_mp3_duration_in_seconds(file):
    tag = TinyTag.get(file)
    return round(tag.duration, 3)
