from enum import Enum


class YoutubeService(Enum):
    YOUTUBE_DL = "YOUTUBE_DL",
    YOUTUBE_API = "YOUTUBE_API"


mode = 'channel'  # [channel,file]
only_creative_commons = True

# Common configurations
source_name = 'kannada_videos_cc_3'  # Scraped Data file name(CSV)
batch_num = 5  # keep batch small on free tier
youtube_service_to_use = YoutubeService.YOUTUBE_DL

# "https://www.youtube.com/channel/UC2XEzs5R1mn2wTKgtjuMxiQ": "sadhgurukannada_non_cc",
# Channel mode configurations
channel_url_dict = {}

# File Mode configurations
file_speaker_gender_column = 'speaker_gender'
file_speaker_name_column = "speaker_name"
file_url_name_column = "video_url"
license_column = "license"
