from enum import Enum


class YoutubeService(Enum):
    YOUTUBE_DL = "YOUTUBE_DL",
    YOUTUBE_API = "YOUTUBE_API"


mode = 'channel'  # [channel,file]

# Common configurations
source_name = 'kannada_videos_cc_3'  # Scraped Data file name(CSV)
batch_num = 1  # keep batch small on free tier
youtube_service_to_use = YoutubeService.YOUTUBE_DL

# "https://www.youtube.com/channel/UC2XEzs5R1mn2wTKgtjuMxiQ": "sadhgurukannada_non_cc",
# Channel mode configurations
channel_url_dict = {
    "https://www.youtube.com/channel/UCPMDhcBogBPsrGQWf5qnArg": "Tech in Kannada",
    "https://www.youtube.com/channel/UClNEDHT_Zo9OJJp2v512vCg": "Knowledge_is_Spherical",
    "https://www.youtube.com/channel/UCst7dIH10mMvu_PVpXMr7Hw": "Unbox_karnataka",
    "https://www.youtube.com/channel/UCZFpWf46GbSQULyE-6T1U4g": "Director_Satishkumar_-_Kannada"
}

# File Mode configurations
file_speaker_gender_column = 'speaker_gender'
file_speaker_name_column = "speaker_name"
file_url_name_column = "video_url"
license_column = "license"
