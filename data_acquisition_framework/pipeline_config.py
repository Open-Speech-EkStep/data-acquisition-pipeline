mode = 'channel'  # [channel,file]

# Common configurations
bucket = 'ekstepspeechrecognition-dev'
channel_blob_path = 'data/audiotospeech/raw/download/downloaded/gujarati/audio'
archive_blob_path = 'archive'
source_name = 'Demo_Source'  # Scraped Data file name(CSV)
batch_num = 2  # keep batch small on free tier
scraped_data_blob_path = "scraped_data"

# Channel mode configurations
channel_url_dict = {}
channel_url = ''
match_title_string = ''
reject_title_string = ''

# File Mode configurations
file_speaker_gender_column = 'speaker_gender'
file_speaker_name_column = "speaker_name"
file_url_name_column = "video_url"
