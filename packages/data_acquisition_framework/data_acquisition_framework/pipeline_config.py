mode = 'file'  # [channel,file]

# Common configurations
bucket = 'ekstepspeechrecognition-dev'
channel_blob_path = 'data/audiotospeech/raw/download/downloaded/hindi/audio'
archive_blob_path = 'archive'
source_name = 'DD_Kisan'  # Scraped Data file name(CSV)
batch_num = 5  # keep batch small on free tier
scraped_data_blob_path = "scraped_data"

# Channel mode configurations
channel_url = 'https://www.youtube.com/channel/UCnDfmcUyhgJp6xC1LmBLfUg'
match_title_string = ''
reject_title_string = ''

# File Mode configurations
file_speaker_name_column = "num_speaker"
file_url_name_column = "video_url"
