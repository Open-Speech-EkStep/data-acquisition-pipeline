
mode = 'file'  # [channel,file]


#Common configurations
bucket = 'ekstepspeechrecognition-dev'
channel_blob_path = 'scrapydump'
archive_blob_path = 'archive'
source_name = 'Test'  #Scraped Data file name(CSV)
batch_num = 5 # keep batch small on free tier
scraped_data_blob_path = "scraped_data"

#Channel mode configurations
channel_url = 'https://www.youtube.com/channel/UCXxvQMEypJB2s4V4-0dP_uw'
match_title_string = ''
reject_title_string = ''


#File Mode configurations
file_speaker_name_column = "speaker_name"
file_url_name_column = "video_url"
