from googleapiclient.discovery import build
import os
# Set DEVELOPER_KEY to the "API key" value from the Google Developers Console:
# https://console.developers.google.com/project/_/apiui/credential
# Please ensure that you have enabled the YouTube Data API for your project.

# Youtube API configurations
MAX_RESULTS = 5
REL_LANGUAGE = "gu"
TYPE = "channel"
PAGES = 1
KEYWORDS = ['in', 'gujarati', 'audio|speech|talk', '-song']


def load_api_key():
    return os.environ["youtube_api_key"]


youtube = build('youtube', 'v3', developerKey=load_api_key())


def get_token():
    with open('token.txt', 'r') as file:
        token = file.read()
        return token


def set_next_token(token):
    with open('token.txt', 'w') as file:
        file.write(token)


def youtube_extract():
    token = get_token()
    results = youtube.search().list(part="id,snippet", type=TYPE, q=(' ').join(
        KEYWORDS), maxResults=MAX_RESULTS, relevanceLanguage=REL_LANGUAGE, pageToken=token).execute()
    next_token = results['nextPageToken']
    set_next_token(next_token)
    page_channels = {}
    for item in results['items']:
        page_channels['https://www.youtube.com/channel/' +
                      item['snippet']['channelId']] = item['snippet']['channelTitle']
    return page_channels


def get_urls():
    complete_channels = {}
    for _ in range(PAGES):
        page_channels = youtube_extract()
        complete_channels.update(page_channels)
    return complete_channels


def get_license_info(video_id):
    result = youtube.videos().list(part='status', id=video_id).execute()
    license_value = result['items'][0]['status']['license']
    if license_value == 'creativeCommon':
        return 'Creative Commons'
    else:
        return 'Standard Youtube'
