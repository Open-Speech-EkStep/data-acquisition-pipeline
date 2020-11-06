import logging
import subprocess

import pandas as pd
from pandas.io.common import EmptyDataError

from .gcs_operations import *
from data_acquisition_framework.configs.pipeline_config import *
from data_acquisition_framework.configs.paths import archives_path, playlist_path

logging.basicConfig(level=logging.DEBUG)


def get_video_batch(ob):
    source = ob.source_without_channel_id.replace('.txt', '')
    playlist_file_name = playlist_path + ob.source_file
    archive_file_name = archives_path.replace('<source>', source)
    try:
        full_playlist = pd.read_csv(playlist_file_name, header=None)
    except EmptyDataError:
        return 0
    try:
        archive_file = pd.read_csv(archive_file_name, delimiter=' ', header=None, encoding='utf-8')[1]
    except EmptyDataError:
        archive_file = pd.DataFrame(columns=[1])
    video_batch = full_playlist[full_playlist.merge(archive_file, left_on=0, right_on=1, how='left')[1].isnull()].head(
        batch_num)
    video_batch.to_csv(ob.VIDEO_BATCH_FILE_NAME, header=False, index=False)
    return int(video_batch.count()[0])


def check_mode(ob):
    if mode == "file":
        if check_blob(bucket, get_videos_file_path_in_bucket()):
            download_blob(bucket, get_videos_file_path_in_bucket(), source_name + ".csv")
            logging.info(str("Source scraped file has been downloaded from bucket {0} to local path...".format(bucket)))
            ob.scraped_data = create_playlist_for_file_mode(ob, source_name + ".csv", file_url_name_column)
            ob.check_speaker = True
            return ob.check_speaker
        else:
            logging.error(str("{0} File doesn't exists on the given location: {1}".format(source_name + ".csv",
                                                                                          get_videos_file_path_in_bucket())))
            exit()
    if mode == "channel":
        ob.scrape_links()
        return False
    else:
        logging.error("Invalid mode")
        exit()


def check_dataframe_validity(df):
    if file_url_name_column not in df.columns:
        logging.error("Url column entered wrong.")
        exit()
    if file_speaker_name_column not in df.columns:
        logging.error("Speaker name column entered wrong.")
        exit()
    if file_speaker_gender_column not in df.columns:
        logging.error("Speaker gender column entered wrong.")
        exit()


def create_playlist_for_file_mode(ob, source_file, file_url_name_column):
    df = pd.read_csv(source_file)
    check_dataframe_validity(df)
    df = df[df[file_url_name_column].notna()]
    df[file_url_name_column] = df[file_url_name_column].apply(
        lambda x: str(x).replace("https://www.youtube.com/watch?v=", ""))
    df[file_url_name_column] = df[file_url_name_column].apply(lambda x: str(x).replace("https://youtu.be/", ""))
    if not os.path.exists(playlist_path):
        os.system("mkdir " + playlist_path)
    df[file_url_name_column].to_csv(playlist_path + source_file.replace(".csv", ".txt"), index=False, header=None)
    return df


def create_channel_playlist(ob):
    if not (os.path.exists(playlist_path)):
        os.mkdir(playlist_path)

    for channel_url in ob.source_channel_dict.keys():
        channel_id = channel_url.split('/')[-1]
        ob.source_channel_dict[channel_url] = str(ob.source_channel_dict[channel_url]).replace(' ', '_')
        source_playlist_file = playlist_path + channel_id + '__' + ob.source_channel_dict[channel_url] + '.txt'

        os.system(
            ob.youtube_call + '{0} --flat-playlist --get-id --match-title "{1}" --reject-title "{2}" > {3} '.format(
                channel_url, match_title_string, reject_title_string, source_playlist_file))


def get_playlist_count(file):
    return int(subprocess.check_output(
        "cat {0} | wc -l".format(file), shell=True).decode("utf-8").split('\n')[0])


def get_videos_file_path_in_bucket():
    return channel_blob_path + '/' + scraped_data_blob_path + '/' + source_name + '.csv'


def get_speaker(scraped_data, video_id):
    return scraped_data[scraped_data[file_url_name_column] == video_id].iloc[0][file_speaker_name_column]


def get_gender(scraped_data, video_id):
    return str(scraped_data[scraped_data[file_url_name_column] == video_id].iloc[0][file_speaker_gender_column]).lower()


def check_and_log_download_output(source, downloader_output):
    if downloader_output.stderr:
        formatted_error = str(downloader_output.stderr.decode("utf-8"))
        check = False
        if not ("WARNING" in formatted_error):
            logging.error(formatted_error)
        if ": YouTube said: Unable to extract video data" in formatted_error:
            video_id = formatted_error.split(":")[1].strip()
            remove_rejected_video(source, video_id)
            check = True
            logging.info(str("Video I'd {0} removed from playlist and won't be downloaded".format(video_id)))
        if "Did not get any data blocks" in formatted_error or "HTTP Error 404: Not Found" in formatted_error:
            video_id = open("video_list.txt").readlines()[0].replace("\n", "")
            remove_rejected_video(source, video_id)
            check = True
            logging.info(str("ERROR Handeled"))
        if "HTTP Error 429" in formatted_error:
            logging.error("Too many Requests... \nAborting..... \nPlease Re-Deploy")
            exit()
        if len(formatted_error) > 5 and check == False:
            video_id = open("video_list.txt").readlines()[0].replace("\n", "")
            remove_rejected_video(source, video_id)
            logging.info(str("ERROR Handeled"))
    formatted_output = downloader_output.stdout.decode("utf-8").split("\n")
    for _ in formatted_output:
        logging.info(str(_))


def remove_rejected_video(source, video_id):
    os.system(" sed '/{0}/d' {1}>b.txt && mv b.txt {1}".format(video_id, playlist_path + source))
