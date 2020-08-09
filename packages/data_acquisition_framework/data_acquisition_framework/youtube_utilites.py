import logging
import subprocess

import pandas as pd

from .gcs_operations import *
from .pipeline_config import *

logging.basicConfig(level=logging.DEBUG)


def get_video_batch(ob):
    os.system(
        "cat {0} {1} | sed 's/youtube //g' | sort |uniq -c |awk '$1==1' | cut -c9- | head -{2} > {3} ".format(
            ob.FULL_PLAYLIST_FILE_NAME, ob.ARCHIVE_FILE_NAME, batch_num, ob.VIDEO_BATCH_FILE_NAME))
    return int(subprocess.check_output(
        "cat {0} | wc -l".format(ob.VIDEO_BATCH_FILE_NAME), shell=True).decode("utf-8").split('\n')[
                   0])


def check_mode(ob):
    if mode == "file":
        if check_blob(bucket, get_scraped_file_path()):
            download_blob(bucket, get_scraped_file_path(), source_name + ".csv")
            logging.info(str("Source scraped file has been downloaded from bucket {0} to local path...".format(bucket)))
            ob.scraped_data = create_playlist(ob, source_name + ".csv", file_url_name_column)
            ob.check_speaker = True
            return ob.check_speaker
        else:
            logging.error(str("{0} File doesn't exists on the given location: {1}".format(source_name + ".csv",
                                                                                      get_scraped_file_path())))
            exit()
    if mode == "channel":
        ob.scrape_links()
        return False
    else:
        logging.error("Invalid mode")
        exit()


def create_playlist(ob, source_file, file_url_name_column):
    df = pd.read_csv(source_file)
    df[file_url_name_column] = df[file_url_name_column].apply(
        lambda x: str(x).replace("https://www.youtube.com/watch?v=", ""))
    df[file_url_name_column].to_csv(ob.FULL_PLAYLIST_FILE_NAME, index=False, header=None)
    return df


def create_channel_playlist(ob, channel_url):
    os.system(
        ob.youtube_call + '{0} --flat-playlist --get-id --match-title "{1}" --reject-title "{2}" > {3} '.format(
            channel_url, match_title_string, reject_title_string, ob.FULL_PLAYLIST_FILE_NAME))


def get_playlist_count(ob):
    return int(subprocess.check_output(
        "cat {0} | wc -l".format(ob.FULL_PLAYLIST_FILE_NAME), shell=True).decode("utf-8").split('\n')[0])


def get_archive_file_path(ob):
    return channel_blob_path + '/' + archive_blob_path + '/' + source_name + '/' + ob.ARCHIVE_FILE_NAME


def get_scraped_file_path():
    return channel_blob_path + '/' + scraped_data_blob_path + '/' + source_name + '.csv'


def get_speaker(scraped_data, video_id):
    return scraped_data[scraped_data[file_url_name_column] == video_id].iloc[0][file_speaker_name_column]


def get_archive(ob):
    if check_blob(bucket, get_archive_file_path(ob)):
        download_blob(bucket, get_archive_file_path(ob), ob.ARCHIVE_FILE_NAME)
        logging.info(str("Archive file has been downloaded from bucket {0} to local path...".format(bucket)))
        num_downloaded = sum(1 for line in open(ob.ARCHIVE_FILE_NAME))
        logging.info(str("Count of Previously downloaded files are : {0}".format(num_downloaded)))
    else:
        os.system('touch {0}'.format(ob.ARCHIVE_FILE_NAME))
        logging.info("No Archive file has been found on bucket...Downloading all files...")


def check_and_log_download_output(ob, downloader_output):
    if downloader_output.stderr:
        formatted_error = str(downloader_output.stderr.decode("utf-8"))
        logging.error(formatted_error)
        if ": YouTube said: Unable to extract video data" in formatted_error:
            video_id = formatted_error.split(":")[1].strip()
            remove_rejected_video(ob, video_id)
            logging.info(str("Video I'd {0} removed from playlist and won't be downloaded".format(video_id)))
        if "HTTP Error 429" in formatted_error:
            logging.error("Too many Requests... \nAborting..... \nPlease Re-Deploy")
            exit()
    else:
        formatted_output = downloader_output.stdout.decode("utf-8").split("\n")
        for _ in formatted_output:
            logging.info(str(_))


def remove_rejected_video(ob, video_id):
    os.system(" sed '/{0}/d' {1}>b.txt && mv b.txt {1}".format(video_id, ob.FULL_PLAYLIST_FILE_NAME))
