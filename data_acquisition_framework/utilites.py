import logging

import yaml
from tinytag import TinyTag
import shutil
from .gcs_operations import *
from .pipeline_config import *

ARCHIVE_FILE_NAME = 'archive.txt'
logging.basicConfig(level=logging.DEBUG)


def config_yaml():
    config_path = os.path.dirname(os.path.realpath(__file__))
    logging.info(os.listdir(config_path))
    config_file = config_path + '/config.py'
    config_yaml_file = config_file.replace('.py', '.yaml')
    shutil.copyfile(config_file, config_yaml_file)
    with open(config_yaml_file) as file:
        yml = yaml.load(file, Loader=yaml.FullLoader)
    return yml


def create_metadata(video_info, yml):
    metadata = {'raw_file_name': video_info['raw_file_name'],
                'duration': str(video_info['duration']),
                'title': video_info['raw_file_name'],
                'speaker_name': yml['speaker_name'] if yml['speaker_name'] else video_info['name'],
                'audio_id': yml['audio_id'],
                'cleaned_duration': yml['cleaned_duration'],
                'num_of_speakers': yml['num_of_speakers'],  # check
                'language': yml['language'],
                'has_other_audio_signature': yml['has_other_audio_signature'],
                'type': yml['type'],
                'source': yml['source'],  # --------
                'experiment_use': yml['experiment_use'],  # check
                'utterances_files_list': yml['utterances_files_list'],
                'source_url': video_info['source_url'],
                'speaker_gender': yml['speaker_gender'] if yml['speaker_gender'] else video_info['gender'],
                'source_website': yml['source_website'],  # --------
                'experiment_name': yml['experiment_name'],
                'mother_tongue': yml['mother_tongue'],
                'age_group': yml['age_group'],  # -----------
                'recorded_state': yml['recorded_state'],
                'recorded_district': yml['recorded_district'],
                'recorded_place': yml['recorded_place'],
                'recorded_date': yml['recorded_date'],
                'purpose': yml['purpose']}
    return metadata


def set_gcs_creds(gcs_credentials_string):
    gcs_credentials = json.loads(gcs_credentials_string)["Credentials"]
    logging.info("**********Setting Bucket Credentials**********")
    set_gcs_credentials(gcs_credentials)
    logging.info("**********Bucket Credentials Set**********")


def get_archive_file_path():
    return channel_blob_path + '/' + archive_blob_path + '/' + source_name + '/' + ARCHIVE_FILE_NAME


def retrive_archive_from_bucket():
    if check_blob(bucket, get_archive_file_path()):
        download_blob(bucket, get_archive_file_path(), ARCHIVE_FILE_NAME)
        logging.info(str("Archive file has been downloaded from bucket {0} to local path...".format(bucket)))
        num_downloaded = sum(1 for line in open(ARCHIVE_FILE_NAME))
        logging.info(str("Count of Previously downloaded files are : {0}".format(num_downloaded)))
    else:
        os.system('touch {0}'.format(ARCHIVE_FILE_NAME))
        logging.info("No Archive file has been found on bucket...Downloading all files...")


def populate_archive(url):
    with open('archive.txt', 'a+') as f:
        f.write(url + '\n')


def retrieve_archive_from_local():
    if os.path.exists('archive.txt'):
        with open('archive.txt', 'r') as f:
            lines = f.readlines()
        return [line.replace('\n', '') for line in lines]
    else:
        logging.info("No archive.txt is found.....")
        return []


def upload_archive_to_bucket():
    upload_blob(bucket, ARCHIVE_FILE_NAME, get_archive_file_path())


def upload_media_and_metadata_to_bucket(file):
    FILE_FORMAT = file.split('.')[-1]
    meta_file_name = file.replace(FILE_FORMAT, "csv")
    upload_blob(bucket, file, channel_blob_path + '/' + source_name + '/' + file)
    os.remove(file)
    upload_blob(bucket, meta_file_name, channel_blob_path + '/' + source_name + '/' + meta_file_name)
    os.remove(meta_file_name)


def get_mp3_duration(file):
    tag = TinyTag.get(file)
    return round(tag.duration, 3) / 60
