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
                # 'source': yml['source'],  # --------
                'experiment_use': yml['experiment_use'],  # check
                'utterances_files_list': yml['utterances_files_list'],
                'file_url': video_info['file_url'],
                'source_url': video_info['source_url'],
                'speaker_gender': str(yml['speaker_gender']).lower() if yml['speaker_gender'] else video_info['gender'],
                'source_website': yml['source_website'],  # --------
                'experiment_name': yml['experiment_name'],
                'mother_tongue': yml['mother_tongue'],
                'age_group': yml['age_group'],  # -----------
                'recorded_state': yml['recorded_state'],
                'recorded_district': yml['recorded_district'],
                'recorded_place': yml['recorded_place'],
                'recorded_date': yml['recorded_date'],
                'purpose': yml['purpose'],
                'license': video_info["license"] if "license" in video_info else ""
    }
    return metadata

def create_metadata_for_audio(video_info, yml, item):
    metadata = create_metadata(video_info, yml)
    metadata["source"] = item["source"]
    metadata["language"] = item["language"]
    metadata["file_url"] = video_info["file_url"]
    return metadata

def set_gcs_creds(gcs_credentials_string):
    gcs_credentials = json.loads(gcs_credentials_string)["Credentials"]
    logging.info("**********Setting Bucket Credentials**********")
    set_gcs_credentials(gcs_credentials)
    logging.info("**********Bucket Credentials Set**********")


def get_archive_file_path_for_api(source_file):
    source_name=source_file.replace(".txt",'')
    return channel_blob_path + '/' + archive_blob_path + '/' + source_name + '/' + ARCHIVE_FILE_NAME

def get_archive_file_path():
    return channel_blob_path + '/' + archive_blob_path + '/' + source_name + '/' + ARCHIVE_FILE_NAME


def get_archive_file_path_by_source(item):
    return channel_blob_path.replace("<language>",item["language"]) + '/' + archive_blob_path + '/' + item["source"] + '/' + ARCHIVE_FILE_NAME


def retrive_archive_from_bucket():
    if check_blob(bucket, get_archive_file_path()):
        download_blob(bucket, get_archive_file_path(), ARCHIVE_FILE_NAME)
        logging.info(str("Archive file has been downloaded from bucket {0} to local path...".format(bucket)))
        num_downloaded = sum(1 for line in open(ARCHIVE_FILE_NAME))
        logging.info(str("Count of Previously downloaded files are : {0}".format(num_downloaded)))
    else:
        os.system('touch {0}'.format(ARCHIVE_FILE_NAME))
        logging.info("No Archive file has been found on bucket...Downloading all files...")


def retrive_archive_from_bucket_for_api(source_file):
    archive_file_name = 'archive_' + source_file
    if check_blob(bucket, get_archive_file_path_for_api(source_file)):
        download_blob(bucket, get_archive_file_path_for_api(source_file), archive_file_name)
        logging.info(str("Archive file has been downloaded from bucket {0} to local path...".format(bucket)))
        num_downloaded = sum(1 for line in open(archive_file_name))
        logging.info(str("Count of Previously downloaded files are : {0}".format(num_downloaded)))
    else:
        os.system('touch {0}'.format(archive_file_name))



def retrive_archive_from_bucket_by_source(item):
    source = item["source"]
    if check_blob(bucket, get_archive_file_path_by_source(item)):
        if not os.path.exists(source+"/"):
            os.system('mkdir {0}'.format(source))
        download_blob(bucket, get_archive_file_path_by_source(item), source+"/"+ARCHIVE_FILE_NAME)
        logging.info(str("Archive file has been downloaded from bucket {0} to local path...".format(bucket)))
        num_downloaded = sum(1 for line in open(source+"/"+ARCHIVE_FILE_NAME))
        logging.info(str("Count of Previously downloaded files are : {0}".format(num_downloaded)))
    else:
        os.system('mkdir {0}'.format(source))
        os.system('touch {0}'.format(source+"/"+ARCHIVE_FILE_NAME))
        logging.info("No Archive file has been found on bucket...Downloading all files...")


def populate_archive(url):
    with open('archive.txt', 'a+') as f:
        f.write(url + '\n')


def populate_archive_to_source(source, url):
    with open(source+'/archive.txt', 'a+') as f:
        f.write(url + '\n')


def retrieve_archive_from_local():
    if os.path.exists('archive.txt'):
        with open('archive.txt', 'r') as f:
            lines = f.readlines()
        return [line.replace('\n', '') for line in lines]
    else:
        logging.info("No archive.txt is found.....")
        return []


def retrieve_archive_from_local_by_source(source):
    if os.path.exists(source+'/archive.txt'):
        with open(source+'/archive.txt', 'r') as f:
            lines = f.readlines()
        return [line.replace('\n', '') for line in lines]
    else:
        logging.info("No archive.txt is found.....")
        return []


def upload_archive_to_bucket_for_api(source_file):
    upload_blob(bucket, 'archive_' + source_file, get_archive_file_path_for_api(source_file))


def upload_archive_to_bucket():
    upload_blob(bucket, ARCHIVE_FILE_NAME, get_archive_file_path())


def upload_media_and_metadata_to_bucket_for_api(source_file, file):
    FILE_FORMAT = file.split('.')[-1]
    meta_file_name = file.replace(FILE_FORMAT, "csv")
    source_name = source_file.replace('.txt', '')
    upload_blob(bucket, file, channel_blob_path + '/' + source_name + '/' + file)
    os.remove(file)
    upload_blob(bucket, meta_file_name, channel_blob_path + '/' + source_name + '/' + meta_file_name)
    os.remove(meta_file_name)


def upload_archive_to_bucket_by_source(item):
    upload_blob(bucket, item["source"]+"/"+ARCHIVE_FILE_NAME, get_archive_file_path_by_source(item))


def upload_media_and_metadata_to_bucket(file):
    FILE_FORMAT = file.split('.')[-1]
    meta_file_name = file.replace(FILE_FORMAT, "csv")
    upload_blob(bucket, file, channel_blob_path + '/' + source_name + '/' + file)
    os.remove(file)
    upload_blob(bucket, meta_file_name, channel_blob_path + '/' + source_name + '/' + meta_file_name)
    os.remove(meta_file_name)


def upload_audio_and_metadata_to_bucket(file, item):
    FILE_FORMAT = file.split('.')[-1]
    meta_file_name = file.replace(FILE_FORMAT, "csv")
    upload_blob(bucket, file, channel_blob_path
                .replace("<language>", item["language"]) + '/' + item["source"] + '/' + file)
    os.remove(file)
    upload_blob(bucket, meta_file_name, channel_blob_path
                .replace("<language>", item["language"]) + '/' + item["source"] + '/' + meta_file_name)
    os.remove(meta_file_name)


def get_mp3_duration(file):
    tag = TinyTag.get(file)
    return round(tag.duration, 3) / 60


def get_mp3_duration_in_seconds(file):
    tag = TinyTag.get(file)
    return round(tag.duration, 3)
