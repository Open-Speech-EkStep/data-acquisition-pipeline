import logging

from tinytag import TinyTag

from data_acquisition_framework.configs.pipeline_config import *
from .gcs_operations import *

ARCHIVE_FILE_NAME = 'archive.txt'
logging.basicConfig(level=logging.DEBUG)


def config_json():
    config_path = os.path.dirname(os.path.realpath(__file__))
    logging.info(os.listdir(config_path))
    config_file = os.path.join(config_path, "configs", "config.json")
    with open(config_file, 'r') as file:
        metadata = json.load(file)
    return metadata


def create_metadata(video_info, meta_json):
    metadata = {'raw_file_name': video_info['raw_file_name'],
                'duration': str(video_info['duration']),
                'title': video_info['raw_file_name'],
                'speaker_name': meta_json['speaker_name'] if meta_json['speaker_name'] else video_info['name'],
                'audio_id': meta_json['audio_id'],
                'cleaned_duration': meta_json['cleaned_duration'],
                'num_of_speakers': meta_json['num_of_speakers'],  # check
                'language': meta_json['language'],
                'has_other_audio_signature': meta_json['has_other_audio_signature'],
                'type': meta_json['type'],
                'source': meta_json['source'],  # --------
                'experiment_use': meta_json['experiment_use'],  # check
                'utterances_files_list': meta_json['utterances_files_list'],
                # 'file_url': video_info['file_url'],
                'source_url': video_info['source_url'],
                'speaker_gender': str(meta_json['speaker_gender']).lower() if meta_json['speaker_gender'] else video_info['gender'],
                'source_website': meta_json['source_website'],  # --------
                'experiment_name': meta_json['experiment_name'],
                'mother_tongue': meta_json['mother_tongue'],
                'age_group': meta_json['age_group'],  # -----------
                'recorded_state': meta_json['recorded_state'],
                'recorded_district': meta_json['recorded_district'],
                'recorded_place': meta_json['recorded_place'],
                'recorded_date': meta_json['recorded_date'],
                'purpose': meta_json['purpose'],
                'license': video_info["license"] if "license" in video_info else ""
                }
    return metadata


def create_metadata_for_api(video_info, meta_json):
    metadata = {'raw_file_name': video_info['raw_file_name'],
                'duration': str(video_info['duration']),
                'title': video_info['raw_file_name'],
                'speaker_name': meta_json['speaker_name'] if meta_json['speaker_name'] else video_info['name'],
                'audio_id': meta_json['audio_id'],
                'cleaned_duration': meta_json['cleaned_duration'],
                'num_of_speakers': meta_json['num_of_speakers'],  # check
                'language': meta_json['language'],
                'has_other_audio_signature': meta_json['has_other_audio_signature'],
                'type': meta_json['type'],
                'source': video_info['source'],
                'experiment_use': meta_json['experiment_use'],  # check
                'utterances_files_list': meta_json['utterances_files_list'],
                'source_url': video_info['source_url'],
                'speaker_gender': str(meta_json['speaker_gender']).lower() if meta_json['speaker_gender'] else video_info['gender'],
                'source_website': video_info['source_website'] if 'source_website' in video_info else meta_json["source_website"],
                'experiment_name': meta_json['experiment_name'],
                'mother_tongue': meta_json['mother_tongue'],
                'age_group': meta_json['age_group'],  # -----------
                'recorded_state': meta_json['recorded_state'],
                'recorded_district': meta_json['recorded_district'],
                'recorded_place': meta_json['recorded_place'],
                'recorded_date': meta_json['recorded_date'],
                'purpose': meta_json['purpose'],
                'license': video_info['license']}
    return metadata


def create_metadata_for_audio(video_info, yml, item):
    metadata = create_metadata(video_info, yml)
    metadata["source"] = item["source"]
    metadata["language"] = item["language"]
    metadata['source_website'] = item["source_url"]
    # metadata["file_url"] = video_info["file_url"]
    return metadata


def set_gcs_creds(gcs_credentials_string):
    gcs_credentials = json.loads(gcs_credentials_string)["Credentials"]
    logging.info("**********Setting Bucket Credentials**********")
    set_gcs_credentials(gcs_credentials)
    logging.info("**********Bucket Credentials Set**********")


def get_archive_file_path_for_api(source_file):
    source_name = source_file.replace(".txt", '')
    return channel_blob_path + '/' + archive_blob_path + '/' + source_name + '/' + ARCHIVE_FILE_NAME


def get_archive_file_path():
    return channel_blob_path + '/' + archive_blob_path + '/' + source_name + '/' + ARCHIVE_FILE_NAME


def get_archive_file_path_by_source(item):
    return channel_blob_path.replace("<language>", item["language"]) + '/' + archive_blob_path + '/' + item[
        "source"] + '/' + ARCHIVE_FILE_NAME


def retrieve_archive_from_bucket():
    if check_blob(bucket, get_archive_file_path()):
        download_blob(bucket, get_archive_file_path(), ARCHIVE_FILE_NAME)
        logging.info(str("Archive file has been downloaded from bucket {0} to local path...".format(bucket)))
        num_downloaded = sum(1 for line in open(ARCHIVE_FILE_NAME))
        logging.info(str("Count of Previously downloaded files are : {0}".format(num_downloaded)))
    else:
        os.system('touch {0}'.format(ARCHIVE_FILE_NAME))
        logging.info("No Archive file has been found on bucket...Downloading all files...")


def retrieve_archive_from_bucket_for_api(source_file):
    archive_file_name = 'archive_' + source_file
    if check_blob(bucket, get_archive_file_path_for_api(source_file)):
        download_blob(bucket, get_archive_file_path_for_api(source_file), archive_file_name)
        logging.info(str("Archive file has been downloaded from bucket {0} to local path...".format(bucket)))
        num_downloaded = sum(1 for line in open(archive_file_name))
        logging.info(str("Count of Previously downloaded files are : {0}".format(num_downloaded)))
    else:
        os.system('touch {0}'.format(archive_file_name))


def retrieve_archive_from_bucket_by_source(item):
    source = item["source"]
    if check_blob(bucket, get_archive_file_path_by_source(item)):
        if not os.path.exists("archives"):
            os.system('mkdir archives')
        if not os.path.exists("archives/" + source + "/"):
            os.system('mkdir archives/{0}'.format(source))
        download_blob(bucket, get_archive_file_path_by_source(item), "archives/" + source + "/" + ARCHIVE_FILE_NAME)
        logging.info(str("Archive file has been downloaded from bucket {0} to local path...".format(bucket)))
        num_downloaded = sum(1 for line in open("archives/" + source + "/" + ARCHIVE_FILE_NAME))
        logging.info(str("Count of Previously downloaded files are : {0}".format(num_downloaded)))
    else:
        os.system('mkdir archives')
        os.system('mkdir archives/{0}'.format(source))
        os.system('touch {0}'.format("archives/" + source + "/" + ARCHIVE_FILE_NAME))
        logging.info("No Archive file has been found on bucket...Downloading all files...")


def populate_archive(url):
    with open('archive.txt', 'a+') as f:
        f.write(url + '\n')


def populate_archive_to_source(source, url):
    with open("archives/" + source + '/archive.txt', 'a+') as f:
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
    if os.path.exists("archives/" + source + '/archive.txt'):
        with open("archives/" + source + '/archive.txt', 'r') as f:
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
    upload_blob(bucket, "archives/" + item["source"] + "/" + ARCHIVE_FILE_NAME, get_archive_file_path_by_source(item))


def upload_media_and_metadata_to_bucket(file):
    FILE_FORMAT = file.split('.')[-1]
    meta_file_name = file.replace(FILE_FORMAT, "csv")
    upload_blob(bucket, file, channel_blob_path + '/' + source_name + '/' + file)
    os.remove(file)
    upload_blob(bucket, meta_file_name, channel_blob_path + '/' + source_name + '/' + meta_file_name)
    os.remove(meta_file_name)


def upload_audio_and_metadata_to_bucket(file, item):
    blob_path = channel_blob_path
    FILE_FORMAT = file.split('.')[-1]
    meta_file_name = file.replace(FILE_FORMAT, "csv")
    file_path = blob_path.replace("<language>", item["language"]) + '/' + item["source"] + '/' + file
    print(file_path)
    upload_blob(bucket, file, file_path)
    os.remove(file)
    meta_path = blob_path.replace("<language>", item["language"]) + '/' + item["source"] + '/' + meta_file_name
    print(meta_path)
    upload_blob(bucket, meta_file_name, meta_path)
    os.remove(meta_file_name)


def get_mp3_duration(file):
    tag = TinyTag.get(file)
    return round(tag.duration, 3) / 60


def get_mp3_duration_in_seconds(file):
    tag = TinyTag.get(file)
    return round(tag.duration, 3)
