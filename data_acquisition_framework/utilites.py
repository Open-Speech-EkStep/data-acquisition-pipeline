import logging

from tinytag import TinyTag

from data_acquisition_framework.configs.pipeline_config import *
from data_acquisition_framework.configs.paths import download_path, archives_path
from .gcs_operations import *

ARCHIVE_FILE_NAME = archives_path.split('/')[-1]
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
                'language': video_info['language'] if 'language' in video_info else meta_json['language'],
                'has_other_audio_signature': meta_json['has_other_audio_signature'],
                'type': meta_json['type'],
                'source': video_info['source'] if 'source' in video_info else meta_json['source'],
                'experiment_use': meta_json['experiment_use'],  # check
                'utterances_files_list': meta_json['utterances_files_list'],
                'source_url': video_info['source_url'],
                'speaker_gender': str(meta_json['speaker_gender']).lower() if meta_json['speaker_gender'] else
                video_info['gender'],
                'source_website': video_info['source_website'] if 'source_website' in video_info else meta_json[
                    "source_website"],
                'experiment_name': meta_json['experiment_name'],
                'mother_tongue': meta_json['mother_tongue'],
                'age_group': meta_json['age_group'],  # -----------
                'recorded_state': meta_json['recorded_state'],
                'recorded_district': meta_json['recorded_district'],
                'recorded_place': meta_json['recorded_place'],
                'recorded_date': meta_json['recorded_date'],
                'purpose': meta_json['purpose'],
                'license': video_info["license"] if "license" in video_info else ""}
    return metadata


def create_metadata_for_audio(video_info, yml, item):
    metadata = create_metadata(video_info, yml)
    metadata["source"] = item["source"]
    metadata["language"] = item["language"]
    metadata['source_website'] = item["source_url"]
    return metadata


def set_gcs_creds(gcs_credentials_string):
    gcs_credentials = json.loads(gcs_credentials_string)["Credentials"]
    logging.info("**********Setting Bucket Credentials**********")
    set_gcs_credentials(gcs_credentials)
    logging.info("**********Bucket Credentials Set**********")


def get_archive_file_bucket_path(source, language=""):
    return channel_blob_path.replace("<language>",
                                     language) + '/' + archive_blob_path + '/' + source + '/' + ARCHIVE_FILE_NAME


def retrieve_archive_from_bucket(source, language=""):
    archive_path = archives_path.replace('<source>', source)
    archive_path_parts = archive_path.split('/')
    archive_base_folder = archive_path_parts[0]
    if not os.path.exists(archive_base_folder):
        os.system('mkdir ' + archive_base_folder)
    if not os.path.exists(archive_base_folder + '/' + source + "/"):
        os.system('mkdir {0}/{1}'.format(archive_base_folder, source))
    if check_blob(bucket, get_archive_file_bucket_path(source, language)):
        download_blob(bucket, get_archive_file_bucket_path(source, language),
                      archive_path)
        logging.info(str("Archive file has been downloaded from bucket {0} to local path...".format(bucket)))
        num_downloaded = sum(1 for line in open(archive_path))
        logging.info(str("Count of Previously downloaded files are : {0}".format(num_downloaded)))
    else:
        os.system('touch {0}'.format(archive_path))
        logging.info("No Archive file has been found on bucket...Downloading all files...")


def populate_local_archive(source, url):
    with open(archives_path.replace('<source>', source), 'a+') as f:
        f.write(url + '\n')


def retrieve_archive_from_local(source):
    if os.path.exists(archives_path.replace('<source>', source)):
        with open(archives_path.replace('<source>', source), 'r') as f:
            lines = f.readlines()
        return [line.replace('\n', '') for line in lines]
    else:
        logging.info("No archive.txt is found.....")
        return []


def upload_archive_to_bucket(source, language=""):
    upload_blob(bucket, archives_path.replace('<source>', source), get_archive_file_bucket_path(source, language))


def upload_media_and_metadata_to_bucket(source, file, language=""):
    blob_path = channel_blob_path
    file_format = file.split('.')[-1]
    meta_file_name = file.replace(file_format, "csv")
    file_path = blob_path.replace("<language>", language) + '/' + source + '/' + file.replace(download_path, "")
    upload_blob(bucket, file, file_path)
    os.remove(file)
    meta_path = blob_path.replace("<language>", language) + '/' + source + '/' + meta_file_name.replace(download_path,
                                                                                                        "")
    upload_blob(bucket, meta_file_name, meta_path)
    os.remove(meta_file_name)


def get_mp3_duration_in_seconds(file):
    tag = TinyTag.get(file)
    return round(tag.duration, 3)
