import logging

from tinytag import TinyTag

from data_acquisition_framework.configs.paths import download_path, archives_path
from data_acquisition_framework.configs.pipeline_config import *
from .gcs_operations import *

ARCHIVE_FILE_NAME = archives_path.split('/')[-1]
logging.basicConfig(level=logging.DEBUG)


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
