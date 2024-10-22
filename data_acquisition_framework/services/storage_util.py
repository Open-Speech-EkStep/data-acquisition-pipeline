import glob
import json
import logging
import os

from data_acquisition_framework.configs.paths import archives_path, download_path, channels_path, archives_base_path
from data_acquisition_framework.services.loader_util import load_config_file
from data_acquisition_framework.services.storage.gcs_operations import set_gcs_credentials, upload_blob, download_blob, \
    check_blob


class StorageUtil:

    def __init__(self):
        storage_config = load_config_file("storage_config.json")
        self.channel_blob_path = storage_config['channel_blob_path']
        self.channels_file_blob_path = storage_config['channels_file_blob_path']
        self.bucket = storage_config['bucket']
        self.archive_blob_path = storage_config['archive_blob_path']
        self.scraped_data_blob_path = storage_config['scraped_data_blob_path']
        self.archive_file_name = "archive.txt"
        self.token_file_name = 'token.txt'

    def set_gcs_creds(self, gcs_credentials_string):
        gcs_credentials = json.loads(gcs_credentials_string)["Credentials"]
        logging.info("**********Setting Bucket Credentials**********")
        set_gcs_credentials(gcs_credentials)
        logging.info("**********Bucket Credentials Set**********")

    def upload(self, file_to_upload, location_to_upload):
        upload_blob(self.bucket, file_to_upload, location_to_upload)

    def download(self, file_to_download, download_location):
        download_blob(self.bucket, file_to_download, download_location)

    def check(self, file_to_check):
        return check_blob(self.bucket, file_to_check)

    def get_archive_file_bucket_path(self, source, language=""):
        return self.channel_blob_path.replace('<language>',
                                              language) + '/' + self.archive_blob_path + '/' + source + '/' + self.archive_file_name

    def get_channel_file_upload_path(self, source, language=""):
        path = self.channels_file_blob_path + '/' + source + '/videos_list.txt'
        return self.channel_blob_path.replace('<language>',
                                              language) + '/' + path

    def retrieve_archive_from_bucket(self, source, language=""):
        archive_path = archives_path.replace('<source>', source)
        if not os.path.exists(archives_base_path):
            os.system('mkdir ' + archives_base_path)
        if not os.path.exists(archives_base_path + source + "/"):
            os.system('mkdir {0}/{1}'.format(archives_base_path, source))
        if self.check(self.get_archive_file_bucket_path(source, language)):
            self.download(self.get_archive_file_bucket_path(source, language), archive_path)
            logging.info(str("Archive file has been downloaded from bucket {0} to local path...".format(self.bucket)))
            with open(archive_path, 'r') as f:
                num_downloaded = len(f.read().splitlines())
            logging.info(str("Count of Previously downloaded files are : {0}".format(num_downloaded)))
        else:
            os.system('touch {0}'.format(archive_path))
            logging.info("No Archive file has been found on bucket...Downloading all files...")

    def populate_local_archive(self, source, url):
        with open(archives_path.replace('<source>', source), 'a+') as f:
            f.write(url + '\n')

    def retrieve_archive_from_local(self, source):
        if os.path.exists(archives_path.replace('<source>', source)):
            with open(archives_path.replace('<source>', source), 'r') as f:
                lines = f.readlines()
            return [line.replace('\n', '') for line in lines]
        else:
            logging.info("No archive.txt is found.....")
            return []

    def upload_archive_to_bucket(self, source, language=""):
        archive_path = archives_path.replace('<source>', source)
        archive_bucket_path = self.get_archive_file_bucket_path(source, language)
        self.upload(archive_path, archive_bucket_path)

    def upload_media_and_metadata_to_bucket(self, source, media_filename, language=""):
        blob_path = self.channel_blob_path
        file_format = media_filename.split('.')[-1]
        meta_file_name = media_filename.replace(file_format, "csv")
        file_path = blob_path.replace("<language>", language) + '/' + source + '/' + media_filename.replace(
            download_path, "")
        self.upload(media_filename, file_path)
        os.remove(media_filename)
        meta_path = blob_path.replace("<language>", language) + '/' + source + '/' + meta_file_name.replace(
            download_path,
            "")
        self.upload(meta_file_name, meta_path)
        os.remove(meta_file_name)

    def upload_license(self, media_file_path, source, language=""):
        blob_path = self.channel_blob_path
        file_path = blob_path.replace("<language>",
                                      language) + '/' + source + '/' + 'license/' + media_file_path.replace(
            download_path, "")
        self.upload(media_file_path, file_path)
        os.remove(media_file_path)

    def get_token_path(self):
        return self.channel_blob_path + '/' + self.token_file_name

    def upload_token_to_bucket(self):
        if os.path.exists(self.token_file_name):
            self.upload(self.token_file_name, self.get_token_path())

    def get_token_from_bucket(self):
        if self.check(self.get_token_path()):
            self.download(self.get_token_path(), self.token_file_name)
        else:
            os.system('echo '' > {0}'.format(self.token_file_name))

    def get_token_from_local(self):
        if os.path.exists(self.token_file_name):
            with open(self.token_file_name, 'r') as file:
                token = file.read()
                return token.rstrip().lstrip()
        return ""

    def set_token_in_local(self, token):
        with open(self.token_file_name, 'w') as file:
            file.write(token)

    def get_videos_file_path_in_bucket(self, source_name):
        return self.channel_blob_path + '/' + self.scraped_data_blob_path + '/' + source_name + '.csv'

    def clear_required_directories(self):
        if os.path.exists(download_path):
            os.system('rm -rf ' + download_path)
        else:
            os.system("mkdir " + download_path)
        if os.path.exists(channels_path):
            os.system('rm -rf ' + channels_path)
        if os.path.exists(archives_base_path):
            os.system('rm -rf ' + archives_base_path)

    def write_license_to_local(self, file_name, content):
        path = download_path + file_name
        with open(path, 'w') as f:
            f.write(content)

    def get_channel_videos_count(self, file_name):
        file_path = channels_path + file_name
        with open(file_path, 'r') as f:
            count = len(f.read().splitlines())
        return count

    def get_media_paths(self):
        return glob.glob(download_path + '*.mp4')

    def get_videos_of_channel(self, id_name_join):
        channel_base_path = self.get_channel_file_upload_path(id_name_join)
        if self.check(channel_base_path):
            local_path = channels_path + id_name_join + ".txt"
            self.download(channel_base_path, local_path)
            return True
        else:
            return False
