import json
import logging
import os

from data_acquisition_framework.configs.paths import archives_path, download_path, channels_path, archives_base_path
from data_acquisition_framework.services.loader_util import load_storage_config
from data_acquisition_framework.services.storage.gcs_operations import set_gcs_credentials, upload_blob, download_blob, \
    check_blob


class StorageUtil:

    def __init__(self):
        storage_config = load_storage_config()
        self.channel_blob_path = storage_config['channel_blob_path']
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

    def retrieve_archive_from_bucket(self, source, language=""):
        archive_path = archives_path.replace('<source>', source)
        archive_path_parts = archive_path.split('/')
        archive_base_folder = archive_path_parts[0]
        if not os.path.exists(archive_base_folder):
            os.system('mkdir ' + archive_base_folder)
        if not os.path.exists(archive_base_folder + '/' + source + "/"):
            os.system('mkdir {0}/{1}'.format(archive_base_folder, source))
        if self.check(self.get_archive_file_bucket_path(source, language)):
            self.download(self.get_archive_file_bucket_path(source, language), archive_path)
            logging.info(str("Archive file has been downloaded from bucket {0} to local path...".format(self.bucket)))
            num_downloaded = sum(1 for line in open(archive_path))
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
            self.download(self.token_file_name, self.get_token_path())
        else:
            os.system('echo '' > {0}'.format(self.token_file_name))

    def get_token_from_local(self):
        if os.path.exists(self.token_file_name):
            with open(self.token_file_name, 'r') as file:
                token = file.read()
                return token

    def set_token_in_local(self, token):
        with open(self.token_file_name, 'w') as file:
            file.write(token)

    def get_videos_file_path_in_bucket(self, source_name):
        return self.channel_blob_path + '/' + self.scraped_data_blob_path + '/' + source_name + '.csv'

    @staticmethod
    def clear_required_directories():
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
