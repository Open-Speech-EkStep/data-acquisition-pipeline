import os

from gcs import upload_blob, download_blob, check_blob
from utilities import create_required_dirs_for_archive_if_not_present


class GCSHelper:
    def __init__(self, bucket_name, bucket_path):
        self.bucket_name = bucket_name
        self.bucket_path = bucket_path

    def upload_archive_to_bucket(self, source):
        archive_path = "archive/" + source + "/archive.txt"
        upload_blob(
            self.bucket_name,
            archive_path,
            self.bucket_path + "/" + archive_path,
        )

    def upload_token_to_bucket(self):
        token_file_name = 'token.txt'
        upload_blob(self.bucket_name, '%s' % token_file_name, self.bucket_path + '/' + token_file_name)

    def download_token_from_bucket(self):
        token_file_name = "token.txt"
        token_path = self.bucket_path + "/" + token_file_name
        if check_blob(self.bucket_name, token_path):
            download_blob(self.bucket_name, token_path, token_file_name)
            print(
                str(
                    "Token file has been downloaded from bucket {0} to local path...".format(
                        self.bucket_name
                    )
                )
            )
        else:
            if not os.path.exists(token_file_name):
                os.system("touch {0}".format(token_file_name))
            print("No token file has been found on bucket...")

    def validate_and_download_archive(self, source):
        archive_file_path = self.bucket_path + "/archive/" + source + "/archive.txt"
        local_archive_source = "archive/{0}".format(source)
        local_archive_file_path = "{0}/archive.txt".format(local_archive_source)
        create_required_dirs_for_archive_if_not_present(source)

        if check_blob(self.bucket_name, archive_file_path):
            return self.download_archive_from_bucket(archive_file_path, local_archive_file_path)
        else:
            return self.get_local_archive_data(local_archive_file_path)

    def get_local_archive_data(self, local_archive_file_path):
        if not os.path.exists(local_archive_file_path):
            os.system("touch {0}".format(local_archive_file_path))
            print(
                "No Archive file has been found on bucket...Downloading all files..."
            )
            return []
        else:
            print(
                "Local Archive file has been found on bucket...Downloading all files..."
            )
            with open(local_archive_file_path, "r") as f:
                return f.read().splitlines()

    def download_archive_from_bucket(self, archive_file_path, local_archive_file_path):
        download_blob(self.bucket_name, archive_file_path, local_archive_file_path)
        print(
            str(
                "Archive file has been downloaded from bucket {0} to local path...".format(
                    self.bucket_name
                )
            )
        )
        with open(local_archive_file_path, "r") as f:
            archive_data_list = f.read().splitlines()
            num_downloaded = len(archive_data_list)
            print(
                "Count of Previously downloaded files are : {0}".format(num_downloaded)
            )
            return archive_data_list

    # file-dir is the directory where files get downloads
    def upload_file_to_bucket(self, source, source_file_name, destination_file_name, file_dir):
        try:
            base_path = self.bucket_path + "/" + source + "/"
            upload_blob(
                self.bucket_name, file_dir + "/" + source_file_name, base_path + destination_file_name
            )
        except Exception as exc:
            print("%r generated an exception: %s" % (source, exc))
