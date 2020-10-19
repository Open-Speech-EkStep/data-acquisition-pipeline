from gcs import upload_blob, download_blob, check_blob
import os


class GCSHelper:
    def __init__(self, bucket_name, bucket_path):
        self.bucket_name = bucket_name
        self.bucket_path = bucket_path

    def upload_archive_to_bucket(self, source):
        upload_blob(
            self.bucket_name,
            "archive/"+source + "/archive.txt",
            self.bucket_path + "/archive/" + source + "/archive.txt",
        )

    def upload_token_to_bucket(self):
        upload_blob(self.bucket_name,'token.txt',self.bucket_path+'/token.txt')

    def download_token_from_bucket(self):
        token_path = self.bucket_path + "/token.txt"
        if check_blob(self.bucket_name, token_path):
            download_blob(self.bucket_name, token_path, "token.txt")
            print(
                str(
                    "Token file has been downloaded from bucket {0} to local path...".format(
                        self.bucket_name
                    )
                )
            )
        else:
            if not os.path.exists("token.txt"):
                os.system("touch {0}".format("token.txt"))
            print("No token file has been found on bucket...")

    def download_archive_from_bucket(self, source):
        if not os.path.exists("archive"):
            os.system("mkdir archive")

        archive_file_path = self.bucket_path + "/archive/" + source + "/archive.txt"
        local_archive_source ="archive/{0}".format(source)
        local_archive_file_path = "{0}/archive.txt".format(local_archive_source)

        if check_blob(self.bucket_name, archive_file_path):

            if not os.path.exists(local_archive_source):
                os.system("mkdir {0}".format(local_archive_source))

            download_blob(self.bucket_name, archive_file_path, local_archive_file_path)
            print(
                str(
                    "Archive file has been downloaded from bucket {0} to local path...".format(
                        self.bucket_name
                    )
                )
            )
            num_downloaded = sum(1 for line in open(local_archive_file_path))
            print(
                "Count of Previously downloaded files are : {0}".format(num_downloaded)
            )
            with open(local_archive_file_path, "r") as f:
                return f.read().splitlines()
        else:
            if not os.path.exists(local_archive_source):
                os.system("mkdir {0}".format(local_archive_source))
                os.system("touch {0}".format(local_archive_file_path))
                print(
                    "No Archive file has been found on bucket...Downloading all files..."
                )
                return []
            else:
                with open(local_archive_file_path, "r") as f:
                    return f.read().splitlines()
                print(
                    "Local Archive file has been found on bucket...Downloading all files..."
                )

    # file-dir is the directory where files get downloads
    def upload_file_to_bucket(self, source, source_file_name, destination_file_name, file_dir):
        try:
            base_path = self.bucket_path + "/" + source + "/"
            upload_blob(
                self.bucket_name, file_dir+"/"+source_file_name, base_path + destination_file_name
            )
        except Exception as exc:
            print("%r generated an exception: %s" % (source, exc))
