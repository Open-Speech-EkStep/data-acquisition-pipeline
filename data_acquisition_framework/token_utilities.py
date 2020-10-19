import logging

from .gcs_operations import *
from .pipeline_config import *

logging.basicConfig(level=logging.DEBUG)


def get_token_path():
    return channel_blob_path + '/' + 'token'


def update_token_in_bucket():
    upload_blob(bucket, 'token.txt', get_token_path())


def get_token_from_bucket():
    if check_blob(bucket, get_token_path()):
        download_blob(bucket, get_token_path(),
                      "token.txt")
    else:
        os.system('touch {0}'.format("token.txt"))
