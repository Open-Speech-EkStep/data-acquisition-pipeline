# import os
#
# from data_acquisition_framework.configs.pipeline_config import channel_blob_path
# from data_acquisition_framework.services.storage_util import StorageUtil
#
#
# def get_token_path():
#     return channel_blob_path + '/' + 'token'
#
#
# def update_token_in_bucket():
#     if os.path.exists('token.txt'):
#         StorageUtil().upload('token.txt', get_token_path())
#
#
# def get_token_from_bucket():
#     if StorageUtil().check(get_token_path()):
#         StorageUtil().download('token.txt', get_token_path())
#     else:
#         os.system('touch {0}'.format('token.txt'))
