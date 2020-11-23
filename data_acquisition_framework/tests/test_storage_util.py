import json
import os
from unittest import TestCase
from unittest.mock import patch, call

from data_acquisition_framework.configs.paths import channels_path
from data_acquisition_framework.services.storage_util import StorageUtil, archives_base_path, archives_path, \
    download_path


class TestStorageUtil(TestCase):

    @patch('data_acquisition_framework.services.storage_util.load_config_file')
    def setUp(self, mock_load_storage_config):
        data = {
            "bucket": "ekstepspeechrecognition-dev",
            "channel_blob_path": "scrapydump/refactor_test",
            "archive_blob_path": "archive",
            "scraped_data_blob_path": "scraped"
        }
        mock_load_storage_config.return_value = data
        self.storage_util = StorageUtil()
        self.storage_config = data

    @patch('data_acquisition_framework.services.storage_util.set_gcs_credentials')
    def test_set_gcs_creds_called_with_proper_input(self, mock_set_gcs_credentials):
        input_value = '{"Credentials": {"name":"hello"}}'
        cred = json.loads('{"name":"hello"}')

        self.storage_util.set_gcs_creds(input_value)

        mock_set_gcs_credentials.assert_called_once_with(cred)

    @patch('data_acquisition_framework.services.storage_util.set_gcs_credentials')
    def test_set_gcs_creds_throw_input_type_error(self, mock_gcs_creds):
        input_value = {"Credentials": {"name": "hello"}}

        with self.assertRaises(TypeError):
            self.storage_util.set_gcs_creds(input_value)

    @patch('data_acquisition_framework.services.storage_util.set_gcs_credentials')
    def test_set_gcs_creds_throw_input_not_found_error(self, mock_gcs_creds):
        input_value = '{"name": "hello"}'

        with self.assertRaises(KeyError):
            self.storage_util.set_gcs_creds(input_value)

    @patch('data_acquisition_framework.services.storage_util.upload_blob')
    def test_upload_success(self, mock_upload_blob):
        file_to_upload = "test/a.mp4"
        location_to_upload = "test/a/a.mp4"

        self.storage_util.upload(file_to_upload, location_to_upload)

        mock_upload_blob.assert_called_once_with(self.storage_config["bucket"], file_to_upload, location_to_upload)

    @patch('data_acquisition_framework.services.storage_util.download_blob')
    def test_download(self, mock_download_blob):
        file_to_download = "test/a/a.mp4"
        download_location = "test/a.mp4"

        self.storage_util.download(file_to_download, download_location)

        mock_download_blob.assert_called_once_with(self.storage_config["bucket"], file_to_download, download_location)

    @patch('data_acquisition_framework.services.storage_util.check_blob')
    def test_check(self, mock_check_blob):
        mock_check_blob.return_value = True
        file_to_check = "test/a.mp4"
        result = self.storage_util.check(file_to_check)

        mock_check_blob.assert_called_once_with(self.storage_config['bucket'], file_to_check)
        self.assertTrue(result)

    @patch('data_acquisition_framework.services.storage_util.load_config_file')
    def test_get_archive_file_bucket_path_with_language(self, storage_config_mock):
        source = "test"
        language = "tamil"
        expected = self.storage_config["channel_blob_path"] + "/" + language + '/' + self.storage_config[
            "archive_blob_path"] + '/' + source + '/' + self.storage_util.archive_file_name
        config = self.storage_config.copy()
        config["channel_blob_path"] = config["channel_blob_path"] + "/<language>"
        storage_config_mock.return_value = config
        storage_util = StorageUtil()

        path = storage_util.get_archive_file_bucket_path(source, language)

        self.assertEqual(expected, path)

    def test_get_archive_file_bucket_path_without_language(self):
        source = "test"
        language = "tamil"
        expected = self.storage_config["channel_blob_path"] + '/' + self.storage_config[
            "archive_blob_path"] + '/' + source + '/' + self.storage_util.archive_file_name

        path = self.storage_util.get_archive_file_bucket_path(source, language)

        self.assertEqual(expected, path)

    @patch('data_acquisition_framework.services.storage_util.check_blob')
    @patch('data_acquisition_framework.services.storage_util.download_blob')
    def test_retrieve_archive_from_bucket_if_exists(self, mock_download_blob, mock_check_blob):
        mock_check_blob.return_value = True
        if os.path.exists(archives_base_path):
            os.system('rm -rf ' + archives_base_path)
        source = "test"
        language = "tamil"
        archive_bucket_path = self.storage_util.get_archive_file_bucket_path(source, language)

        def side_effect(bucket, source_file, destination):
            os.system("touch {0}".format(destination))

        mock_download_blob.side_effect = side_effect

        self.storage_util.retrieve_archive_from_bucket(source, language)

        self.assertTrue(os.path.exists(archives_base_path))
        self.assertTrue(os.path.exists(archives_base_path + source))
        self.assertTrue(os.path.exists(archives_path.replace("<source>", source)))
        mock_check_blob.assert_called_once_with(self.storage_config['bucket'],
                                                archive_bucket_path)
        mock_download_blob.assert_called_once_with(self.storage_config['bucket'], archive_bucket_path,
                                                   archives_path.replace("<source>", source))
        if os.path.exists(archives_base_path):
            os.system('rm -rf ' + archives_base_path)

    @patch('data_acquisition_framework.services.storage_util.check_blob')
    @patch('data_acquisition_framework.services.storage_util.download_blob')
    def test_retrieve_archive_from_bucket_if_not_exists(self, mock_download_blob, mock_check_blob):
        mock_check_blob.return_value = False
        if os.path.exists(archives_base_path):
            os.system('rm -rf ' + archives_base_path)
        source = "test"
        language = "tamil"
        archive_bucket_path = self.storage_util.get_archive_file_bucket_path(source, language)

        self.storage_util.retrieve_archive_from_bucket(source, language)

        self.assertTrue(os.path.exists(archives_base_path))
        self.assertTrue(os.path.exists(archives_base_path + source))
        self.assertTrue(os.path.exists(archives_path.replace("<source>", source)))
        mock_check_blob.assert_called_once_with(self.storage_config['bucket'],
                                                archive_bucket_path)
        mock_download_blob.assert_not_called()

        os.system('rm -rf ' + archives_base_path)

    def test_populate_local_archive(self):
        url = "http://gc/a.mp4"
        source = "test"

        if not os.path.exists(archives_base_path):
            os.system('mkdir ' + archives_base_path)
        if not os.path.exists(archives_base_path + source + "/"):
            os.system('mkdir {0}/{1}'.format(archives_base_path, source))

        self.storage_util.populate_local_archive(source, url)

        result = self.storage_util.retrieve_archive_from_local(source)

        self.assertEqual([url], result)

        os.system('rm -rf ' + archives_base_path)

    def test_retrieve_archive_from_local_if_exists(self):
        source = "test"
        url = "http://gc/a.mp4"
        archive_path = archives_path.replace('<source>', source)

        if not os.path.exists(archives_base_path):
            os.system('mkdir ' + archives_base_path)
        if not os.path.exists(archives_base_path + source + "/"):
            os.system('mkdir {0}/{1}'.format(archives_base_path, source))
        if not os.path.exists(archive_path):
            os.system('echo ' + url + '>' + archive_path)

        self.assertEqual([url], self.storage_util.retrieve_archive_from_local(source))

        os.system('rm -rf ' + archives_base_path)

    def test_retrieve_archive_from_local_if_not_exists(self):
        source = "test"
        archive_path = archives_path.replace('<source>', source)

        if os.path.exists(archive_path):
            os.system('rm -rf ' + archive_path)

        self.assertEqual([], self.storage_util.retrieve_archive_from_local(source))

    @patch('data_acquisition_framework.services.storage_util.upload_blob')
    def test_upload_archive_to_bucket(self, mock_upload):
        source = "test"

        test_archive_bucket_path = self.storage_util.get_archive_file_bucket_path(source)
        self.storage_util.upload_archive_to_bucket(source)

        mock_upload.assert_called_with(self.storage_config['bucket'], archives_path.replace("<source>", source),
                                       test_archive_bucket_path)

    @patch('data_acquisition_framework.services.storage_util.upload_blob')
    def test_upload_media_and_metadata_to_bucket(self, mock_upload_blob):
        metadata_file_path = download_path + "a.csv"
        media_file_path = download_path + "a.mp4"
        os.system("mkdir " + download_path)
        os.system("touch " + media_file_path)
        os.system("touch " + metadata_file_path)
        source = "test"
        media_bucket_path = self.storage_config['channel_blob_path'] + '/' + source + '/' + media_file_path.replace(
            download_path,
            "")
        meta_bucket_path = self.storage_config['channel_blob_path'] + '/' + source + '/' + metadata_file_path.replace(
            download_path,
            "")

        self.storage_util.upload_media_and_metadata_to_bucket(source, media_file_path)
        media_call = call(self.storage_config['bucket'], media_file_path, media_bucket_path)
        meta_call = call(self.storage_config['bucket'], metadata_file_path, meta_bucket_path)

        mock_upload_blob.assert_has_calls([media_call, meta_call])
        self.assertFalse(os.path.exists(media_file_path))
        self.assertFalse(os.path.exists(metadata_file_path))

        os.system("rm -rf " + download_path)

    @patch('data_acquisition_framework.services.storage_util.upload_blob')
    def test_upload_license(self, mock_upload_blob):
        license_path = download_path + "a.txt"
        os.system("mkdir " + download_path)
        os.system("touch " + license_path)
        source = "test"
        license_bucket_path = self.storage_config[
                                  'channel_blob_path'] + '/' + source + '/' + 'license/' + license_path.replace(
            download_path,
            "")

        self.storage_util.upload_license(license_path, source)

        mock_upload_blob.assert_called_once_with(self.storage_config['bucket'], license_path, license_bucket_path)
        self.assertFalse(os.path.exists(license_path))

        os.system("rm -rf " + download_path)

    def test_get_token_path(self):
        expected = self.storage_config['channel_blob_path'] + '/' + self.storage_util.token_file_name

        result = self.storage_util.get_token_path()

        self.assertEqual(expected, result)

    @patch('data_acquisition_framework.services.storage_util.upload_blob')
    def test_upload_token_to_bucket_if_exists(self, mock_upload_blob):
        token_path = "token.txt"
        os.system("touch " + token_path)

        self.storage_util.upload_token_to_bucket()

        token_bucket_path = self.storage_util.get_token_path()
        mock_upload_blob.assert_called_once_with(self.storage_config['bucket'], token_path, token_bucket_path)

        os.system("rm " + token_path)

    @patch('data_acquisition_framework.services.storage_util.upload_blob')
    def test_no_upload_token_to_bucket_if_not_exists(self, mock_upload_blob):
        token_path = "token.txt"
        if os.path.exists(token_path):
            os.system("rm " + token_path)

        self.storage_util.upload_token_to_bucket()

        mock_upload_blob.assert_not_called()

    @patch('data_acquisition_framework.services.storage_util.check_blob')
    @patch('data_acquisition_framework.services.storage_util.download_blob')
    def test_get_token_from_bucket_if_exists(self, mock_download_blob, mock_check_blob):
        mock_check_blob.return_value = True
        token_bucket_path = self.storage_util.get_token_path()

        def side_effect(bucket, file_to_download, download_location):
            os.system("touch {0}".format(download_location))

        mock_download_blob.side_effect = side_effect

        self.storage_util.get_token_from_bucket()

        mock_download_blob.assert_called_once_with(self.storage_config['bucket'], token_bucket_path, 'token.txt')
        self.assertTrue(os.path.exists("token.txt"))

        os.system("rm token.txt")

    @patch('data_acquisition_framework.services.storage_util.check_blob')
    @patch('data_acquisition_framework.services.storage_util.download_blob')
    def test_get_token_from_bucket_if_not_exists(self, mock_download_blob, mock_check_blob):
        mock_check_blob.return_value = False

        self.storage_util.get_token_from_bucket()

        mock_download_blob.assert_not_called()
        self.assertTrue(os.path.exists("token.txt"))

        os.system("rm token.txt")

    def test_get_token_from_local_if_exists(self):
        os.system("echo 'hello' > token.txt")
        expected = "hello"

        result = self.storage_util.get_token_from_local()

        self.assertEqual(expected, result)
        os.system("rm token.txt")

    def test_set_token_in_local(self):
        expected = ""

        result = self.storage_util.get_token_from_local()

        self.assertEqual(expected, result)

    def test_get_videos_file_path_in_bucket(self):
        source = "test"
        expected = self.storage_config["channel_blob_path"] + '/' + self.storage_config[
            "scraped_data_blob_path"] + '/' + source + '.csv'

        result = self.storage_util.get_videos_file_path_in_bucket(source)

        self.assertEqual(expected, result)

    def test_clear_required_directories_with_remove_downloads(self):
        os.system("mkdir " + download_path)
        os.system("mkdir " + channels_path)
        os.system("mkdir " + archives_base_path)

        self.storage_util.clear_required_directories()

        self.assertFalse(os.path.exists(download_path))
        self.assertFalse(os.path.exists(channels_path))
        self.assertFalse(os.path.exists(archives_base_path))

        os.system("rm -rf " + download_path)
        os.system("rm -rf " + channels_path)
        os.system("rm -rf " + archives_base_path)

    def test_clear_required_directories_with_create_downloads(self):
        os.system("rm -rf " + download_path)
        os.system("mkdir " + channels_path)
        os.system("mkdir " + archives_base_path)

        self.storage_util.clear_required_directories()

        self.assertTrue(os.path.exists(download_path))
        self.assertFalse(os.path.exists(channels_path))
        self.assertFalse(os.path.exists(archives_base_path))

        os.system("rm -rf " + download_path)
        os.system("rm -rf " + channels_path)
        os.system("rm -rf " + archives_base_path)

    def test_write_license_to_local(self):
        os.system("mkdir " + download_path)
        file_name = "a.txt"
        file_content = "hello"
        self.storage_util.write_license_to_local(file_name, file_content)

        file_path = download_path + file_name
        self.assertTrue(os.path.exists(file_path))
        f = open(file_path)
        self.assertEqual(file_content, f.read().rstrip())
        f.close()
        os.system("rm -rf " + download_path)

    def test_get_channel_videos_count(self):
        file_name = "test.txt"
        expected = 2
        channel_file_path = channels_path + file_name
        if not os.path.exists(channels_path):
            os.system("mkdir "+channels_path)
        with open(channel_file_path, 'w') as f:
            f.write("ab33cd"+"\n")
            f.write("ccdded")

        result = self.storage_util.get_channel_videos_count(file_name)

        self.assertEqual(expected, result)

        os.system('rm -rf '+channels_path)

    def test_get_media_paths(self):
        file1 = download_path+"file1.mp4"
        file2 = download_path+"file2.mp4"
        if not os.path.exists(download_path):
            os.system("mkdir "+download_path)
        os.system("touch "+file1)
        os.system("touch "+file2)
        expected = [file1, file2]

        media_paths = self.storage_util.get_media_paths()

        self.assertEqual(expected, media_paths)

        os.system("rm -rf " + download_path)