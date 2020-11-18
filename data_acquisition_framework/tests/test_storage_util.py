import json
import os
from unittest import TestCase
from unittest.mock import patch

from data_acquisition_framework.services.storage_util import StorageUtil, archives_base_path, archives_path


class TestStorageUtil(TestCase):

    @patch('data_acquisition_framework.services.storage_util.load_storage_config')
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
    def test_set_gcs_creds_throw_input_type_error(self, mock_set_gcs_credentials):
        input_value = {"Credentials": {"name": "hello"}}

        with self.assertRaises(TypeError):
            self.storage_util.set_gcs_creds(input_value)

    @patch('data_acquisition_framework.services.storage_util.set_gcs_credentials')
    def test_set_gcs_creds_throw_input_not_found_error(self, mock_set_gcs_credentials):
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

    @patch('data_acquisition_framework.services.storage_util.load_storage_config')
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

        def side_effect(bucket, source, destination):
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

        if os.path.exists(archives_base_path):
            os.system('rm -rf ' + archives_base_path)

    def test_populate_local_archive(self):
        url = "http://gc/a.mp4"
        source = "test"

        if not os.path.exists(archives_base_path):
            os.system('mkdir ' + archives_base_path)
        if not os.path.exists(archives_base_path + source + "/"):
            os.system('mkdir {0}/{1}'.format(archives_base_path, source))

        self.storage_util.populate_local_archive(source, url)

        self.assertEqual(url, open(archives_path.replace("<source>", source)).read().rstrip())

        if os.path.exists(archives_base_path):
            os.system('rm -rf ' + archives_base_path)

    def test_retrieve_archive_from_local(self):
        self.fail()

    def test_upload_archive_to_bucket(self):
        self.fail()

    def test_upload_media_and_metadata_to_bucket(self):
        self.fail()

    def test_upload_license(self):
        self.fail()

    def test_get_token_path(self):
        self.fail()

    def test_upload_token_to_bucket(self):
        self.fail()

    def test_get_token_from_bucket(self):
        self.fail()

    def test_get_token_from_local(self):
        self.fail()

    def test_set_token_in_local(self):
        self.fail()

    def test_get_videos_file_path_in_bucket(self):
        self.fail()

    def test_clear_required_directories(self):
        self.fail()

    def test_write_license_to_local(self):
        self.fail()
