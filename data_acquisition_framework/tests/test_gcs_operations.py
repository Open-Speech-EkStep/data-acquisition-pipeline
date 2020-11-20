import os
from unittest import TestCase
from unittest.mock import patch

from google.api_core.exceptions import Forbidden, NotFound

from data_acquisition_framework.services.storage.gcs_operations import set_gcs_credentials, upload_blob, download_blob, \
    check_blob


class GcsOperations(TestCase):
    def test_set_gcs_credentials(self):
        cred = {"name": "hello"}

        set_gcs_credentials(cred)

        self.assertTrue(os.path.exists("temp.json"))
        self.assertEqual('temp.json', os.environ['GOOGLE_APPLICATION_CREDENTIALS'])

        os.system('rm temp.json')

    @patch('data_acquisition_framework.services.storage.gcs_operations.storage.Client')
    def test_upload_blob(self, storage_client_mock):
        bucket_name = 'test_bucket'
        source_file_name = "test.txt"
        destination_blob_name = "scrapy/test/test.txt"

        bucket = storage_client_mock.return_value.bucket
        blob = bucket.return_value.blob

        upload_blob(bucket_name, source_file_name, destination_blob_name)

        bucket.assert_called_once_with(bucket_name)
        blob.assert_called_once_with(destination_blob_name)
        blob.return_value.upload_from_filename.assert_called_once_with(source_file_name)

    @patch('data_acquisition_framework.services.storage.gcs_operations.storage.Client')
    def test_upload_blob_raise_error_for_source_file_not_found(self, storage_client_mock):
        def raise_error(file_name):
            if file_name != "tester.txt":
                raise FileNotFoundError

        bucket_name = 'test_bucket'
        source_file_name = "test.txt"
        destination_blob_name = "scrapy/test/test.txt"

        bucket = storage_client_mock.return_value.bucket
        blob = bucket.return_value.blob
        upload_from_file_name_mock = blob.return_value.upload_from_filename
        upload_from_file_name_mock.side_effect = raise_error

        with self.assertRaises(FileNotFoundError):
            upload_blob(bucket_name, source_file_name, destination_blob_name)

        bucket.assert_called_once_with(bucket_name)
        blob.assert_called_once_with(destination_blob_name)
        upload_from_file_name_mock.assert_called_once_with(source_file_name)

    @patch('data_acquisition_framework.services.storage.gcs_operations.storage.Client')
    def test_upload_blob_raise_error_for_forbidden_bucket_access(self, storage_client_mock):
        def raise_error(file_name):
            raise Forbidden("forbidden access to bucket")

        bucket_name = 'test_bucket'
        source_file_name = "test.txt"
        destination_blob_name = "scrapy/test/test.txt"

        bucket = storage_client_mock.return_value.bucket
        blob = bucket.return_value.blob
        upload_from_file_name_mock = blob.return_value.upload_from_filename
        upload_from_file_name_mock.side_effect = raise_error

        with self.assertRaises(Forbidden):
            upload_blob(bucket_name, source_file_name, destination_blob_name)

        bucket.assert_called_once_with(bucket_name)
        blob.assert_called_once_with(destination_blob_name)
        upload_from_file_name_mock.assert_called_once_with(source_file_name)

    @patch('data_acquisition_framework.services.storage.gcs_operations.storage.Client')
    def test_download_blob(self, storage_client_mock):
        bucket_name = 'test_bucket'
        source_blob_name = "scrapy/test/test.txt"
        destination_file_name = "test.txt"

        bucket = storage_client_mock.return_value.bucket
        blob = bucket.return_value.blob

        download_blob(bucket_name, source_blob_name, destination_file_name)

        bucket.assert_called_once_with(bucket_name)
        blob.assert_called_once_with(source_blob_name)
        blob.return_value.download_to_filename.assert_called_once_with(destination_file_name)

    @patch('data_acquisition_framework.services.storage.gcs_operations.storage.Client')
    def test_download_blob_forbidden_access_to_bucket(self, storage_client_mock):
        def raise_error(file_name):
            raise Forbidden("forbidden access to bucket")

        bucket_name = 'test_bucket'
        source_blob_name = "scrapy/test/test.txt"
        destination_file_name = "test.txt"

        bucket = storage_client_mock.return_value.bucket
        blob = bucket.return_value.blob
        download_to_filename = blob.return_value.download_to_filename
        download_to_filename.side_effect = raise_error

        with self.assertRaises(Forbidden):
            download_blob(bucket_name, source_blob_name, destination_file_name)

        bucket.assert_called_once_with(bucket_name)
        blob.assert_called_once_with(source_blob_name)
        download_to_filename.assert_called_once_with(destination_file_name)

    @patch('data_acquisition_framework.services.storage.gcs_operations.storage.Client')
    def test_download_blob_file_not_in_to_bucket(self, storage_client_mock):
        def raise_error(file_name):
            raise NotFound("File not in bucket")

        bucket_name = 'test_bucket'
        source_blob_name = "scrapy/test/test.txt"
        destination_file_name = "test.txt"

        bucket = storage_client_mock.return_value.bucket
        blob = bucket.return_value.blob
        download_to_filename = blob.return_value.download_to_filename
        download_to_filename.side_effect = raise_error

        with self.assertRaises(NotFound):
            download_blob(bucket_name, source_blob_name, destination_file_name)

        bucket.assert_called_once_with(bucket_name)
        blob.assert_called_once_with(source_blob_name)
        download_to_filename.assert_called_once_with(destination_file_name)

    @patch('data_acquisition_framework.services.storage.gcs_operations.storage.Client')
    @patch('data_acquisition_framework.services.storage.gcs_operations.storage.Blob.exists')
    def test_check_blob(self, exists_mock, storage_client_mock):
        bucket_name = 'ekstepspeechrecognition-dev'
        file_prefix = "scrapydump/archive/Tamil_talks/archive.txt"
        exists_mock.return_value = True
        bucket = storage_client_mock.return_value.bucket

        stats = check_blob(bucket_name, file_prefix)

        bucket.assert_called_once_with(bucket_name)
        self.assertEqual(True, stats)

    @patch('data_acquisition_framework.services.storage.gcs_operations.storage.Client')
    @patch('data_acquisition_framework.services.storage.gcs_operations.storage.Blob.exists')
    def test_check_blob_forbidden_access(self, exists_mock, storage_client_mock):
        def raise_error(file_name):
            raise Forbidden("forbidden access to bucket")

        bucket_name = 'ekstepspeechrecognition-dev'
        file_prefix = "scrapydump/archive/Tamil_talks/archive.txt"
        exists_mock.return_value = True
        exists_mock.side_effect = raise_error
        bucket = storage_client_mock.return_value.bucket

        with self.assertRaises(Forbidden):
            check_blob(bucket_name, file_prefix)
        bucket.assert_called_once_with(bucket_name)
