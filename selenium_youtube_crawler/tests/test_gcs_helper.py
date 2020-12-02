import os
from unittest import TestCase
from unittest.mock import patch

from selenium_youtube_crawler.gcs_helper import GCSHelper


class TestGCSHelper(TestCase):

    def setUp(self):
        self.bucket_name = "test_bucket"
        self.bucket_path = "test_bucket_path"
        self.gcs_helper = GCSHelper(self.bucket_name, self.bucket_path)

    @patch("selenium_youtube_crawler.gcs_helper.upload_blob")
    def test_upload_archive_to_bucket(self, mock_upload_blob):
        source = "test"

        self.gcs_helper.upload_archive_to_bucket(source)

        archive_path = "archive/" + source + "/archive.txt"
        mock_upload_blob.assert_called_once_with(self.bucket_name, archive_path, self.bucket_path + "/" + archive_path)

    @patch("selenium_youtube_crawler.gcs_helper.upload_blob")
    def test_upload_token_to_bucket(self, mock_upload_blob):
        token_file_name = "token.txt"
        self.gcs_helper.upload_token_to_bucket()

        mock_upload_blob.assert_called_once_with(self.bucket_name, token_file_name,
                                                 self.bucket_path + "/" + token_file_name)

    @patch("selenium_youtube_crawler.gcs_helper.check_blob")
    @patch("selenium_youtube_crawler.gcs_helper.download_blob")
    def test_download_token_from_bucket_if_present(self, mock_download_blob, mock_check_blob):
        token_file_name = "token.txt"
        token_path = self.bucket_path + "/" + token_file_name
        mock_check_blob.return_value = True

        self.gcs_helper.download_token_from_bucket()

        mock_check_blob.assert_called_once_with(self.bucket_name, token_path)
        mock_download_blob.assert_called_once_with(self.bucket_name, token_path, token_file_name)

    @patch("selenium_youtube_crawler.gcs_helper.check_blob")
    @patch("selenium_youtube_crawler.gcs_helper.download_blob")
    def test_download_token_from_bucket_if_not_present_in_bucket_and_local(self, mock_download_blob, mock_check_blob):
        token_file_name = "token.txt"
        token_path = self.bucket_path + "/" + token_file_name
        mock_check_blob.return_value = False

        self.gcs_helper.download_token_from_bucket()

        mock_check_blob.assert_called_once_with(self.bucket_name, token_path)
        mock_download_blob.assert_not_called()
        self.assertTrue(os.path.exists(token_file_name))

        os.system("rm " + token_file_name)

    # call of os.system is not tested
    @patch("selenium_youtube_crawler.gcs_helper.check_blob")
    @patch("selenium_youtube_crawler.gcs_helper.download_blob")
    def test_download_token_from_bucket_if_not_present_in_bucket_but_in_local(self, mock_download_blob,
                                                                              mock_check_blob):
        token_file_name = "token.txt"
        os.system("touch " + token_file_name)
        token_path = self.bucket_path + "/" + token_file_name
        mock_check_blob.return_value = False
        self.assertTrue(os.path.exists(token_file_name))

        self.gcs_helper.download_token_from_bucket()

        mock_check_blob.assert_called_once_with(self.bucket_name, token_path)
        mock_download_blob.assert_not_called()
        self.assertTrue(os.path.exists(token_file_name))

        os.system("rm " + token_file_name)

    @patch("selenium_youtube_crawler.gcs_helper.create_required_dirs_for_archive_if_not_present")
    @patch("selenium_youtube_crawler.gcs_helper.check_blob")
    def test_validate_and_download_archive_if_present_in_bucket(self, mock_check_blob, mock_create):
        source = "test"
        archive_file_path = self.bucket_path + "/archive/" + source + "/archive.txt"
        expected = ['hello']
        mock_check_blob.return_value = True
        is_method_called_flag = False

        with patch.object(self.gcs_helper, 'download_archive_from_bucket') as mock_download:
            with patch.object(self.gcs_helper, 'get_local_archive_data') as mock_get_local:
                mock_download.return_value = expected
                data = self.gcs_helper.validate_and_download_archive(source)

                self.assertEqual(expected, data)
                mock_download.assert_called_once_with(archive_file_path, "archive/" + source + "/archive.txt")
                mock_get_local.assert_not_called()
                is_method_called_flag = True

        mock_create.assert_called_once_with(source)
        self.assertTrue(is_method_called_flag)
        mock_check_blob.assert_called_once_with(self.bucket_name, archive_file_path)

    @patch("selenium_youtube_crawler.gcs_helper.create_required_dirs_for_archive_if_not_present")
    @patch("selenium_youtube_crawler.gcs_helper.check_blob")
    def test_validate_and_download_archive_if_not_present_in_bucket(self, mock_check_blob, mock_create):
        source = "test"
        archive_file_path = self.bucket_path + "/archive/" + source + "/archive.txt"
        expected = ['hello']
        mock_check_blob.return_value = False
        is_method_called_flag = False

        with patch.object(self.gcs_helper, 'download_archive_from_bucket') as mock_download:
            with patch.object(self.gcs_helper, 'get_local_archive_data') as mock_get_local:
                mock_get_local.return_value = expected
                data = self.gcs_helper.validate_and_download_archive(source)

                self.assertEqual(expected, data)
                mock_download.assert_not_called()
                is_method_called_flag = True

        mock_create.assert_called_once_with(source)
        self.assertTrue(is_method_called_flag)
        mock_check_blob.assert_called_once_with(self.bucket_name, archive_file_path)

    @patch("selenium_youtube_crawler.gcs_helper.download_blob")
    def test_download_archive_from_bucket(self, mock_download_blob):
        source = "test"
        local_archive_file_path = "archive/" + source + "/archive.txt"
        os.system("mkdir archive")
        os.system("mkdir archive/" + source)
        expected = ['hello', 'world']
        with open(local_archive_file_path, 'w') as f:
            f.writelines(word + "\n" for word in expected)
        archive_file_path = self.bucket_path + "/" + local_archive_file_path

        result = self.gcs_helper.download_archive_from_bucket(archive_file_path, local_archive_file_path)

        mock_download_blob.assert_called_once_with(self.bucket_name, archive_file_path, local_archive_file_path)
        self.assertTrue(os.path.exists(local_archive_file_path))
        self.assertEqual(expected, result)

        os.system("rm -rf archive")

    def test_get_local_archive_data_if_present(self):
        source = "test"
        local_archive_file_path = "archive/" + source + "/archive.txt"
        os.system("mkdir archive")
        os.system("mkdir archive/" + source)
        expected = ['hello', 'world']
        with open(local_archive_file_path, 'w') as f:
            f.writelines(word + "\n" for word in expected)

        result = self.gcs_helper.get_local_archive_data(local_archive_file_path)

        self.assertTrue(os.path.exists(local_archive_file_path))
        self.assertEqual(expected, result)

        os.system("rm -rf archive")

    def test_get_local_archive_data_if_not_present(self):
        source = "test"
        local_archive_file_path = "archive/" + source + "/archive.txt"
        os.system("mkdir archive")
        os.system("mkdir archive/" + source)
        expected = []

        result = self.gcs_helper.get_local_archive_data(local_archive_file_path)

        self.assertTrue(os.path.exists(local_archive_file_path))
        self.assertEqual(expected, result)

        os.system("rm -rf archive")

    @patch("selenium_youtube_crawler.gcs_helper.upload_blob")
    def test_upload_file_to_bucket(self, mock_upload_blob):
        file_dir = "downloads/"
        source_file_name = "test.mp4"
        dest_file_name = "test1.mp4"
        source = "test"

        file_path = file_dir + "/" + source_file_name
        bucket_path = self.bucket_path + "/" + source + "/" + dest_file_name

        self.gcs_helper.upload_file_to_bucket(source, source_file_name, dest_file_name, file_dir)

        mock_upload_blob.assert_called_once_with(self.bucket_name, file_path, bucket_path)
