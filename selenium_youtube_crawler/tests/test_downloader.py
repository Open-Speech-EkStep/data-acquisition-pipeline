import os
from unittest import TestCase
from unittest.mock import patch, call, MagicMock

import pandas as pd

from selenium_youtube_crawler.downloader import Downloader


class DownloaderTest(TestCase):

    @patch("selenium_youtube_crawler.downloader.GCSHelper")
    def setUp(self, mock_gcs_helper):
        self.bucket_name = "tester"
        self.bucket_path = "test"
        self.language = "ta"
        self.thread_local = MagicMock()
        self.downloader = Downloader(self.thread_local, self.bucket_name, self.bucket_path, self.language)
        self.file_dir = self.downloader.file_dir
        self.mock_gcs_helper = mock_gcs_helper.return_value

    def test_download(self):
        download_url = "http://test.mp4"
        video_id = "3sdf8sdf"
        source = "test"

        with patch.object(self.downloader, 'download_and_save') as mock_download_and_save:
            with patch.object(self.downloader, 'post_download_process') as mock_post_download_process:
                self.downloader.download(download_url, video_id, source)

                file_name = "file-id" + video_id + ".mp4"
                csv_name = "file-id" + video_id + ".csv"

                mock_download_and_save.assert_called_once_with(download_url, file_name)
                mock_post_download_process.assert_called_once_with(file_name, csv_name, source, video_id)

    @patch("selenium_youtube_crawler.downloader.requests.Response")
    @patch("selenium_youtube_crawler.downloader.requests.Session")
    def test_download_and_save_with_file_dir_not_present(self, mock_session, mock_response):
        os.system("rm -rf " + self.file_dir)
        url = "http://test.mp4"
        file_name = "test.mp4"

        def respond_stream(byte_count):
            for _ in range(3):
                yield b'dsf'

        mock_response_value = mock_response.return_value.__enter__.return_value
        mock_response_value.iter_content.side_effect = respond_stream
        mock_session.get.return_value = mock_response.return_value
        with patch.object(self.downloader, 'get_session') as mock_get_session:
            mock_get_session.return_value = mock_session
            self.downloader.download_and_save(url, file_name)

        self.assertTrue(os.path.exists(self.file_dir))
        self.assertTrue(os.path.exists(self.file_dir + "/" + file_name))
        mock_session.get.assert_called_once_with(url, stream=True)
        mock_response_value.iter_content.assert_called_once_with(1024)
        os.system("rm -rf " + self.file_dir)

    @patch("selenium_youtube_crawler.downloader.requests.Response")
    @patch("selenium_youtube_crawler.downloader.requests.Session")
    def test_download_and_save_with_file_dir_present(self, mock_session, mock_response):
        os.system("rm -rf " + self.file_dir)
        os.system("mkdir " + self.file_dir)
        url = "http://test.mp4"
        file_name = "test.mp4"

        def respond_stream(byte_count):
            for _ in range(3):
                yield b'dsf'

        mock_response_value = mock_response.return_value.__enter__.return_value
        mock_response_value.iter_content.side_effect = respond_stream
        mock_session.get.return_value = mock_response.return_value
        with patch.object(self.downloader, 'get_session') as mock_get_session:
            mock_get_session.return_value = mock_session
            self.downloader.download_and_save(url, file_name)

        self.assertTrue(os.path.exists(self.file_dir))
        self.assertTrue(os.path.exists(self.file_dir + "/" + file_name))
        mock_session.get.assert_called_once_with(url, stream=True)
        mock_response_value.iter_content.assert_called_once_with(1024)
        os.system("rm -rf " + self.file_dir)

    @patch("selenium_youtube_crawler.downloader.extract_metadata")
    def test_post_download_process(self, mock_extract_metadata):
        file_name = "test.mp4"
        csv_name = "test.csv"
        source = "test"
        video_id = "43dsfgs"
        youtube_url = "https://www.youtube.com/watch?v=" + video_id
        duration = 60
        mock_extract_metadata.return_value = duration
        modified_file_name = str(duration) + file_name
        modified_csv_name = str(duration) + csv_name
        is_method_called_flag = False

        with patch.object(self.downloader, 'update_metadata_fields') as mock_update_metadata_fields:
            with patch.object(self.downloader, 'clean_up_files') as mock_clean_up_files:
                with patch.object(self.downloader, 'update_archive') as mock_update_archive:
                    self.downloader.post_download_process(file_name, csv_name, source, video_id)

                    mock_update_metadata_fields.assert_called_once_with(modified_file_name, csv_name, video_id)
                    mock_clean_up_files.assert_called_once_with(file_name, csv_name)
                    mock_update_archive.assert_called_once_with(source, video_id)
                    is_method_called_flag = True

        self.assertTrue(is_method_called_flag)
        mock_extract_metadata.assert_called_once_with(self.downloader.file_dir, file_name, youtube_url, source)
        upload1_call = call(source, file_name, modified_file_name, self.file_dir)
        upload2_call = call(source, csv_name, modified_csv_name, self.file_dir)

        self.mock_gcs_helper.upload_file_to_bucket.assert_has_calls([upload1_call, upload2_call])

    @patch("selenium_youtube_crawler.downloader.YoutubeApiUtils")
    def test_update_metdata_fields(self, mock_youtube_api_utils):
        os.system("mkdir " + self.file_dir)
        metadata = dict({
            'name': ['hello']
        })
        csv_name = "test.csv"
        df = pd.DataFrame(metadata, columns=['name'])
        df.to_csv(self.file_dir + "/" + csv_name, index=False)

        modified_file_name = str(60) + "test.mp4"
        video_id = "43dsfgs"
        expected_license_string = 'Creative Commons'
        mock_youtube_api_utils.return_value.get_license_info.return_value = expected_license_string

        self.downloader.update_metadata_fields(modified_file_name, csv_name, video_id)

        data = pd.read_csv(self.file_dir + "/" + csv_name)
        self.assertEqual('hello', data['name'][0])
        self.assertEqual(modified_file_name, data['raw_file_name'][0])
        self.assertEqual(modified_file_name, data['title'][0])
        self.assertEqual(self.language, data['language'][0])
        self.assertEqual(expected_license_string, data['license'][0])
        os.system("rm -rf " + self.file_dir)

    def test_clean_up_files(self):
        file_name = "test.mp4"
        csv_name = "test.csv"
        os.system("mkdir " + self.file_dir)
        os.system("touch {0}/{1}".format(self.file_dir, csv_name))
        os.system("touch {0}/{1}".format(self.file_dir, csv_name))

        self.downloader.clean_up_files(file_name, csv_name)

        self.assertFalse(os.path.exists(self.file_dir + "/" + file_name))
        self.assertFalse(os.path.exists(self.file_dir + "/" + csv_name))

        os.system("rm -rf " + self.file_dir)

    @patch("selenium_youtube_crawler.downloader.populate_local_archive")
    def test_update_archive(self, mock_populate_local_archive):
        video_id = "24jsdf"
        source = "test"

        self.downloader.update_archive(source, video_id)

        mock_populate_local_archive.assert_called_once_with(source, video_id)
        self.mock_gcs_helper.upload_archive_to_bucket.assert_called_once_with(source)

    def test_get_session_with_session_attribute_present(self):
        self.thread_local.session = "hello"

        session = self.downloader.get_session()

        self.assertEqual("hello", session)

    # @patch("selenium_youtube_crawler.downloader.requests.Session")
    # def test_get_session_with_session_attribute_not_present(self, mock_session):
    #
    #     session = self.downloader.get_session()
