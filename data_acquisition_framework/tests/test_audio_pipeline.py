import os
from unittest import TestCase
from unittest.mock import patch

from data_acquisition_framework.configs.paths import download_path
from data_acquisition_framework.pipelines.audio_pipeline import AudioPipeline


class Test(TestCase):

    # have to test if downloads folder is created if not present
    @patch('data_acquisition_framework.pipelines.audio_pipeline.MediaMetadata')
    @patch('data_acquisition_framework.pipelines.audio_pipeline.StorageUtil')
    def setUp(self, mock_storage_util, mock_media_metadata):
        current_path = os.path.dirname(os.path.realpath(__file__))
        self.audio_pipeline = AudioPipeline("file://" + current_path)
        self.mock_storage_util = mock_storage_util.return_value
        self.mock_media_metadata = mock_media_metadata.return_value

    def test_file_path_with_special_chars(self):
        class Request:
            url = "file://testweb%20%20_test.mp4"

        file_name = self.audio_pipeline.file_path(Request())

        expected_file_name = "testweb_20_20_test.mp4"
        self.assertEqual(expected_file_name, file_name)

    def test_file_path_without_special_chars(self):
        class Request:
            url = "file://test.mp4"

        file_name = self.audio_pipeline.file_path(Request())

        expected_file_name = "test.mp4"
        self.assertEqual(expected_file_name, file_name)

    # super has to tested
    def test_process_item(self):
        pass
        # item = Media()
        #
        # self.audio_pipeline.process_item(item, None)
        #
        # self.assertTrue(self.mock_files_pipeline.called)

    def test_item_completed(self):
        pass

    def test_upload_file_to_storage_with_no_exception(self):
        source = 'test'
        language = 'Tamil'
        item = {'source': source, 'language': language}
        file = 't.mp4'
        media_file_path = download_path + file
        url = 'http://test.com/t.mp4'
        expected = 60

        with patch.object(self.audio_pipeline, 'extract_metadata') as mock_extract_metadata:
            mock_extract_metadata.return_value = expected
            time_in_seconds = self.audio_pipeline.upload_file_to_storage(file, item, media_file_path, url)
            mock_extract_metadata.assert_called_once_with(media_file_path, url, item)

        self.assertEqual(expected, time_in_seconds)
        self.mock_storage_util.populate_local_archive.assert_called_once_with(source, url)
        self.mock_storage_util.upload_media_and_metadata_to_bucket.assert_called_once_with(source, media_file_path,
                                                                                           language)
        self.mock_storage_util.upload_archive_to_bucket.assert_called_once_with(source, language)

    def test_upload_file_to_storage_with_exception(self):
        source = 'test'
        language = 'Tamil'
        item = {'source': source, 'language': language}
        file = 't.mp4'
        media_file_path = download_path + file
        url = 'http://test.com/t.mp4'
        os.system("touch "+file)

        def throw_exception():
            raise FileNotFoundError()

        with patch.object(self.audio_pipeline, 'extract_metadata') as mock_extract_metadata:
            mock_extract_metadata.side_effect = throw_exception
            self.assertTrue(os.path.exists(file))
            self.audio_pipeline.upload_file_to_storage(file, item, media_file_path, url)
            self.assertFalse(os.path.exists(file))

    def test_upload_license_to_bucket(self):
        source = 'test'
        language = 'Tamil'
        item = {'source': source, 'language': language}
        media_file_path = 't.txt'

        self.audio_pipeline.upload_license_to_bucket(item, media_file_path)

        self.mock_storage_util.upload_license.assert_called_once_with(media_file_path, source, language)

    def test_get_media_requests(self):
        pass

    def test_is_download_success_return_true(self):
        item = {'files': ['sadfd']}

        result = self.audio_pipeline.is_download_success(item)

        self.assertTrue(result)

    def test_is_download_success_return_false(self):
        item = {'files': []}

        result = self.audio_pipeline.is_download_success(item)

        self.assertFalse(result)

    @patch('data_acquisition_framework.pipelines.audio_pipeline.get_file_format')
    @patch('data_acquisition_framework.pipelines.audio_pipeline.get_media_info')
    def test_extract_metadata(self, mock_get_media_info, mock_get_file_format):
        mock_get_file_format.return_value = "mp4"
        url = 'http://test.com/test.mp4'
        dummy_data = {'duration': 1,
                      'raw_file_name': 'test.mp4',
                      'name': None, 'gender': None,
                      'source_url': url,
                      'license': [],
                      "source": "Test",
                      "language": "Tamil",
                      'source_website': 'http://test.com/'}
        mock_get_media_info.return_value = dummy_data, 60
        self.mock_media_metadata.create_metadata.return_value = dummy_data
        file = download_path + "test.mp4"
        item = {
            'source_url': url,
            'license_urls': [],
            "source": "Test",
            "language": "Tamil",
        }

        duration_in_seconds = self.audio_pipeline.extract_metadata(file, 'http://test.com/test.mp4', item)

        self.assertEqual(60, duration_in_seconds)
        mock_get_file_format.assert_called_once_with(file)
        mock_get_media_info.assert_called_once_with(file, item['source'], item['language'], item['source_url'],
                                                    item['license_urls'], url)

        self.mock_media_metadata.create_metadata.assert_called_once_with(dummy_data)
        self.assertTrue(os.path.exists(download_path + "test.csv"))

        os.system("rm -rf " + download_path)
