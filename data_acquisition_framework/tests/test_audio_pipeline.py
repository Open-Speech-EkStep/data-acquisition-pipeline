import os
from unittest import TestCase
from unittest.mock import patch

from scrapy import Request
from scrapy.exceptions import DropItem

from data_acquisition_framework.configs.paths import download_path, archives_base_path
from data_acquisition_framework.items import Media, LicenseItem
from data_acquisition_framework.pipelines.audio_pipeline import AudioPipeline


class TestAudioPipeline(TestCase):

    # have to test if downloads folder is created if not present
    @patch('data_acquisition_framework.pipelines.audio_pipeline.MediaMetadata')
    @patch('data_acquisition_framework.pipelines.audio_pipeline.StorageUtil')
    def setUp(self, mock_storage_util, mock_media_metadata):
        current_path = os.path.dirname(os.path.realpath(__file__))
        self.audio_pipeline = AudioPipeline("file://" + current_path)
        self.mock_storage_util = mock_storage_util.return_value
        self.mock_media_metadata = mock_media_metadata.return_value
        self.assertTrue(os.path.exists(download_path))

    def tearDown(self):
        if os.path.exists(download_path):
            os.system("rm -rf "+download_path)

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

    def test_process_item_for_license_with_key_html_page(self):
        item = LicenseItem(file_urls=[],
                           key_name="html_page",
                           source='newsonair.gov.in',
                           language='Gujarati',
                           content="<body>hello world</body>")

        file_name = "license_{0}.txt".format(item["source"])
        with self.assertRaises(DropItem):
            self.audio_pipeline.process_item(item, None)

        self.mock_storage_util.write_license_to_local.assert_called_once_with(file_name, item["content"])
        self.mock_storage_util.upload_license.assert_called_once_with(file_name, item["source"], item["language"])

    def test_process_item_for_license_with_key_creative_commons(self):
        item = LicenseItem(file_urls=['https://test.com/license.html'],
                           key_name="creativecommons",
                           source='newsonair.gov.in',
                           language='Gujarati')
        content = "creative commons => " + item["file_urls"][0]
        file_name = "license_{0}.txt".format(item["source"])
        with self.assertRaises(DropItem):
            self.audio_pipeline.process_item(item, None)

        self.mock_storage_util.write_license_to_local.assert_called_once_with(file_name, content)
        self.mock_storage_util.upload_license.assert_called_once_with(file_name, item["source"], item["language"])

    @patch("data_acquisition_framework.pipelines.audio_pipeline.FilesPipeline.process_item")
    def test_process_item_for_license_with_key_document(self, mock_process_item):
        item = LicenseItem(
            key_name="document"
        )
        expected = "process"
        mock_process_item.return_value = expected

        result = self.audio_pipeline.process_item(item, None)

        mock_process_item.assert_called_once()
        self.assertEqual(expected, result)

    def test_process_item_for_license_with_invalid_key(self):
        item = LicenseItem(file_urls=['https://test.com/license.html'],
                           key_name="tester",
                           source='newsonair.gov.in',
                           language='Gujarati')

        with self.assertRaises(DropItem):
            self.audio_pipeline.process_item(item, None)

    @patch("data_acquisition_framework.pipelines.audio_pipeline.FilesPipeline.process_item")
    def test_process_item_for_non_license_item(self, mock_process_item):
        item = Media()
        expected = "process"
        mock_process_item.return_value = expected

        result = self.audio_pipeline.process_item(item, None)

        mock_process_item.assert_called_once()
        self.assertEqual(expected, result)

    def test_item_completed_for_success_download_path_with_audio_file_present(self):
        item = Media(
            title='Money-Talk-MT-2020317215621.mp3',
            file_urls=['https://newsonair.gov.in/Money-Talk-MT-2020317215621.mp3'],
            source='newsonair.com',
            license_urls=['http://newsonair.com/Website-Policy.aspx'],
            language='Gujarati',
            source_url='http://newsonair.com/regional-audio.aspx'
        )
        duration = 60
        os.system("touch {}{}".format(download_path, item['title']))
        results = [(True,
                    {
                        'url': 'https://newsonair.gov.in/Money-Talk-MT-2020317215621.mp3',
                        'path': 'Money-Talk-MT-2020317215621.mp3', 'checksum': 'a0a7e4c5aed4cde3c01e1de359cf323a',
                        'status': 'downloaded'
                    })]

        with patch.object(self.audio_pipeline, 'upload_file_to_storage') as mock_upload_file_to_storage:
            with patch.object(self.audio_pipeline, 'upload_license_to_bucket') as mock_upload_license_to_bucket:
                mock_upload_file_to_storage.return_value = duration
                result_from_method = self.audio_pipeline.item_completed(results, item, "")
                mock_upload_file_to_storage.assert_called_once_with(item["title"], item, download_path + item["title"],
                                                                    item["file_urls"][0])
                mock_upload_license_to_bucket.assert_not_called()
                item['files'] = [results[0][1]]
                item['duration'] = duration
                self.assertEqual(item, result_from_method)
        os.system("rm -rf " + download_path)

    def test_item_completed_for_success_download_path_with_license_file_present(self):
        item = LicenseItem(file_urls=['http://newsonair.com/Website-Policy.pdf'], key_name="creativecommons",
                           source='newsonair.gov.in',
                           language='Gujarati')
        os.system("touch {}{}".format(download_path, 'Website-Policy.pdf'))
        results = [(True,
                    {
                        'url': 'http://newsonair.com/Website-Policy.pdf',
                        'path': 'Website-Policy.pdf', 'checksum': 'a0a7e4c5aed4cde3c01e1de359cf323a',
                        'status': 'downloaded'
                    })]

        with patch.object(self.audio_pipeline, 'upload_file_to_storage') as mock_upload_file_to_storage:
            with patch.object(self.audio_pipeline, 'upload_license_to_bucket') as mock_upload_license_to_bucket:
                result_from_method = self.audio_pipeline.item_completed(results, item, "")
                mock_upload_file_to_storage.assert_not_called()
                mock_upload_license_to_bucket.assert_called_once_with(item, download_path + "Website-Policy.pdf")
                item['files'] = [results[0][1]]
                self.assertEqual(item, result_from_method)

        os.system("rm -rf " + download_path)

    def test_item_completed_for_success_download_path_with_file_not_present(self):
        item = Media(
            title='Money-Talk-MT-2020317215621.mp3',
            file_urls=['https://newsonair.gov.in/Money-Talk-MT-2020317215621.mp3'],
            source='newsonair.com',
            license_urls=['http://newsonair.com/Website-Policy.aspx'],
            language='Gujarati',
            source_url='http://newsonair.com/regional-audio.aspx'
        )
        if os.path.exists(download_path):
            os.system("rm -rf {}".format(download_path))
            os.system("mkdir {}".format(download_path))

        results = [(True,
                    {
                        'url': 'https://newsonair.gov.in/Money-Talk-MT-2020317215621.mp3',
                        'path': 'Money-Talk-MT-2020317215621.mp3', 'checksum': 'a0a7e4c5aed4cde3c01e1de359cf323a',
                        'status': 'downloaded'
                    })]

        with patch.object(self.audio_pipeline, 'upload_file_to_storage') as mock_upload_file_to_storage:
            with patch.object(self.audio_pipeline, 'upload_license_to_bucket') as mock_upload_license_to_bucket:
                result_from_method = self.audio_pipeline.item_completed(results, item, "")
                mock_upload_file_to_storage.assert_not_called()
                mock_upload_license_to_bucket.assert_not_called()
                item['files'] = [results[0][1]]
                item['duration'] = 0
                self.assertEqual(item, result_from_method)

        os.system("rm -rf " + download_path)

    def test_item_completed_for_failed_download(self):
        item = Media(
            title='Money-Talk-MT-2020317215621.mp3',
            file_urls=['https://newsonair.gov.in/Money-Talk-MT-2020317215621.mp3'],
            source='newsonair.com',
            license_urls=['http://newsonair.com/Website-Policy.aspx'],
            language='Gujarati',
            source_url='http://newsonair.com/regional-audio.aspx'
        )

        results = []
        with patch.object(self.audio_pipeline, 'upload_file_to_storage') as mock_upload_file_to_storage:
            with patch.object(self.audio_pipeline, 'upload_license_to_bucket') as mock_upload_license_to_bucket:
                output = self.audio_pipeline.item_completed(results, item, "")
                mock_upload_file_to_storage.assert_not_called()
                mock_upload_license_to_bucket.assert_not_called()

        self.assertEqual(item, output)

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
        os.system("touch " + media_file_path)

        def throw_exception(a, b, c):
            raise FileNotFoundError()

        with patch.object(self.audio_pipeline, 'extract_metadata') as mock_extract_metadata:
            mock_extract_metadata.side_effect = throw_exception
            self.assertTrue(os.path.exists(media_file_path))
            self.audio_pipeline.upload_file_to_storage(file, item, media_file_path, url)
            self.assertFalse(os.path.exists(media_file_path))

    def test_upload_license_to_bucket(self):
        source = 'test'
        language = 'Tamil'
        item = {'source': source, 'language': language}
        media_file_path = 't.txt'

        self.audio_pipeline.upload_license_to_bucket(item, media_file_path)

        self.mock_storage_util.upload_license.assert_called_once_with(media_file_path, source, language)

    @patch('data_acquisition_framework.pipelines.audio_pipeline.ItemAdapter')
    def test_get_media_requests_with_empty_archive(self, mock_item_adapter):
        urls = ['https://newsonair.gov.in/Regional-Aurangabad-Urdu-0840-202012210939.mp3']
        if not os.path.exists(archives_base_path):
            os.system("mkdir " + archives_base_path)
        mock_item_adapter.return_value.get.return_value = urls
        item = {
            "source": "test",
            "language": "ta",
            "FILES_URLS_FIELD": urls
        }
        self.mock_storage_util.retrieve_archive_from_local.return_value = []
        expected = [Request(urls[0])]

        results = self.audio_pipeline.get_media_requests(item, "")

        for index, result in enumerate(results):
            self.assertEqual(result.url, expected[index].url)
            self.assertEqual(result.headers.to_string(), expected[index].headers.to_string())

        self.mock_storage_util.retrieve_archive_from_bucket.assert_called_once_with(item['source'], item['language'])
        self.mock_storage_util.retrieve_archive_from_local.assert_called_once_with(item['source'])
        mock_item_adapter.assert_called_once_with(item)

    @patch('data_acquisition_framework.pipelines.audio_pipeline.ItemAdapter')
    def test_get_media_requests_with_filled_archive(self, mock_item_adapter):
        urls = ['https://newsonair.gov.in/Regional-Aurangabad-Urdu-0840-202012210939.mp3']
        if not os.path.exists(archives_base_path):
            os.system("mkdir " + archives_base_path)
        mock_item_adapter.return_value.get.return_value = urls
        item = {
            "source": "test",
            "language": "ta",
            "FILES_URLS_FIELD": urls
        }
        self.mock_storage_util.retrieve_archive_from_local.return_value = urls

        results = self.audio_pipeline.get_media_requests(item, "")

        self.assertTrue(len(results) == 0)

        self.mock_storage_util.retrieve_archive_from_bucket.assert_called_once_with(item['source'], item['language'])
        self.mock_storage_util.retrieve_archive_from_local.assert_called_once_with(item['source'])
        mock_item_adapter.assert_called_once_with(item)

    @patch('data_acquisition_framework.pipelines.audio_pipeline.ItemAdapter')
    def test_get_media_requests_with_source_already_in_class_object_and_folder_present(self, mock_item_adapter):
        urls = ['https://newsonair.gov.in/Regional-Aurangabad-Urdu-0840-202012210939.mp3']
        self.audio_pipeline.archive_list["test"] = urls
        if not os.path.exists(archives_base_path):
            os.system("mkdir " + archives_base_path)
            os.system("mkdir {}/test".format(archives_base_path))
        else:
            os.system("mkdir {}/test".format(archives_base_path))
        mock_item_adapter.return_value.get.return_value = urls
        item = {
            "source": "test",
            "language": "ta",
            "FILES_URLS_FIELD": urls
        }

        results = self.audio_pipeline.get_media_requests(item, "")

        self.mock_storage_util.retrieve_archive_from_bucket.assert_not_called()
        self.mock_storage_util.retrieve_archive_from_local.assert_not_called()
        self.assertEqual(self.audio_pipeline.archive_list, {"test": urls})
        self.assertTrue(len(results) == 0)
        mock_item_adapter.assert_called_once_with(item)

        os.system("rm -rf " + archives_base_path)

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
