import os
from unittest import TestCase
from unittest.mock import patch

from data_acquisition_framework.spiders.datacollector_youtube import DatacollectorYoutubeSpider


class TestDatacollectorYoutubeSpider(TestCase):
    @patch('data_acquisition_framework.spiders.datacollector_youtube.YoutubeUtil')
    @patch('data_acquisition_framework.spiders.datacollector_youtube.StorageUtil')
    def setUp(self, mock_storage_util, mock_youtube_util):
        self.spider = DatacollectorYoutubeSpider(my_setting="", youtube_api_key="")
        self.mock_storage_util = mock_storage_util
        self.mock_youtube_util = mock_youtube_util

    def test_init(self):
        self.assertEqual(self.mock_storage_util.return_value, self.spider.storage_util)
        self.assertEqual(self.mock_youtube_util.return_value, self.spider.youtube_util)
        self.assertEqual("", os.environ['youtube_api_key'])
        self.mock_storage_util.return_value.set_gcs_creds.assert_called_once_with("")

    @patch('data_acquisition_framework.spiders.datacollector_youtube.is_youtube_api_mode')
    def test_spider_opened_channel_from_api(self, mock_is_youtube_api_mode):
        mock_is_youtube_api_mode.return_value = True
        self.spider.spider_opened()
        self.mock_storage_util.return_value.clear_required_directories.assert_called_once()
        self.mock_storage_util.return_value.get_token_from_bucket.assert_called_once()

    @patch('data_acquisition_framework.spiders.datacollector_youtube.is_youtube_api_mode')
    def test_spider_opened_channel_from_config(self, mock_is_youtube_api_mode):
        mock_is_youtube_api_mode.return_value = False
        self.spider.spider_opened()
        self.mock_storage_util.return_value.clear_required_directories.assert_called_once()
        self.mock_storage_util.return_value.get_token_from_bucket.assert_not_called()

    @patch('data_acquisition_framework.spiders.datacollector_youtube.is_youtube_api_mode')
    def test_spider_closed_channel_from_api(self, mock_is_youtube_api_mode):
        mock_is_youtube_api_mode.return_value = True
        self.spider.spider_closed()
        self.mock_storage_util.return_value.upload_token_to_bucket.assert_called_once()

    @patch('data_acquisition_framework.spiders.datacollector_youtube.is_youtube_api_mode')
    def test_spider_closed_channel_from_config(self, mock_is_youtube_api_mode):
        mock_is_youtube_api_mode.return_value = False
        self.spider.spider_closed()
        self.mock_storage_util.return_value.upload_token_to_bucket.assert_not_called()

    def _test_item_results(self, results, channel_ids, filemode_data, channel_names, filenames):
        for i, item in enumerate(results):
            self.assertEqual(channel_ids[i], item['channel_id'])
            self.assertEqual(filemode_data[i], item['filemode_data'])
            self.assertEqual(channel_names[i], item['channel_name'])
            self.assertEqual(filenames[i], item['filename'])

    def test_parse_for_filemode(self):
        test_filename_one = 'test_source.txt'
        self.mock_youtube_util.return_value.validate_mode_and_get_result.return_value = [
            ('file', test_filename_one, None)]

        results = self.spider.parse("", abc="")

        self._test_item_results(results, [None], [None], ['test_source'], [test_filename_one])
        self.mock_youtube_util.return_value.validate_mode_and_get_result.assert_called_once()

    def test_parse_for_channel_mode(self):
        test_filename_two = 'test_id__test_source.txt'
        self.mock_youtube_util.return_value.validate_mode_and_get_result.return_value = [
            ('channel', test_filename_two, None)]

        results = self.spider.parse("", abc="")

        self._test_item_results(results, ['test_id'], [None], ['test_source'], [test_filename_two])
        self.mock_youtube_util.return_value.validate_mode_and_get_result.assert_called_once()

    def test_parse_for_multiple(self):
        test_filename_one = 'test_source2.txt'
        test_filename_two = 'test_id__test_source.txt'
        self.mock_youtube_util.return_value.validate_mode_and_get_result.return_value = [
            ('channel', test_filename_two, None), ('file', test_filename_one, None)]

        results = self.spider.parse("", abc="")

        self._test_item_results(results, ['test_id', None], [None, None], ['test_source', 'test_source2'],
                                [test_filename_two, test_filename_one])
        self.mock_youtube_util.return_value.validate_mode_and_get_result.assert_called_once()

    def tearDown(self):
        del os.environ['youtube_api_key']
