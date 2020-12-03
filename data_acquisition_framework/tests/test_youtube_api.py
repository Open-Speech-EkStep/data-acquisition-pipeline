import os
import unittest
from unittest import TestCase
from unittest.mock import patch

from data_acquisition_framework.services.youtube.youtube_api import YoutubeApiUtils, YoutubeChannelCollector, \
    YoutubeApiBuilder


class TestYoutubeAPIBuilder(TestCase):
    def setUp(self):
        self.test_youtube_api_key = 'sampleyoutubeapikey'
        os.environ['youtube_api_key'] = self.test_youtube_api_key
        self.youtube_api_builder = YoutubeApiBuilder()

    def testInit(self):
        self.assertEqual(self.test_youtube_api_key, self.youtube_api_builder.youtube_api_key)

    @patch('data_acquisition_framework.services.youtube.youtube_api.build')
    def testGetYoutubeObject(self, mock_build):
        self.youtube_api_builder.get_youtube_object()
        mock_build.assert_called_once_with('youtube', 'v3', developerKey=self.test_youtube_api_key,
                                           cache_discovery=False)

    def tearDown(self):
        del os.environ['youtube_api_key']


class TestYoutubeApiUtils(TestCase):
    @patch('data_acquisition_framework.services.youtube.youtube_api.YoutubeChannelCollector')
    @patch('data_acquisition_framework.services.youtube.youtube_api.YoutubeApiBuilder.get_youtube_object')
    def setUp(self, mock_get_youtube_object, mock_youtube_channel_collector):
        test_youtube_api_key = 'sampleyoutubeapikey'
        os.environ["youtube_api_key"] = test_youtube_api_key
        self.youtube_api_utils = YoutubeApiUtils()
        self.mock_get_youtube_object = mock_get_youtube_object
        self.mock_youtube_channel_collector = mock_youtube_channel_collector

    def test_get_license_info_for_cc_video(self):
        video_id = 'abcdef'
        self.mock_get_youtube_object.return_value.videos.return_value.list.return_value.execute.return_value = {
            "items": [{"status": {"license": "creativeCommon"}}]}

        license_value = self.youtube_api_utils.get_license_info(video_id)

        self.assertEqual('Creative Commons', license_value)

    def test_get_license_info_for_sy_video(self):
        video_id = 'abcdef'
        self.mock_get_youtube_object.return_value.videos.return_value.list.return_value.execute.return_value = {
            "items": [{"status": {"license": "standardYoutube"}}]}

        license_value = self.youtube_api_utils.get_license_info(video_id)

        self.assertEqual('Standard Youtube', license_value)

    def test_get_videos_single_page(self):
        channel_id = 'dfwexae'
        video_id_one = 'jhs324jd'
        video_id_two = 'jeursn'
        video_id_three = 'uwnchaq23e'
        expected_video_ids = [video_id_one, video_id_two, video_id_three]
        self.mock_get_youtube_object.return_value.search.return_value.list.return_value.execute.return_value = {
            "items": [{"id": {"videoId": video_id_one}},
                      {"id": {"videoId": video_id_two}},
                      {"id": {"videoId": video_id_three}}]}

        actual_video_ids = self.youtube_api_utils.get_videos(channel_id)

        self.mock_get_youtube_object.return_value.search.return_value.list.return_value.execute.assert_called_once()
        self.assertEqual(expected_video_ids, actual_video_ids)

    def test_get_videos_two_page(self):
        channel_id = 'dfwexae'
        video_id_one = 'jhs324jd'
        video_id_two = 'jeursn'
        video_id_three = 'uwnchaq23e'
        video_id_four = 'umeixnat'
        expected_video_ids = [video_id_one, video_id_two, video_id_three, video_id_four]
        youtube_execute = self.mock_get_youtube_object.return_value.search.return_value.list.return_value.execute
        youtube_execute.side_effect = [
            {
                "items": [{"id": {"videoId": video_id_one}},
                          {"id": {"videoId": video_id_two}}],
                'nextPageToken': 'ABCD'},
            {
                "items": [{"id": {"videoId": video_id_three}},
                          {"id": {"videoId": video_id_four}}]}]

        actual_video_ids = self.youtube_api_utils.get_videos(channel_id)

        self.assertEqual(2, youtube_execute.call_count)
        self.assertEqual(expected_video_ids, actual_video_ids)

    def test_get_channels(self):
        self.youtube_api_utils.get_channels()
        self.mock_youtube_channel_collector.return_value.get_urls.assert_called_once()

    def tearDown(self):
        self.youtube_api_utils = None
        self.mock_get_youtube_object = None
        del os.environ["youtube_api_key"]


class TestYoutubeChannelCollector(TestCase):
    @patch('data_acquisition_framework.services.youtube.youtube_api.YoutubeApiBuilder.get_youtube_object')
    @patch('data_acquisition_framework.services.youtube.youtube_api.StorageUtil')
    @patch('data_acquisition_framework.services.youtube.youtube_api.load_config_file')
    def setUp(self, mock_load_config, mock_storage_util, mock_get_youtube_object):
        self.mock_storage_util = mock_storage_util
        self.test_config = {
            "language": "test_language",
            "language_code": "test_code",
            "keywords": [
                "key_one",
                "key_two",
            ],
            "words_to_ignore": [
                "ignore_one",
                "ignore_two"
            ],
            "max_results": 150
        }
        mock_load_config.return_value = self.test_config
        self.mock_get_youtube_object = mock_get_youtube_object
        self.youtube_channel_collector = YoutubeChannelCollector(self.mock_get_youtube_object.return_value)
        test_id_one = 'abcd'
        test_id_two = 'efgh'
        test_id_three = 'ijkl'
        test_channel_one = 'Test Channel One'
        test_channel_two = 'Test Channel Two'
        test_channel_three = 'Test Channel Three'
        channel_prefix_url = 'https://www.youtube.com/channel/'
        self.channel_search = self.mock_get_youtube_object.return_value.search.return_value.list.return_value.execute
        self.channel_search.return_value = {
            "items": [{"snippet": {"channelId": test_id_one, "channelTitle": test_channel_one}},
                      {"snippet": {"channelId": test_id_two, "channelTitle": test_channel_two}},
                      {"snippet": {"channelId": test_id_three, "channelTitle": test_channel_three}},
                      ]}
        self.expected_channels_collection = {channel_prefix_url + test_id_one: test_channel_one,
                                             channel_prefix_url + test_id_two: test_channel_two,
                                             channel_prefix_url + test_id_three: test_channel_three}

    def test_init(self):
        self.assertEqual(self.mock_get_youtube_object.return_value, self.youtube_channel_collector.youtube)
        self.assertEqual(self.mock_storage_util.return_value, self.youtube_channel_collector.storage_util)
        self.assertEqual("channel", self.youtube_channel_collector.TYPE)
        self.assertEqual(50, self.youtube_channel_collector.MAX_PAGE_RESULT)
        self.assertEqual(50, self.youtube_channel_collector.max_results)
        self.assertEqual(int(self.test_config['max_results'] // 50), self.youtube_channel_collector.pages)
        self.assertEqual(self.test_config["language_code"], self.youtube_channel_collector.rel_language)
        self.assertEqual(['in', self.test_config["language"],
                          "|".join(self.test_config["keywords"]),
                          "|".join(["-" + word_to_ignore for word_to_ignore in self.test_config["words_to_ignore"]])],
                         self.youtube_channel_collector.keywords)
        self.assertEqual(False, self.youtube_channel_collector.pages_exhausted)

    def test_get_urls_having_next_page_token(self):
        self.youtube_channel_collector.pages = 2
        self.channel_search.return_value['nextPageToken'] = 'HSNTIC'

        actual_collections = self.youtube_channel_collector.get_urls()

        self.assertEqual(2, self.mock_storage_util.return_value.get_token_from_local.call_count)
        self.assertEqual(2, self.mock_storage_util.return_value.set_token_in_local.call_count)
        self.assertEqual(self.expected_channels_collection, actual_collections)

    def test_get_urls_not_having_next_page_token(self):
        actual_collections = self.youtube_channel_collector.get_urls()

        self.mock_storage_util.return_value.get_token_from_local.assert_called_once()
        self.mock_storage_util.return_value.set_token_in_local.assert_not_called()
        self.assertEqual(self.expected_channels_collection, actual_collections)


if __name__ == '__main__':
    unittest.main()
