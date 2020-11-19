import os
from unittest import TestCase
from unittest.mock import patch

from data_acquisition_framework.services.youtube.youtube_api import YoutubeApiUtils, YoutubeChannelCollector


class TestYoutubeApiUtils(TestCase):
    @patch('data_acquisition_framework.services.youtube.youtube_api.YoutubeChannelCollector')
    @patch('data_acquisition_framework.services.youtube.youtube_api.build')
    def setUp(self, mock_build, mock_youtube_channel_collector):
        test_youtube_api_key = 'sampleyoutubeapikey'
        os.environ["youtube_api_key"] = test_youtube_api_key
        self.youtube_api_utils = YoutubeApiUtils()
        self.mock_build = mock_build
        self.mock_youtube_channel_collector = mock_youtube_channel_collector

    def test_get_license_info_for_cc_video(self):
        video_id = 'abcdef'
        self.mock_build.return_value.videos.return_value.list.return_value.execute.return_value = {
            "items": [{"status": {"license": "creativeCommon"}}]}

        license_value = self.youtube_api_utils.get_license_info(video_id)

        self.assertEqual('Creative Commons', license_value)

    def test_get_license_info_for_sy_video(self):
        video_id = 'abcdef'
        self.mock_build.return_value.videos.return_value.list.return_value.execute.return_value = {
            "items": [{"status": {"license": "standardYoutube"}}]}

        license_value = self.youtube_api_utils.get_license_info(video_id)

        self.assertEqual('Standard Youtube', license_value)

    def test_get_videos(self):
        channel_id = 'dfwexae'
        video_id_one = 'jhs324jd'
        video_id_two = 'jeursn'
        video_id_three = 'uwnchaq23e'
        expected_video_ids = [video_id_one, video_id_two, video_id_three]
        self.mock_build.return_value.search.return_value.list.return_value.execute.return_value = {
            "items": [{"id": {"videoId": video_id_one}},
                      {"id": {"videoId": video_id_two}},
                      {"id": {"videoId": video_id_three}}]}

        actual_video_ids = self.youtube_api_utils.get_videos(channel_id)

        self.assertEqual(expected_video_ids, actual_video_ids)

    # More tests for get_videos can be added

    def test_get_channels(self):
        self.youtube_api_utils.get_channels()
        self.mock_youtube_channel_collector.return_value.get_urls.assert_called_once()

    def tearDown(self):
        self.youtube_api_utils = None
        self.mock_build = None
        del os.environ["youtube_api_key"]


class TestYoutubeChannelCollector(TestCase):
    @patch('data_acquisition_framework.services.youtube.youtube_api.build')
    @patch('data_acquisition_framework.services.youtube.youtube_api.StorageUtil')
    @patch('data_acquisition_framework.services.youtube.youtube_api.load_youtube_api_config')
    def setUp(self, mock_load_config, mock_storage_util, mock_build):
        self.mock_storage_util = mock_storage_util
        self.test_config = {
            "language": "test_language",
            "language_code": "test_code",
            "keywords": [
                "key_one",
                "key_two",
            ],
            "max_results": 40
        }
        mock_load_config.return_value = self.test_config
        self.mock_build = mock_build
        self.youtube_channel_collector = YoutubeChannelCollector(self.mock_build.return_value)

    def test_init(self):
        self.assertEqual(self.mock_build.return_value, self.youtube_channel_collector.youtube)
        self.assertEqual(self.mock_storage_util.return_value, self.youtube_channel_collector.storage_util)
        self.assertEqual("channel", self.youtube_channel_collector.TYPE)
        self.assertEqual(50, self.youtube_channel_collector.MAX_PAGE_RESULT)
        self.assertEqual(40, self.youtube_channel_collector.max_results)
        self.assertEqual(1, self.youtube_channel_collector.pages)
        self.assertEqual(self.test_config["language_code"], self.youtube_channel_collector.rel_language)
        self.assertEqual(['in', self.test_config["language"], "|".join(self.test_config["keywords"]), '-song'],
                         self.youtube_channel_collector.keywords)

    def test_get_urls_having_next_page_token(self):
        test_id_one = 'abcd'
        test_id_two = 'efgh'
        test_id_three = 'ijkl'
        test_channel_one = 'Test Channel One'
        test_channel_two = 'Test Channel Two'
        test_channel_three = 'Test Channel Three'
        expected_channels_collection = {'https://www.youtube.com/channel/' + test_id_one: test_channel_one,
                                        'https://www.youtube.com/channel/' + test_id_two: test_channel_two,
                                        'https://www.youtube.com/channel/' + test_id_three: test_channel_three}
        self.mock_build.return_value.search.return_value.list.return_value.execute.return_value = {
            "items": [{"snippet": {"channelId": test_id_one, "channelTitle": test_channel_one}},
                      {"snippet": {"channelId": test_id_two, "channelTitle": test_channel_two}},
                      {"snippet": {"channelId": test_id_three, "channelTitle": test_channel_three}},
                      ], "nextPageToken": "RQSTZ"}

        actual_collections = self.youtube_channel_collector.get_urls()

        self.mock_storage_util.return_value.get_token_from_local.assert_called_once()
        self.mock_storage_util.return_value.set_token_in_local.assert_called_once()
        self.assertEqual(expected_channels_collection, actual_collections)

    def test_get_urls_not_having_next_page_token(self):
        test_id_one = 'abcd'
        test_id_two = 'efgh'
        test_id_three = 'ijkl'
        test_channel_one = 'Test Channel One'
        test_channel_two = 'Test Channel Two'
        test_channel_three = 'Test Channel Three'
        expected_channels_collection = {'https://www.youtube.com/channel/' + test_id_one: test_channel_one,
                                        'https://www.youtube.com/channel/' + test_id_two: test_channel_two,
                                        'https://www.youtube.com/channel/' + test_id_three: test_channel_three}
        self.mock_build.return_value.search.return_value.list.return_value.execute.return_value = {
            "items": [{"snippet": {"channelId": test_id_one, "channelTitle": test_channel_one}},
                      {"snippet": {"channelId": test_id_two, "channelTitle": test_channel_two}},
                      {"snippet": {"channelId": test_id_three, "channelTitle": test_channel_three}},
                      ]}

        actual_collections = self.youtube_channel_collector.get_urls()

        self.mock_storage_util.return_value.get_token_from_local.assert_called_once()
        self.mock_storage_util.return_value.set_token_in_local.assert_not_called()
        self.assertEqual(expected_channels_collection, actual_collections)
