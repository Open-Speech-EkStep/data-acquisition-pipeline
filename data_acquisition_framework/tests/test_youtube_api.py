import os
from unittest import TestCase
from unittest.mock import patch

from data_acquisition_framework.services.youtube.youtube_api import YoutubeApiUtils


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
    def test_calculate_pages(self):
        self.fail()

    def test_get_urls(self):
        self.fail()
