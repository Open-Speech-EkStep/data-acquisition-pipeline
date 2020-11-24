import os
from concurrent import futures
from unittest import TestCase
from unittest.mock import patch, MagicMock

import pandas as pd

from data_acquisition_framework.configs.paths import channels_path, archives_base_path
from data_acquisition_framework.services.youtube_util import YoutubeUtil, remove_rejected_video, \
    check_dataframe_validity, create_channel_file_for_file_mode, get_gender, get_speaker, get_video_batch


class TestYoutubeUtil(TestCase):

    @patch('data_acquisition_framework.services.youtube_util.StorageUtil')
    @patch('data_acquisition_framework.services.youtube_util.YoutubeDL')
    @patch('data_acquisition_framework.services.youtube_util.YoutubeApiUtils')
    def setUp(self, mock_yt_api_utils, mock_yt_dl, mock_storage_util):
        mock_yt_api_utils.return_value.get_videos.return_value = ["dfsfsdf", "dfsdfsdf"]
        self.youtube_util = YoutubeUtil()
        self.mock_yt_api_utils = mock_yt_api_utils
        self.mock_storage_util = mock_storage_util

    def test_create_channel_file_if_channels_folder_not_present(self):
        if os.path.exists(channels_path):
            os.system("rm -rf " + channels_path)
        channel1_id = "2342342ibbj"
        channel1_name = 'test1'
        channel2_id = "2342342ib222"
        channel2_name = 'test2'
        channel1_file = channel1_id + '__' + channel1_name + '.txt'
        channel2_file = channel2_id + '__' + channel2_name + '.txt'
        expected = [channel1_file, channel2_file]
        source_channel_dict = {("https://youtube.com/channel/%s" % channel1_id): channel1_name,
                               ("https://youtube.com/channel/%s" % channel2_id): channel2_name}

        self.youtube_util.create_channel_file(source_channel_dict)

        channel_files = os.listdir(channels_path)
        self.assertEqual(expected, channel_files)

        os.system("rm -rf " + channels_path)

    def test_create_channel_file_if_channels_folder_present(self):
        if not os.path.exists(channels_path):
            os.system("mkdir " + channels_path)
        channel1_id = "2342342ibbj"
        channel1_name = 'test1'
        channel2_id = "2342342ib222"
        channel2_name = 'test2'
        channel1_file = channel1_id + '__' + channel1_name + '.txt'
        channel2_file = channel2_id + '__' + channel2_name + '.txt'
        expected = [channel1_file, channel2_file]
        source_channel_dict = {("https://youtube.com/channel/%s" % channel1_id): channel1_name,
                               ("https://youtube.com/channel/%s" % channel2_id): channel2_name}

        self.youtube_util.create_channel_file(source_channel_dict)

        channel_files = os.listdir(channels_path)
        self.assertEqual(expected, channel_files)

        os.system("rm -rf " + channels_path)

    def test_remove_rejected_video(self):
        if not os.path.exists(channels_path):
            os.system("mkdir " + channels_path)
        file_name = "test.txt"
        file_path = channels_path + file_name
        os.system("echo '2342343\n234233' > {0}".format(file_path))
        expected = ["234233"]

        remove_rejected_video(file_name, "2342343")

        with open(file_path, 'r') as f:
            result = f.read().splitlines()

        self.assertEqual(expected, result)

        os.system("rm -rf " + channels_path)

    @patch('data_acquisition_framework.services.youtube_util.as_completed')
    @patch('data_acquisition_framework.services.youtube_util.ThreadPoolExecutor')
    def test_download_files(self, mock_thread_pool_executor, mock_as_completed):
        channel_name = "test"
        file_name = "test.txt"
        file_path = channels_path + file_name
        expected = ["234233"]
        if not os.path.exists(channels_path):
            os.system("mkdir " + channels_path)

        os.system("echo '2342343\n234233' > {0}".format(file_path))
        result1 = MagicMock(spec=futures.Future)
        result1.result.return_value = (False, "234233")
        result2 = MagicMock(spec=futures.Future)
        result2.result.return_value = (True, "2342343")
        mock_as_completed.return_value = [result1, result2]

        self.youtube_util.download_files(channel_name, file_name, ["234233", '2342343'])

        with open(file_path, 'r') as f:
            result = f.read().splitlines()

        self.assertEqual(expected, result)

        os.system("rm -rf " + channels_path)

    def test_get_license_info(self):
        license_info = "Creative Commons"
        self.mock_yt_api_utils.return_value.get_license_info.return_value = license_info

        video_id = "948495"
        result = self.youtube_util.get_license_info("%s" % video_id)

        self.assertEqual(license_info, result)
        self.mock_yt_api_utils.return_value.get_license_info.assert_called_once_with(video_id)

    def test_get_channels(self):
        channels = {"https://youtube.com/channel/1231243432432": "test1",
                    "https://youtube.com/channel/32423423432": "test2"}
        self.mock_yt_api_utils.return_value.get_channels.return_value = channels

        result = self.youtube_util.get_channels()

        self.assertEqual(channels, result)
        self.mock_yt_api_utils.return_value.get_channels.assert_called_once()

    @patch('data_acquisition_framework.services.youtube_util.mode', 'channel')
    def test_get_video_info_for_channel_mode(self):
        license_info = "Creative Commons"
        self.mock_yt_api_utils.return_value.get_license_info.return_value = license_info
        file = "60file-idsfgsft.mp4"
        channel_name = "test"
        channel_id = "ew52455435"
        expected = {
            'duration': 1.0, 'source': channel_name,
            'raw_file_name': file,
            'name': None,
            'gender': None,
            'source_url': "https://www.youtube.com/watch?v=sfgsft", 'license': license_info,
            'source_website': 'https://www.youtube.com/channel/' + channel_id
        }

        result = self.youtube_util.get_video_info(file, channel_name, None, channel_id)

        self.assertEqual(expected, result)

    @patch('data_acquisition_framework.services.youtube_util.mode', 'file')
    @patch('data_acquisition_framework.services.youtube_util.file_speaker_gender_column', 'gender')
    @patch('data_acquisition_framework.services.youtube_util.file_speaker_name_column', 'name')
    @patch('data_acquisition_framework.services.youtube_util.file_url_name_column', 'url')
    def test_get_video_info_for_file_mode(self):
        license_info = "Creative Commons"
        self.mock_yt_api_utils.return_value.get_license_info.return_value = license_info
        file = "60file-idsfgsft.mp4"
        channel_name = "test"
        channel_id = "ew52455435"
        expected = {
            'duration': 1.0, 'source': channel_name,
            'raw_file_name': file,
            'name': "tester",
            'gender': "male",
            'source_url': "https://www.youtube.com/watch?v=sfgsft", 'license': license_info
        }
        data = [
            ["male", 'tester', 'sfgsft']
        ]
        scraped_data = pd.DataFrame(data, columns=['gender', 'name', 'url'])

        result = self.youtube_util.get_video_info(file, channel_name, scraped_data, channel_id)

        self.assertEqual(expected, result)

    @patch('data_acquisition_framework.services.youtube_util.channel_url_dict',
           {"https://youtube.com/channel/1231243432432": "test1", "https://youtube.com/channel/32423423432": "test2"})
    def test_get_channel_from_source_with_dict_present(self):
        channel_dict = {"https://youtube.com/channel/1231243432432": "test1",
                        "https://youtube.com/channel/32423423432": "test2"}
        with patch.object(self.youtube_util, 'create_channel_file') as create_channel_file_mock:
            self.youtube_util.get_channels_from_source()
            create_channel_file_mock.assert_called_once_with(channel_dict)

    @patch('data_acquisition_framework.services.youtube_util.channel_url_dict', {})
    def test_get_channel_from_source_with_dict_empty(self):
        channels = {"https://youtube.com/channel/sdfdsf34545": "test1",
                    "https://youtube.com/channel/sadfds444555": "test2"}
        self.mock_yt_api_utils.return_value.get_channels.return_value = channels

        with patch.object(self.youtube_util, 'create_channel_file') as create_channel_file_mock:
            self.youtube_util.get_channels_from_source()
            create_channel_file_mock.assert_called_once_with(channels)

    @patch('data_acquisition_framework.services.youtube_util.mode', 'file')
    @patch('data_acquisition_framework.services.youtube_util.file_speaker_gender_column', 'gender')
    @patch('data_acquisition_framework.services.youtube_util.file_speaker_name_column', 'name')
    @patch('data_acquisition_framework.services.youtube_util.file_url_name_column', 'url')
    @patch('data_acquisition_framework.services.youtube_util.source_name', 'test')
    def test_validate_mode_and_get_result_for_file_mode_if_file_is_in_bucket(self):
        expected_scraped_data = pd.DataFrame([["male", 'tester', 'sfgsft']],
                                             columns=['gender', 'name', 'url'])

        def download_side_effect(bucket_path, file_path):
            data = [
                ["male", 'tester', 'https://www.youtube.com/watch?v=sfgsft']
            ]
            scraped_data = pd.DataFrame(data, columns=['gender', 'name', 'url'])
            scraped_data.to_csv(file_path, index=False)

        self.mock_storage_util.return_value.get_videos_file_path_in_bucket.return_value = "testpath/bucket/a.csv"
        self.mock_storage_util.return_value.check.return_value = True
        self.mock_storage_util.return_value.download.side_effect = download_side_effect

        for mode, channel_file, data_scrap in self.youtube_util.validate_mode_and_get_result():
            self.assertEqual('file', mode)
            self.assertEqual('test.txt', channel_file)
            self.assertTrue(expected_scraped_data.equals(data_scrap))
        self.mock_storage_util.return_value.download.assert_called()
        self.mock_storage_util.return_value.check.assert_called()
        os.system("rm -rf " + channels_path)
        os.system("rm test.csv")

    @patch('data_acquisition_framework.services.youtube_util.mode', 'file')
    def test_validate_mode_and_get_result_for_file_mode_if_file_is_not_in_bucket(self):
        if os.path.exists(channels_path):
            os.system("rm -rf " + channels_path)

        self.mock_storage_util.return_value.get_videos_file_path_in_bucket.return_value = "testpath/bucket/a.csv"
        self.mock_storage_util.return_value.check.return_value = False

        for mode, channel_file, _ in self.youtube_util.validate_mode_and_get_result():
            self.fail()

        self.mock_storage_util.return_value.download.assert_not_called()

    @patch('data_acquisition_framework.services.youtube_util.mode', 'channel')
    @patch('data_acquisition_framework.services.youtube_util.channel_url_dict', {})
    def test_validate_mode_and_get_result_for_channel_mode(self):
        if os.path.exists(channels_path):
            os.system("rm -rf " + channels_path)

        channels = {"https://youtube.com/channel/123456": "test1",
                    "https://youtube.com/channel/987655": "test2"}
        self.mock_yt_api_utils.return_value.get_channels.return_value = channels
        count = 0
        expected_datas = [
            {
                "mode": 'channel',
                "file": "987655__test2.txt",
                "data": None
            },
            {
                "mode": 'channel',
                "file": "123456__test1.txt",
                "data": None
            },
        ]

        for mode, channel_file, data_scrap in self.youtube_util.validate_mode_and_get_result():
            expected = expected_datas[count]
            count += 1
            self.assertEqual(expected["mode"], mode)
            self.assertEqual(expected["file"], channel_file)
            self.assertTrue(data_scrap is None)

        os.system("rm -rf " + channels_path)

    @patch('data_acquisition_framework.services.youtube_util.mode', 'playlist')
    def test_validate_mode_and_get_result_for_invalid_mode(self):

        for mode, channel_file, _ in self.youtube_util.validate_mode_and_get_result():
            self.fail()

    @patch('data_acquisition_framework.services.youtube_util.file_speaker_gender_column', 'gender')
    @patch('data_acquisition_framework.services.youtube_util.file_speaker_name_column', 'name')
    @patch('data_acquisition_framework.services.youtube_util.file_url_name_column', 'url')
    def test_check_dataframe_validity_all_present(self):
        data = [
            ["male", 'tester', 'https://www.youtube.com/watch?v=sfgsft']
        ]
        scraped_data = pd.DataFrame(data, columns=['gender', 'name', 'url'])

        try:
            check_dataframe_validity(scraped_data)
        except KeyError:
            self.fail()

    @patch('data_acquisition_framework.services.youtube_util.file_speaker_gender_column', 'gender')
    @patch('data_acquisition_framework.services.youtube_util.file_speaker_name_column', 'name')
    @patch('data_acquisition_framework.services.youtube_util.file_url_name_column', 'url')
    def test_check_dataframe_validity_url_column_not_recognizable(self):
        data = [
            ["male", 'tester', 'https://www.youtube.com/watch?v=sfgsft']
        ]
        scraped_data = pd.DataFrame(data, columns=['gender', 'name', 'video_url'])

        def method_to_call():
            check_dataframe_validity(scraped_data)

        self.assertRaises(KeyError, method_to_call)

    @patch('data_acquisition_framework.services.youtube_util.file_speaker_gender_column', 'gender')
    @patch('data_acquisition_framework.services.youtube_util.file_speaker_name_column', 'name')
    @patch('data_acquisition_framework.services.youtube_util.file_url_name_column', 'url')
    def test_check_dataframe_validity_name_column_not_recognizable(self):
        data = [
            ["male", 'tester', 'https://www.youtube.com/watch?v=sfgsft']
        ]
        scraped_data = pd.DataFrame(data, columns=['gender', 'name_of_speaker', 'url'])

        def method_to_call():
            check_dataframe_validity(scraped_data)

        self.assertRaises(KeyError, method_to_call)

    @patch('data_acquisition_framework.services.youtube_util.file_speaker_gender_column', 'gender')
    @patch('data_acquisition_framework.services.youtube_util.file_speaker_name_column', 'name')
    @patch('data_acquisition_framework.services.youtube_util.file_url_name_column', 'url')
    def test_check_dataframe_validity_gender_column_not_recognizable(self):
        data = [
            ["male", 'tester', 'https://www.youtube.com/watch?v=sfgsft']
        ]
        scraped_data = pd.DataFrame(data, columns=['gender_of_speaker', 'name', 'url'])

        def method_to_call():
            check_dataframe_validity(scraped_data)

        self.assertRaises(KeyError, method_to_call)

    @patch('data_acquisition_framework.services.youtube_util.file_speaker_gender_column', 'gender')
    @patch('data_acquisition_framework.services.youtube_util.file_speaker_name_column', 'name')
    @patch('data_acquisition_framework.services.youtube_util.file_url_name_column', 'url')
    def test_create_channel_file_for_file_mode_with_channel_folder_present(self):
        if not os.path.exists(channels_path):
            os.system("mkdir " + channels_path)
        file_path = "test.csv"
        data = [
            ["male", 'tester', 'https://www.youtube.com/watch?v=sfgsft']
        ]
        scraped_data = pd.DataFrame(data, columns=['gender', 'name', 'url'])
        scraped_data.to_csv(file_path, index=False)

        expected_scraped_data = pd.DataFrame([["male", 'tester', 'sfgsft']],
                                             columns=['gender', 'name', 'url'])

        result = create_channel_file_for_file_mode(file_path, 'url')
        channel_file = os.listdir(channels_path)[0]

        self.assertTrue(expected_scraped_data.equals(result))
        self.assertEqual("test.txt", channel_file)

        with open(channels_path + channel_file, 'r') as f:
            result_data = f.read().rstrip()
            self.assertEqual('sfgsft', result_data)

        os.system("rm " + file_path)
        os.system("rm -rf " + channels_path)

    @patch('data_acquisition_framework.services.youtube_util.file_speaker_gender_column', 'gender')
    @patch('data_acquisition_framework.services.youtube_util.file_speaker_name_column', 'name')
    @patch('data_acquisition_framework.services.youtube_util.file_url_name_column', 'url')
    def test_create_channel_file_for_file_mode_with_channel_folder_not_present(self):
        if os.path.exists(channels_path):
            os.system("rm -rf " + channels_path)
        file_path = "test.csv"
        data = [
            ["male", 'tester', 'https://www.youtube.com/watch?v=sfgsft']
        ]
        scraped_data = pd.DataFrame(data, columns=['gender', 'name', 'url'])
        scraped_data.to_csv(file_path, index=False)

        expected_scraped_data = pd.DataFrame([["male", 'tester', 'sfgsft']],
                                             columns=['gender', 'name', 'url'])

        result = create_channel_file_for_file_mode(file_path, 'url')
        channel_file = os.listdir(channels_path)[0]

        self.assertTrue(expected_scraped_data.equals(result))
        self.assertEqual("test.txt", channel_file)

        with open(channels_path + channel_file, 'r') as f:
            result_data = f.read().rstrip()
            self.assertEqual('sfgsft', result_data)

        os.system("rm " + file_path)
        os.system("rm -rf " + channels_path)

    @patch('data_acquisition_framework.services.youtube_util.file_speaker_name_column', 'name')
    @patch('data_acquisition_framework.services.youtube_util.file_url_name_column', 'url')
    def test_get_speaker(self):
        data = [
            ["male", 'tester', 'sfgsft']
        ]
        scraped_data = pd.DataFrame(data, columns=['gender', 'name', 'url'])
        video_id = "sfgsft"
        expected = "tester"

        result = get_speaker(scraped_data, video_id)

        self.assertEqual(expected, result)

    def test_get_speaker_raise_key_error(self):
        data = [
            ["male", 'tester', 'sfgsft']
        ]
        scraped_data = pd.DataFrame(data, columns=['gender', 'name', 'url'])
        video_id = "sfgsft"

        with self.assertRaises(KeyError):
            get_speaker(scraped_data, video_id)

    @patch('data_acquisition_framework.services.youtube_util.file_speaker_name_column', 'name')
    @patch('data_acquisition_framework.services.youtube_util.file_url_name_column', 'url')
    def test_get_speaker_return_empty_for_video_id_not_found(self):
        data = [
            ["male", 'tester', 'sfgssft']
        ]
        scraped_data = pd.DataFrame(data, columns=['gender', 'name', 'url'])
        video_id = "sfgsft"

        result = get_speaker(scraped_data, video_id)

        self.assertEqual("", result)

    @patch('data_acquisition_framework.services.youtube_util.file_speaker_gender_column', 'gender')
    @patch('data_acquisition_framework.services.youtube_util.file_url_name_column', 'url')
    def test_get_gender(self):
        data = [
            ["male", 'tester', 'sfgsft']
        ]
        scraped_data = pd.DataFrame(data, columns=['gender', 'name', 'url'])
        video_id = "sfgsft"
        expected = "male"

        result = get_gender(scraped_data, video_id)

        self.assertEqual(expected, result)

    def test_get_gender_raise_key_error(self):
        data = [
            ["male", 'tester', 'sfgsft']
        ]
        scraped_data = pd.DataFrame(data, columns=['gender', 'name', 'url'])
        video_id = "sfgsft"

        with self.assertRaises(KeyError):
            get_gender(scraped_data, video_id)

    @patch('data_acquisition_framework.services.youtube_util.file_speaker_gender_column', 'gender')
    @patch('data_acquisition_framework.services.youtube_util.file_url_name_column', 'url')
    def test_get_gender_return_empty_for_video_id_not_found(self):
        data = [
            ["male", 'tester', 'sfgssft']
        ]
        scraped_data = pd.DataFrame(data, columns=['gender', 'name', 'url'])
        video_id = "sfgsft"

        result = get_gender(scraped_data, video_id)

        self.assertEqual("", result)

    @patch('data_acquisition_framework.services.youtube_util.batch_num', 2)
    def test_get_video_batch_read_success(self):
        if not os.path.exists(channels_path):
            os.system("mkdir " + channels_path)
        if not os.path.exists(archives_base_path):
            os.system("mkdir " + archives_base_path)
        channel_name = "test"
        os.system("mkdir " + archives_base_path + channel_name)
        os.system("touch " + archives_base_path + channel_name + "/archive.txt")
        channel_file_path = channels_path + channel_name + ".txt"
        video_ids = ['dsfasdfsdf', 'sdfdsfdsf', 'adfsafsdf', 'adfasfdf']
        with open(channel_file_path, 'w') as f:
            f.writelines("{0}\n".format(video_id) for video_id in video_ids)

        videos = get_video_batch(channel_name, channel_name + ".txt")
        self.assertEqual(['dsfasdfsdf', 'sdfdsfdsf'], videos)

        with open(archives_base_path + channel_name + "/archive.txt", 'w') as f:
            f.writelines("youtube {0}\n".format(video_id) for video_id in ['dsfasdfsdf', 'sdfdsfdsf'])

        videos = get_video_batch(channel_name, channel_name + ".txt")
        self.assertEqual(['adfsafsdf', 'adfasfdf'], videos)

        os.system("rm -rf " + channels_path)
        os.system("rm -rf " + archives_base_path)

    @patch('data_acquisition_framework.services.youtube_util.batch_num', 2)
    def test_get_video_batch_read_archive_not_present(self):
        if not os.path.exists(channels_path):
            os.system("mkdir " + channels_path)
        if os.path.exists(archives_base_path):
            os.system("rm -rf " + archives_base_path)
        channel_name = "test"
        channel_file_path = channels_path + channel_name + ".txt"
        video_ids = ['dsfasdfsdf', 'sdfdsfdsf', 'adfsafsdf', 'adfasfdf']
        with open(channel_file_path, 'w') as f:
            f.writelines("{0}\n".format(video_id) for video_id in video_ids)

        with self.assertRaises(FileNotFoundError):
            get_video_batch(channel_name, channel_name + ".txt")

        os.system("rm -rf " + channels_path)
        os.system("rm -rf " + archives_base_path)

    @patch('data_acquisition_framework.services.youtube_util.batch_num', 2)
    def test_get_video_batch_read_channel_file_not_present(self):
        if os.path.exists(channels_path):
            os.system("rm -rf"+channels_path)

        channel_name = "test"

        with self.assertRaises(FileNotFoundError):
            get_video_batch(channel_name, channel_name + ".txt")

        os.system("rm -rf " + channels_path)
        os.system("rm -rf " + archives_base_path)
