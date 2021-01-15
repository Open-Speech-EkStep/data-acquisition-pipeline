from subprocess import CompletedProcess
from unittest import TestCase
from unittest.mock import patch, call

from data_acquisition_framework.services.youtube.youtube_dl_api import YoutubeDL


class TestYoutubeDL(TestCase):
    def setUp(self):
        self.youtube_dl_service = YoutubeDL()

    def test_init(self):
        self.assertEqual('youtube-dl', self.youtube_dl_service.youtube_call)

    def test_get_videos(self):  # Not working
        test_channel_url = 'https://youtube.com/channel/abcd'
        expected_video_list = ['']
        actual_video_list = self.youtube_dl_service.get_videos(test_channel_url)
        self.assertEqual(expected_video_list, actual_video_list)

    @patch('data_acquisition_framework.services.youtube.youtube_dl.subprocess')
    def test_youtube_download_with_no_retries(self, mock_subprocess):
        test_archive_path = '/archive.txt'
        test_download_path = '/downloads'
        test_video_id = 'testid'
        test_output = ""
        mock_subprocess.run.return_value = test_output
        with patch.object(self.youtube_dl_service, 'check_and_log_download_output') as mock_check_and_log:
            mock_check_and_log.return_value = False

            remove_video_flag, video_id = self.youtube_dl_service.youtube_download(test_video_id, test_archive_path,
                                                                                   test_download_path)
            self.assertFalse(remove_video_flag)
            self.assertEqual(test_video_id, video_id)
            mock_subprocess.run.assert_called_once()
            mock_check_and_log.assert_called_once_with(test_output)

    @patch('data_acquisition_framework.services.youtube.youtube_dl.subprocess')
    def test_youtube_download_with_retries(self, mock_subprocess):
        test_archive_path = '/archive.txt'
        test_download_path = '/downloads'
        test_video_id = 'testid'
        test_output = ""
        mock_subprocess.run.return_value = test_output
        with patch.object(self.youtube_dl_service, 'check_and_log_download_output') as mock_check_and_log:
            mock_check_and_log.return_value = True

            remove_video_flag, video_id = self.youtube_dl_service.youtube_download(test_video_id, test_archive_path,
                                                                                   test_download_path)
            self.assertTrue(remove_video_flag)
            self.assertEqual(test_video_id, video_id)
            self.assertEqual(4, mock_subprocess.run.call_count)
            mock_check_and_log.assert_has_calls([call(test_output), call(test_output), call(test_output),
                                                call(test_output)])

    def test_check_and_log_without_error(self):
        test_output = CompletedProcess(args='', returncode=1, stdout=b'Download success')
        flag = self.youtube_dl_service.check_and_log_download_output(test_output)
        self.assertFalse(flag)

    @patch('data_acquisition_framework.services.youtube.youtube_dl.logging')
    def test_check_and_log_raises_exit(self, mock_logging):
        test_output = CompletedProcess(args='', returncode=1, stdout=b'',
                                       stderr=b'ERROR:": HTTP Error 429"\n')
        with self.assertRaises(SystemExit):
            self.youtube_dl_service.check_and_log_download_output(test_output)

    @patch('data_acquisition_framework.services.youtube.youtube_dl.logging')
    def test_check_and_log_with_yt_errors(self, mock_logging):
        test_output = CompletedProcess(args='', returncode=1, stdout=b'',
                                       stderr=b'HTTP Error 404: Not Found"\n')
        flag = self.youtube_dl_service.check_and_log_download_output(test_output)
        self.assertTrue(flag)

    @patch('data_acquisition_framework.services.youtube.youtube_dl.logging')
    def test_check_and_log_with_other_errors(self, mock_logging):
        test_output = CompletedProcess(args='', returncode=1, stdout=b'',
                                       stderr=b'ERROR:Incomplete YouTube ID testid.\n')
        flag = self.youtube_dl_service.check_and_log_download_output(test_output)
        self.assertTrue(flag)
