from subprocess import CompletedProcess
from unittest import TestCase
from unittest.mock import patch

from data_acquisition_framework.services.youtube.youtube_dl import YoutubeDL


class TestYoutubeDL(TestCase):
    def setUp(self):
        self.youtube_dl_service = YoutubeDL()

    def test_init(self):
        self.assertEqual('youtube-dl', self.youtube_dl_service.youtube_call)

    def test_get_videos(self):  # Not working
        test_channel_url = 'https://youtube.com/channel/abcd'
        expected_video_list = ['']
        actual_video_list = self.youtube_dl_service.get_videos(test_channel_url, '', '')
        self.assertEqual(expected_video_list, actual_video_list)

    @patch('data_acquisition_framework.services.youtube.youtube_dl.subprocess')
    def test_youtube_download(self, mock_subprocess):
        test_archive_path = '/archive.txt'
        test_download_path = '/downloads'
        test_video_id = 'testid'
        test_output = ""
        mock_subprocess.run.return_value = test_output
        with patch.object(self.youtube_dl_service, 'check_and_log_download_output') as mock_check_and_log:
            self.youtube_dl_service.youtube_download(test_video_id, test_archive_path, test_download_path)

            mock_subprocess.run.assert_called_once()
            mock_check_and_log.assert_called_once_with(test_output)

    def test_check_and_log_without_error(self):
        test_output = CompletedProcess(args='', returncode=1, stdout=b'Download success')
        flag = self.youtube_dl_service.check_and_log_download_output(test_output)
        self.assertFalse(flag)

    def test_check_and_log_raises_exit(self):
        test_output = CompletedProcess(args='', returncode=1, stdout=b'',
                                       stderr=b'ERROR:": HTTP Error 429"\n')
        with self.assertRaises(SystemExit):
            self.youtube_dl_service.check_and_log_download_output(test_output)

    def test_check_and_log_with_yt_errors(self):
        test_output = CompletedProcess(args='', returncode=1, stdout=b'',
                                       stderr=b'HTTP Error 404: Not Found"\n')
        flag = self.youtube_dl_service.check_and_log_download_output(test_output)
        self.assertTrue(flag)

    def test_check_and_log_with_other_errors(self):
        test_output = CompletedProcess(args='', returncode=1, stdout=b'',
                                       stderr=b'ERROR:Incomplete YouTube ID testid.\n')
        flag = self.youtube_dl_service.check_and_log_download_output(test_output)
        self.assertTrue(flag)
