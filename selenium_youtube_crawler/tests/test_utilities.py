import os
from unittest import TestCase
from unittest.mock import patch

from selenium_youtube_crawler.utilities import read_playlist_from_file, read_playlist_from_youtube_api, \
    populate_local_archive, create_required_dirs_for_archive_if_not_present


class TestUtilities(TestCase):

    def test_read_playlist_from_file_folder_not_present(self):
        result = read_playlist_from_file("tester")

        self.assertEqual({}, result)

    def test_read_playlist_from_file_folder_present_with_no_files(self):
        os.system("mkdir channels")
        result = read_playlist_from_file("channels")

        self.assertEqual({}, result)
        os.system("rm -rf channels")

    def test_read_playlist_from_file_folder_present_with_files(self):
        os.system("mkdir channels")
        video_ids = ["asdfdsfds", "dsfadfsad"]
        with open("channels/test.txt", "w") as f:
            f.writelines(video_id + "\n" for video_id in video_ids)
        result = read_playlist_from_file("channels")

        self.assertEqual({'test': video_ids}, result)
        os.system("rm -rf channels")

    @patch('selenium_youtube_crawler.utilities.YoutubeApiUtils')
    def test_read_playlist_from_youtube_api(self, mock_youtube_api):
        dummy_data = {
            "https://www.youtube.com/channel/adfdsf": "Test1",
            "https://www.youtube.com/channel/safafsd": "Test2"
        }
        mock_youtube_api.return_value.get_channels.return_value = dummy_data

        result = read_playlist_from_youtube_api()

        self.assertEqual(dummy_data, result)
        mock_youtube_api.return_value.get_channels.assert_called_once()

    def test_populate_local_archive(self):
        os.system("mkdir archive")
        source = "test"
        os.system("mkdir archive/" + source)
        video_id = "sdflsjfsd"
        populate_local_archive(source, video_id)

        path = "archive/" + source + "/archive.txt"
        self.assertTrue(os.path.exists(path))
        with open(path, 'r') as f:
            words = f.read().rstrip()

        self.assertEqual("youtube " + video_id, words)

        os.system("rm -rf archive")

    def test_create_required_dirs_for_archive_if_not_present(self):
        source = "test"

        create_required_dirs_for_archive_if_not_present(source)

        self.assertTrue(os.path.exists("archive"))
        self.assertTrue(os.path.exists("archive/"+source))

        os.system("rm -rf archive")
