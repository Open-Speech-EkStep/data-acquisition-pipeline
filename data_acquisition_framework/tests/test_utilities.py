import os
import struct
import wave
import random
from unittest import TestCase
from unittest.mock import patch

from data_acquisition_framework.utilities import get_mp3_duration_in_seconds, get_license_info, get_file_format, \
    get_media_info, __get_duration_in_seconds


class TestUtilities(TestCase):
    def create_dummy_file(self, file_name):
        noise_output = wave.open(file_name, 'w')
        noise_output.setparams((2, 2, 44100, 0, 'NONE', 'not compressed'))
        for i in range(0, 88200):
            value = random.randint(-32767, 32767)
            packed_value = struct.pack('h', value)
            noise_output.writeframes(packed_value)
            noise_output.writeframes(packed_value)
        noise_output.close()

    def test_get_mp3_duration_in_seconds(self):
        wav = 'noise.wav'
        self.create_dummy_file(wav)
        result = get_mp3_duration_in_seconds(wav)
        self.assertEqual(2.0, result)
        os.remove(wav)

    def test_get_license_info_if_cc_not_present(self):
        test_license_urls = ['http://www.abcd.com', 'http://www.efgh.com']
        actual_license_info = get_license_info(test_license_urls)
        self.assertEqual(", ".join(test_license_urls), actual_license_info)

    def test_get_license_info_if_cc_present(self):
        test_license_urls = ['http://www.abcd.creativecommons', 'http://www.efgh.com']
        actual_license_info = get_license_info(test_license_urls)
        self.assertEqual('Creative Commons', actual_license_info)

    def test_get_file_format(self):
        test_file_name = 'test.mp4'
        actual_format = get_file_format(test_file_name)
        self.assertEqual('mp4', actual_format)

    @patch('data_acquisition_framework.utilities.editor')
    def test_get_media_info_for_mp4(self, mock_editor):
        test_file_name = 'test.mp4'
        source = "test"
        language = "test_language"
        source_url = 'test_url'
        license_urls = ['test_license_url']
        media_url = 'test_media_url'
        test_duration = 120.0
        mock_editor.VideoFileClip.return_value.duration = test_duration
        expected_media_info = {'duration': 2,
                               'raw_file_name': test_file_name,
                               'name': None,
                               'gender': None,
                               'source_url': media_url,
                               'license': get_license_info(license_urls),
                               "source": source,
                               "language": language,
                               'source_website': source_url}
        expected_result = (expected_media_info, 120)

        actual_media_info = get_media_info(test_file_name, source, language, source_url, license_urls, media_url)

        self.assertEqual(expected_result, actual_media_info)

    def test_get_media_info_for_wav(self):
        test_file_name = 'test.wav'
        self.create_dummy_file(test_file_name)
        source = "test"
        language = "test_language"
        source_url = 'test_url'
        license_urls = ['test_license_url']
        media_url = 'test_media_url'
        expected_media_info = {'duration': 0.033,
                               'raw_file_name': test_file_name,
                               'name': None,
                               'gender': None,
                               'source_url': media_url,
                               'license': get_license_info(license_urls),
                               "source": source,
                               "language": language,
                               'source_website': source_url}
        expected_result = (expected_media_info, 2)

        actual_media_info = get_media_info(test_file_name, source, language, source_url, license_urls, media_url)

        self.assertEqual(expected_result, actual_media_info)
        os.remove(test_file_name)
