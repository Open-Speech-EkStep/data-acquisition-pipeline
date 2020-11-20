import os
import random
import struct
import unittest
import wave
from unittest import TestCase
from unittest.mock import patch

from data_acquisition_framework.utilities import get_mp3_duration_in_seconds, get_license_info, get_file_format, \
    get_media_info, is_unwanted_words_present, is_unwanted_extension_present, \
    is_extension_present, sanitize, is_unwanted_wiki, write, get_meta_filename, is_url_start_with_cc, \
    is_license_terms_in_text


def create_dummy_file(file_name):
    noise_output = wave.open(file_name, 'w')
    noise_output.setparams((2, 2, 44100, 0, 'NONE', 'not compressed'))
    for _ in range(0, 88200):
        value = random.randint(-32767, 32767)
        packed_value = struct.pack('h', value)
        noise_output.writeframes(packed_value)
        noise_output.writeframes(packed_value)
    noise_output.close()


class TestUtilities(TestCase):
    def test_get_mp3_duration_in_seconds(self):
        wav = 'noise.wav'
        create_dummy_file(wav)
        result = get_mp3_duration_in_seconds(wav)
        self.assertEqual(2.0, result)
        os.remove(wav)

    def test_get_file_format(self):
        test_file_name = 'test.mp4'
        actual_format = get_file_format(test_file_name)
        self.assertEqual('mp4', actual_format)

    def test_sanitize(self):
        test_word = '  test word \n '
        self.assertEqual('test word', sanitize(test_word))

    def test_write(self):
        test_file_name = 'test.txt'
        test_content = 'sample content'

        write(test_file_name, test_content)

        with open(test_file_name, 'r') as f:
            self.assertEqual(test_content + '\n', f.read())
        os.remove(test_file_name)

    def test_get_meta_filename(self):
        test_file_name = 'test.txt'
        expected_meta_filename = 'test.csv'

        actual_meta_filename = get_meta_filename(test_file_name)

        self.assertEqual(expected_meta_filename, actual_meta_filename)


class TestGetLicenseInfo(TestCase):

    def test_get_license_info_if_cc_not_present(self):
        test_license_urls = ['http://www.abcd.com', 'http://www.efgh.com']
        actual_license_info = get_license_info(test_license_urls)
        self.assertEqual(", ".join(test_license_urls), actual_license_info)

    def test_get_license_info_if_cc_present(self):
        test_license_urls = ['http://www.abcd.creativecommons', 'http://www.efgh.com']
        actual_license_info = get_license_info(test_license_urls)
        self.assertEqual('Creative Commons', actual_license_info)


class TestGetMediaInfo(TestCase):
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
        create_dummy_file(test_file_name)
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


class TestUrlStartWithCC(TestCase):
    def test_is_url_start_with_cc_true(self):
        test_url = "https://creativecommons.org/publicdomain/mark/abcd"

        self.assertTrue(is_url_start_with_cc(test_url))

    def test_is_url_start_with_cc_false(self):
        test_url = "http://test_website/abcd/something"

        self.assertFalse(is_url_start_with_cc(test_url))


class TextLicenseTermsInText(TestCase):
    def test_is_license_terms_in_text_true(self):
        test_text = 'sample something license testing'

        self.assertTrue(is_license_terms_in_text(test_text))

    def test_is_license_terms_in_text_false(self):
        test_text = 'sample something testing nothing'

        self.assertFalse(is_license_terms_in_text(test_text))


class TestUnwantedWordsPresent(TestCase):
    def test_is_unwanted_words_present_if_true(self):
        test_words_ignore = ['word_one', 'word_two', 'word_three']
        test_url = 'http://test_website/word_one/something'

        self.assertTrue(is_unwanted_words_present(test_words_ignore, test_url))

    def test_is_unwanted_words_present_if_false(self):
        test_words_ignore = ['word_one', 'word_two', 'word_three']
        test_url = 'http://test_website/word_four/something'

        self.assertFalse(is_unwanted_words_present(test_words_ignore, test_url))


class TestUnwantedExtensionPresent(TestCase):
    def test_is_unwanted_extension_present_if_true(self):
        test_extensions_ignore = ['ext_one', 'ext_two', 'ext_three']
        test_url = 'http://test_website.ext_two'

        self.assertTrue(is_unwanted_extension_present(test_extensions_ignore, test_url))

    def test_is_unwanted_extension_present_if_false(self):
        test_extensions_ignore = ['ext_one', 'ext_two', 'ext_three']
        test_url = 'http://test_website.ext_four'

        self.assertFalse(is_unwanted_extension_present(test_extensions_ignore, test_url))


class TestExtensionPresent(TestCase):
    def test_is_extension_present_if_true(self):
        test_extensions_to_include = ['ext_one', 'ext_two', 'ext_three']
        test_url = 'http://test_website.ext_three'

        self.assertTrue(is_extension_present(test_extensions_to_include, test_url))

    def test_is_extension_present_if_false(self):
        test_extensions_to_include = ['ext_one', 'ext_two', 'ext_three']
        test_url = 'http://test_website.ext_four'

        self.assertFalse(is_extension_present(test_extensions_to_include, test_url))


class TestUnwantedWiki(TestCase):
    def test_is_unwanted_wiki_if_url_has_related_wiki(self):
        test_language_code = 'ab'
        test_url = 'http://wikipedia.org'

        self.assertFalse(is_unwanted_wiki(test_language_code, test_url))

    def test_is_unwanted_wiki_if_url_has_unrelated_wiki(self):
        test_language_code = 'ab'
        test_url = 'http://be.wikipedia.org'

        self.assertTrue(is_unwanted_wiki(test_language_code, test_url))

    def test_is_unwanted_wiki_if_url_has_no_wiki(self):
        test_language_code = 'ab'
        test_url = 'http://test_url.com'

        self.assertFalse(is_unwanted_wiki(test_language_code, test_url))


if __name__ == "__main__":
    unittest.main()
