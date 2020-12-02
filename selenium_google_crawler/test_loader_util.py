import json
import os
from unittest import TestCase
from unittest.mock import mock_open, patch

from loader_util import read_config, read_archive


class TestLoadConfig(TestCase):
    def test_valid_json(self):
        read_data = json.dumps({'a': 1, 'b': 2, 'c': 3})
        mock_open_method = mock_open(read_data=read_data)
        test_filename = 'test.json'

        with patch('builtins.open', mock_open_method):
            result = read_config(test_filename)

        self.assertEqual({'a': 1, 'b': 2, 'c': 3}, result)

    def test_invalid_json(self):
        read_data = ''
        mock_open_method = mock_open(read_data=read_data)
        with patch("builtins.open", mock_open_method):
            with self.assertRaises(ValueError):
                read_config('filename')

    def test_file_not_exists(self):
        with self.assertRaises(IOError) as context:
            read_config('null')

        self.assertEqual(
            'file null does not exist.', str(context.exception))


class TestReadArchive(TestCase):
    @patch('loader_util.os')
    def test_file_not_exists(self, mock_os):
        mock_os.path.exists.return_value = False

        self.assertEqual([], read_archive())

    def test_file_exists(self):
        urls = ['abcd', 'defgh']
        with open('archive.txt', 'w') as f:
            f.writelines(url+"\n" for url in urls)

        result = read_archive()

        self.assertEqual(urls, result)
        os.system('rm archive.txt')
