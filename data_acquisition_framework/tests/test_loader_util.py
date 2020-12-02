import json
from unittest import TestCase
from unittest.mock import mock_open, patch

from data_acquisition_framework.services.loader_util import load_config_file


class TestLoadConfig(TestCase):
    def test_valid_json(self):
        read_data = json.dumps({'a': 1, 'b': 2, 'c': 3})
        mock_open_method = mock_open(read_data=read_data)
        test_filename = 'test.json'

        with patch('builtins.open', mock_open_method):
            result = load_config_file(test_filename)

        self.assertEqual({'a': 1, 'b': 2, 'c': 3}, result)

    def test_invalid_json(self):
        read_data = ''
        mock_open_method = mock_open(read_data=read_data)
        with patch("builtins.open", mock_open_method):
            with self.assertRaises(ValueError):
                load_config_file('filename')

    def test_file_not_exists(self):
        with self.assertRaises(IOError) as context:
            load_config_file('null')

        self.assertEqual(
            'file null does not exist.', str(context.exception))
