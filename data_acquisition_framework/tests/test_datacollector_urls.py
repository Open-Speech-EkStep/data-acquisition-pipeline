from concurrent import futures
from unittest import TestCase
from unittest.mock import MagicMock, patch, call

import scrapy

from data_acquisition_framework.items import LicenseItem
from data_acquisition_framework.spiders.datacollector_urls import UrlSearchSpider


class TestUrlSearchSpider(TestCase):

    @patch('data_acquisition_framework.spiders.datacollector_urls.StorageUtil')
    @patch('data_acquisition_framework.spiders.datacollector_bing.load_config_file')
    def setUp(self, mock_load_config_file, mock_storage_util):
        self.mock_config = {
            "language": "Gujarati",
            "language_code": "gu",
            "keywords": [
                "songs download",
                "kavita download"
            ],
            "word_to_ignore": [

            ],
            "extensions_to_ignore": [

            ],
            "extensions_to_include": [

            ],
            "pages": 10,
            "depth": 2,
            "continue_page": "NO",
            "last_visited": 100,
            "enable_hours_restriction": "NO",
            "max_hours": 1
        }
        mock_load_config_file.return_value = self.mock_config
        self.mock_storage_util = mock_storage_util
        self.data_collector_urls = UrlSearchSpider(my_setting="")

    def test_item_scraped_if_item_is_none(self):
        self.data_collector_urls.item_scraped(None, None, self.data_collector_urls)

    def test_item_scraped_if_item_has_duration_field(self):
        self.assertEqual(self.data_collector_urls.total_duration_in_seconds, 0)
        self.data_collector_urls.item_scraped({'duration': 10}, None, self.data_collector_urls)
        self.assertEqual(self.data_collector_urls.total_duration_in_seconds, 10)

    @patch('data_acquisition_framework.spiders.datacollector_urls.as_completed')
    @patch('data_acquisition_framework.spiders.datacollector_urls.concurrent.futures.ThreadPoolExecutor')
    @patch('data_acquisition_framework.spiders.datacollector_urls.open')
    def test_start_requests(self, mock_open, mock_thread_pool_executor, mock_as_completed):
        urls_path_open_mock = mock_open.return_value.__enter__.return_value
        mock_executor = mock_thread_pool_executor.return_value.__enter__.return_value

        result1 = MagicMock(spec=futures.Future)
        result1.result.return_value = None
        result2 = MagicMock(spec=futures.Future)
        result2.result.return_value = "hello"
        mock_executor.submit.side_effect = [result1, result2]

        urls = ["https://test.com.test.mp4", "https://test2.com/test2.mp4"]
        urls_path_open_mock.read.return_value = "\n".join(urls)

        mock_as_completed.return_value = [result1, result2]

        results = self.data_collector_urls.start_requests()
        count = 0
        for data in results:
            count += 1
            self.assertEqual("hello", data)
        self.assertEqual(1, count)
        urls_path_open_mock.read.assert_called_once()
        mock_open.assert_called_once()
        mock_executor.submit.assert_has_calls([call(self.data_collector_urls.parse_results_url, url) for url in urls])
        mock_as_completed.assert_called_once_with({result1: urls[0], result2: urls[1]})
        result1.result.assert_called_once()
        result2.result.assert_called_once()

    def test_parse_results_url(self):
        url = "http://test.com/test.mp4"

        result = self.data_collector_urls.parse_results_url(url)

        self.assertTrue(isinstance(result, scrapy.Request))
        self.assertEqual(url, result.url)
        self.assertEqual(self.data_collector_urls.parse, result.callback)
        self.assertEqual(dict(depth=1), result.cb_kwargs)

    def test_parse(self):
        self.fail()

    def test_extract_license_for_creative_common(self):
        url = 'https://creativecommons.org/v2/license'
        license_urls = [url]
        source_domain = "test.com"
        language = "Gujarati"

        results = self.data_collector_urls.extract_license(license_urls, source_domain)

        for result in results:
            self.assertTrue(isinstance(result, LicenseItem))
            self.assertEqual([url], result["file_urls"])
            self.assertEqual('creativecommons', result["key_name"])
            self.assertEqual(language, result["language"])
            self.assertEqual(source_domain, result["source"])

    def test_extract_license_for_document(self):
        url = 'https://test.com/test.doc'
        license_urls = [url]
        source_domain = "test.com"
        language = "Gujarati"

        results = self.data_collector_urls.extract_license(license_urls, source_domain)

        for result in results:
            self.assertTrue(isinstance(result, LicenseItem))
            self.assertEqual([url], result["file_urls"])
            self.assertEqual('document', result["key_name"])
            self.assertEqual(language, result["language"])
            self.assertEqual(source_domain, result["source"])

    def test_extract_license_for_non_document_and_non_creative_commons(self):
        url = 'https://test.com/test.html'
        license_urls = [url]
        source_domain = "test.com"

        results = self.data_collector_urls.extract_license(license_urls, source_domain)

        for result in results:
            self.assertTrue(isinstance(result, scrapy.Request))
            self.assertEqual(url, result.url)
            self.assertEqual(self.data_collector_urls.parse_license, result.callback)
            self.assertEqual(dict(source=source_domain), result.cb_kwargs)

    def test_parse_license_for_creative_commons(self):
        attrs1 = {'extract.return_value': ["i don't know", 'creativecommons is here']}
        extract_mock = MagicMock(**attrs1)
        url = 'https://test.com/test.mp4'
        attrs = {'xpath.return_value': extract_mock, 'url': url}
        response = MagicMock(**attrs)
        source = "test"
        language = 'Gujarati'

        result = self.data_collector_urls.parse_license(response, source)
        for item in result:
            self.assertEqual([url], item["file_urls"])
            self.assertEqual('creativecommons', item["key_name"])
            self.assertEqual(language, item["language"])
            self.assertEqual(source, item["source"])

    def test_parse_license_for_html_page(self):
        array_text = ["i don't know", 'nothing is here']
        attrs1 = {'extract.return_value': array_text}
        extract_mock = MagicMock(**attrs1)
        url = 'https://test.com/test.mp4'
        attrs = {'xpath.return_value': extract_mock, 'url': url}
        response = MagicMock(**attrs)
        source = "test"
        language = 'Gujarati'
        content = '\n'.join(array_text)

        result = self.data_collector_urls.parse_license(response, source)
        for item in result:
            self.assertEqual([], item["file_urls"])
            self.assertEqual('html_page', item["key_name"])
            self.assertEqual(language, item["language"])
            self.assertEqual(source, item["source"])
            self.assertEqual(content, item["content"])
