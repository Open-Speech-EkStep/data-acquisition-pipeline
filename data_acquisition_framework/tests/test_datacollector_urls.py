from concurrent import futures
from unittest import TestCase
from unittest.mock import MagicMock, patch, call

import scrapy

from data_acquisition_framework.items import LicenseItem, Media
from data_acquisition_framework.spiders.datacollector_urls import UrlSearchSpider


class TestUrlSearchSpider(TestCase):

    @patch('data_acquisition_framework.spiders.datacollector_urls.StorageUtil')
    @patch('data_acquisition_framework.spiders.datacollector_urls.load_config_file')
    def setUp(self, mock_load_config_file, mock_storage_util):
        self.mock_config = {
            "language": "Gujarati",
            "language_code": "gu",
            "keywords": [
                "songs download",
                "kavita download"
            ],
            "word_to_ignore": [],
            "extensions_to_ignore": [],
            "extensions_to_include": [],
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

    def test_init(self):
        self.mock_storage_util.return_value.set_gcs_creds.assert_called_once_with("")
        self.assertEqual(0, self.data_collector_urls.total_duration_in_seconds)
        self.assertEqual(self.mock_config['language'], self.data_collector_urls.language)
        self.assertEqual(self.mock_config['language_code'], self.data_collector_urls.language_code)
        self.assertEqual(self.mock_config['max_hours'] * 3600, self.data_collector_urls.max_seconds)
        self.assertEqual(self.mock_config['depth'], self.data_collector_urls.depth)
        self.assertEqual(self.mock_config['extensions_to_include'], self.data_collector_urls.extensions_to_include)
        self.assertEqual(self.mock_config['extensions_to_ignore'], self.data_collector_urls.extensions_to_ignore)
        self.assertEqual(self.mock_config['word_to_ignore'], self.data_collector_urls.word_to_ignore)
        self.assertFalse(self.data_collector_urls.enable_hours_restriction)

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

    def test_parse_with_hours_restriction_enabled_and_crossed_limit(self):
        self.data_collector_urls.enable_hours_restriction = True
        self.data_collector_urls.total_duration_in_seconds = 2000
        self.data_collector_urls.max_seconds = 20

        with patch.object(self.data_collector_urls, 'extract_media_urls') as mock_extract_media_urls:
            self.data_collector_urls.parse(None, 1)
            mock_extract_media_urls.assert_not_called()

    def test_extract_source_domain_with_www(self):
        domain = "test.com"
        base_url = "https://%s/test.html" % domain

        source_domain = self.data_collector_urls.extract_source_domain(base_url)

        self.assertEqual(domain, source_domain)

    def test_extract_source_domain_without_www(self):
        domain = "test.com"
        base_url = "https://www.%s/test.html" % domain

        source_domain = self.data_collector_urls.extract_source_domain(base_url)

        self.assertEqual(domain, source_domain)

    def test_extract_media_urls(self):
        response = MagicMock()
        a_tag_urls = ["https://test.com/1.mp4", "https://test.com/2.mp4"]
        source_urls = ["https://test.com/4.mp4", "https://test.com/5.mp4"]
        audio_tag_urls = ["https://test.com/6.mp4", "https://test.com/6.mp4"]
        expected_urls = a_tag_urls + source_urls + audio_tag_urls
        css_return_mock = MagicMock()
        response.css.return_value = css_return_mock
        css_return_mock.getall.side_effect = [a_tag_urls, source_urls, audio_tag_urls]

        a_urls, urls = self.data_collector_urls.extract_media_urls(response)
        pairs = [('a', 'href'), ('source', 'src'), ('audio', 'src')]
        call_strings = ["{}::attr({})".format(tag, attribute) for tag, attribute in pairs]
        calls = [call(call_strings[0]), call().getall(),
                 call(call_strings[1]), call().getall(),
                 call(call_strings[2]), call().getall()]
        response.css.assert_has_calls(calls)
        self.assertEqual(a_tag_urls, a_urls)
        self.assertEqual(expected_urls, urls)

    @patch('data_acquisition_framework.spiders.datacollector_urls.write')
    @patch('data_acquisition_framework.spiders.datacollector_urls.extract_license_urls')
    def test_parse_without_hours_restriction_with_depth_one(self, mock_extract_license_urls, write_mock):
        self.data_collector_urls.enable_hours_restriction = False
        self.data_collector_urls.depth = 1
        self.data_collector_urls.extensions_to_include = [".mp4"]
        self.data_collector_urls.extensions_to_ignore = [".pdf"]

        def url_join(url):
            return url

        response = MagicMock()
        response.url = "https://test.com/helloworld"
        response.xpath.return_value = []
        response.urljoin.side_effect = url_join
        license_urls = ["https://test.com/license.html"]
        mock_extract_license_urls.return_value = license_urls
        urls = [
            "https://test.com/a.mp4",
            "https://test.com/a.pdf",
            "mail://test",
            "https://test.com/a.html"
        ]

        with patch.object(self.data_collector_urls, 'extract_media_urls') as mock_extract_media_urls:
            with patch.object(self.data_collector_urls, 'extract_source_domain') as mock_extract_source_domain:
                with patch.object(self.data_collector_urls, 'extract_license') as mock_extract_license:
                    license_item = LicenseItem(file_urls=license_urls, key_name="creativecommons", source="test.com",
                                               language=self.data_collector_urls.language)
                    mock_extract_license.return_value = [license_item]
                    mock_extract_media_urls.return_value = [], urls
                    mock_extract_source_domain.return_value = "test.com"

                    results = self.data_collector_urls.parse(response, 1)

                    count = 0
                    for result in results:
                        if count == 0:
                            self.assertTrue(isinstance(result, LicenseItem))
                            self.assertEqual(license_urls, result['file_urls'])
                            self.assertEqual('creativecommons', result['key_name'])
                            self.assertEqual('Gujarati', result['language'])
                            self.assertEqual('test.com', result['source'])
                        else:
                            self.assertTrue(isinstance(result, Media))
                            self.assertEqual(['https://test.com/a.mp4'], result['file_urls'])
                            self.assertEqual(license_urls, result['license_urls'])
                            self.assertEqual('Gujarati', result['language'])
                            self.assertEqual('test.com', result['source'])
                            self.assertEqual(response.url, result['source_url'])
                            self.assertEqual('a.mp4', result['title'])
                        count += 1

                    self.assertTrue(count == 2)

    @patch('data_acquisition_framework.spiders.datacollector_urls.write')
    @patch('data_acquisition_framework.spiders.datacollector_urls.extract_license_urls')
    def test_parse_without_hours_restriction_with_depth_two(self, mock_extract_license_urls, mock_write):
        self.data_collector_urls.enable_hours_restriction = False
        self.data_collector_urls.depth = 2
        self.data_collector_urls.extensions_to_include = [".mp4"]
        self.data_collector_urls.extensions_to_ignore = [".pdf"]

        def url_join(url):
            return url

        response = MagicMock()
        response.url = "https://test.com/helloworld"
        response.xpath.return_value = []
        response.urljoin.side_effect = url_join
        license_urls = ["https://test.com/license.html"]
        mock_extract_license_urls.return_value = license_urls
        urls = [
            "https://test.com/a.html",
            "https://test.com/a.pdf",
            "mail://test",
            "https://test.com/b.html"
        ]
        expected = [urls[0], urls[3]]

        with patch.object(self.data_collector_urls, 'extract_media_urls') as mock_extract_media_urls:
            with patch.object(self.data_collector_urls, 'extract_source_domain') as mock_extract_source_domain:
                with patch.object(self.data_collector_urls, 'extract_license') as mock_extract_license:
                    license_item = LicenseItem(file_urls=license_urls, key_name="creativecommons", source="test.com",
                                               language=self.data_collector_urls.language)
                    mock_extract_license.return_value = [license_item]
                    mock_extract_media_urls.return_value = [], urls
                    mock_extract_source_domain.return_value = "test.com"

                    results = self.data_collector_urls.parse(response, 1)

                    count = 0
                    for result in results:
                        if isinstance(result, LicenseItem) or isinstance(result, Media):
                            self.fail()

                        self.assertTrue(isinstance(result, scrapy.Request))
                        self.assertEqual(expected[count], result.url)
                        self.assertEqual(self.data_collector_urls.parse, result.callback)
                        self.assertEqual(dict(depth=2), result.cb_kwargs)

                        count += 1

                    self.assertTrue(count == 2)

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
