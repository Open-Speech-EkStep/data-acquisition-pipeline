from unittest import TestCase
from unittest.mock import patch, MagicMock, call

import scrapy

from data_acquisition_framework.items import LicenseItem, Media
from data_acquisition_framework.spiders.datacollector_bing import BingSearchSpider


class TestBingSearchSpider(TestCase):

    @patch('data_acquisition_framework.spiders.datacollector_bing.StorageUtil')
    @patch('data_acquisition_framework.spiders.datacollector_bing.load_config_file')
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
        self.data_collector_bing = BingSearchSpider(my_setting="")

    def test_init(self):
        self.mock_storage_util.return_value.set_gcs_creds.assert_called_once_with("")
        self.assertEqual(0, self.data_collector_bing.total_duration_in_seconds)
        self.assertEqual(self.mock_config, self.data_collector_bing.config)
        self.assertEqual(self.mock_config['language'], self.data_collector_bing.language)
        self.assertEqual(self.mock_config['language_code'], self.data_collector_bing.language_code)
        self.assertEqual(self.mock_config['max_hours'] * 3600, self.data_collector_bing.max_seconds)
        self.assertEqual(self.mock_config['max_hours'], self.data_collector_bing.max_hours)
        self.assertEqual(self.mock_config['depth'], self.data_collector_bing.depth)
        self.assertEqual(self.mock_config['pages'], self.data_collector_bing.pages)
        self.assertEqual(self.mock_config['extensions_to_include'], self.data_collector_bing.extensions_to_include)
        self.assertEqual(self.mock_config['extensions_to_ignore'], self.data_collector_bing.extensions_to_ignore)
        self.assertEqual(self.mock_config['word_to_ignore'], self.data_collector_bing.word_to_ignore)
        self.assertFalse(self.data_collector_bing.is_continued)
        self.assertFalse(self.data_collector_bing.enable_hours_restriction)

    def test_item_scraped_if_item_is_none(self):
        self.data_collector_bing.item_scraped(None, None, self.data_collector_bing)

    def test_item_scraped_if_item_has_duration_field(self):
        self.assertEqual(self.data_collector_bing.total_duration_in_seconds, 0)
        self.data_collector_bing.item_scraped({'duration': 10}, None, self.data_collector_bing)
        self.assertEqual(self.data_collector_bing.total_duration_in_seconds, 10)

    @patch('data_acquisition_framework.spiders.datacollector_bing.json.dump')
    @patch('data_acquisition_framework.spiders.datacollector_bing.open')
    def test_start_requests_with_no_continued(self, mock_open, mock_json_dump):
        mock_open_file = mock_open.return_value.__enter__.return_value

        keywords = ["news", "talks"]
        language = "tamil"
        self.data_collector_bing.is_continued = False
        self.data_collector_bing.config["keywords"] = keywords
        self.data_collector_bing.language = language
        keywords = [language + "+" + keyword.replace(" ", "+") for keyword in keywords]
        urls = ["https://www.bing.com/search?q={0}".format(keyword) for keyword in keywords]

        results = self.data_collector_bing.start_requests()

        count = 0
        for result in results:
            self.assertTrue(isinstance(result, scrapy.Request))
            self.assertEqual(urls[count], result.url)
            self.assertEqual(self.data_collector_bing.parse_search_page, result.callback)
            self.assertEqual(dict(page_number=1, keyword=keywords[count]), result.cb_kwargs)
            count += 1

        self.assertTrue(count > 0)
        mock_open.assert_called_once_with(self.data_collector_bing.web_crawl_config, 'w')
        mock_json_dump.assert_called_once_with(self.data_collector_bing.config, mock_open_file, indent=4)

    @patch('data_acquisition_framework.spiders.datacollector_bing.json.dump')
    @patch('data_acquisition_framework.spiders.datacollector_bing.open')
    def test_start_requests_with_continued(self, mock_open, mock_json_dump):
        mock_open_file = mock_open.return_value.__enter__.return_value
        start_page = 20
        keywords = ["news", "talks"]
        language = "tamil"
        self.data_collector_bing.is_continued = True
        self.data_collector_bing.config["last_visited"] = 20
        self.data_collector_bing.config["keywords"] = keywords
        self.data_collector_bing.language = language
        keywords = [language + "+" + keyword.replace(" ", "+") for keyword in keywords]
        urls = ["https://www.bing.com/search?q={0}&first={1}".format(keyword, start_page) for keyword in keywords]

        results = self.data_collector_bing.start_requests()

        count = 0
        for result in results:
            self.assertTrue(isinstance(result, scrapy.Request))
            self.assertEqual(urls[count], result.url)
            self.assertEqual(self.data_collector_bing.parse_search_page, result.callback)
            self.assertEqual(dict(page_number=1, keyword=keywords[count]), result.cb_kwargs)
            count += 1

        self.assertTrue(count == 2)
        mock_open.assert_called_once_with(self.data_collector_bing.web_crawl_config, 'w')
        mock_json_dump.assert_called_once_with(self.data_collector_bing.config, mock_open_file, indent=4)

    @patch('data_acquisition_framework.spiders.datacollector_bing.write')
    def test_parse_search_page(self, write_mock):
        url = "http://bing.com/q=audios"
        urls = [
            "https://test.com/audios",
            "mail://test.com",
            "http://test2.com/audios",
            "https://go.microsoft.com/test",
            "https://microsofttranslator.com/test"
        ]

        css_return = MagicMock()
        response = MagicMock()
        response.url = url
        response.css.return_value = css_return
        css_return.getall.return_value = urls
        keyword = "tamil+news"
        self.data_collector_bing.pages = 2
        expected = [urls[0], urls[2]]

        with patch.object(self.data_collector_bing, 'filter_unwanted_urls') as mock_filter:
            mock_filter.return_value = expected

            results = self.data_collector_bing.parse_search_page(response, 1, keyword)

            count = 0
            for result in results:
                self.assertTrue(isinstance(result, scrapy.Request))
                if count == 0 or count == 1:
                    self.assertEqual(expected[count], result.url)
                    self.assertEqual(self.data_collector_bing.parse, result.callback)
                    self.assertEqual(dict(depth=1), result.cb_kwargs)
                else:
                    self.assertEqual("https://www.bing.com/search?q={0}&first={1}".format(keyword, 10), result.url)
                    self.assertEqual(self.data_collector_bing.parse_search_page, result.callback)
                    self.assertEqual(dict(page_number=2, keyword=keyword), result.cb_kwargs)
                count += 1

            mock_filter.assert_called_once_with(response, urls)
            response.css.assert_called_once_with('a::attr(href)')
            css_return.getall.assert_called_once()

        self.assertTrue(count == 3)

    def test_get_request_for_search_result(self):
        url = "http://test.com/test.mp4"

        result = self.data_collector_bing.get_request_for_search_result(url)

        self.assertTrue(isinstance(result, scrapy.Request))
        self.assertEqual(url, result.url)
        self.assertEqual(self.data_collector_bing.parse, result.callback)
        self.assertEqual(dict(depth=1), result.cb_kwargs)

    @patch('data_acquisition_framework.spiders.datacollector_bing.write')
    def test_filter_unwanted_urls(self, mock_write):
        url = "http://bing.com/q=audios"
        urls = [
            "https://test.com/audios",
            "mail://test.com",
            "http://test2.com/audios",
            "https://go.microsoft.com/test",
            "https://microsofttranslator.com/test"
        ]
        expected = [urls[0], urls[2]]
        attrs = {'url': url}
        response = MagicMock(**attrs)

        filtered_urls = self.data_collector_bing.filter_unwanted_urls(response, urls)

        mock_write.assert_has_calls(
            [call('base_url.txt', url),
             call('results.txt', "%s results = [\n" % url),
             call('results.txt', urls[0] + "\n"),
             call('results.txt', urls[2] + "\n"),
             call("results.txt", "\n]\n")])
        self.assertEqual(expected, filtered_urls)

    def test_parse_with_hours_restriction_enabled_and_crossed_limit(self):
        self.data_collector_bing.enable_hours_restriction = True
        self.data_collector_bing.total_duration_in_seconds = 2000
        self.data_collector_bing.max_seconds = 20

        with patch.object(self.data_collector_bing, 'extract_media_urls') as mock_extract_media_urls:
            self.data_collector_bing.parse(None, 1)
            mock_extract_media_urls.assert_not_called()

    def test_extract_source_domain_with_www(self):
        domain = "test.com"
        base_url = "https://%s/test.html" % domain

        source_domain = self.data_collector_bing.extract_source_domain(base_url)

        self.assertEqual(domain, source_domain)

    def test_extract_source_domain_without_www(self):
        domain = "test.com"
        base_url = "https://www.%s/test.html" % domain

        source_domain = self.data_collector_bing.extract_source_domain(base_url)

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

        a_urls, urls = self.data_collector_bing.extract_media_urls(response)
        pairs = [('a', 'href'), ('source', 'src'), ('audio', 'src')]
        call_strings = ["{}::attr({})".format(tag, attribute) for tag, attribute in pairs]
        calls = [call(call_strings[0]), call().getall(),
                 call(call_strings[1]), call().getall(),
                 call(call_strings[2]), call().getall()]
        response.css.assert_has_calls(calls)
        self.assertEqual(a_tag_urls, a_urls)
        self.assertEqual(expected_urls, urls)

    @patch('data_acquisition_framework.spiders.datacollector_bing.extract_license_urls')
    def test_parse_without_hours_restriction_with_depth_one(self, mock_extract_license_urls):
        self.data_collector_bing.enable_hours_restriction = False
        self.data_collector_bing.depth = 1
        self.data_collector_bing.extensions_to_include = [".mp4"]
        self.data_collector_bing.extensions_to_ignore = [".pdf"]

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

        with patch.object(self.data_collector_bing, 'extract_media_urls') as mock_extract_media_urls:
            with patch.object(self.data_collector_bing, 'extract_source_domain') as mock_extract_source_domain:
                with patch.object(self.data_collector_bing, 'extract_license') as mock_extract_license:
                    license_item = LicenseItem(file_urls=license_urls, key_name="creativecommons", source="test.com",
                                               language=self.data_collector_bing.language)
                    mock_extract_license.return_value = [license_item]
                    mock_extract_media_urls.return_value = [], urls
                    mock_extract_source_domain.return_value = "test.com"

                    results = self.data_collector_bing.parse(response, 1)

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

    @patch('data_acquisition_framework.spiders.datacollector_bing.extract_license_urls')
    def test_parse_without_hours_restriction_with_depth_two(self, mock_extract_license_urls):
        self.data_collector_bing.enable_hours_restriction = False
        self.data_collector_bing.depth = 2
        self.data_collector_bing.extensions_to_include = [".mp4"]
        self.data_collector_bing.extensions_to_ignore = [".pdf"]

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

        with patch.object(self.data_collector_bing, 'extract_media_urls') as mock_extract_media_urls:
            with patch.object(self.data_collector_bing, 'extract_source_domain') as mock_extract_source_domain:
                with patch.object(self.data_collector_bing, 'extract_license') as mock_extract_license:
                    license_item = LicenseItem(file_urls=license_urls, key_name="creativecommons", source="test.com",
                                               language=self.data_collector_bing.language)
                    mock_extract_license.return_value = [license_item]
                    mock_extract_media_urls.return_value = [], urls
                    mock_extract_source_domain.return_value = "test.com"

                    results = self.data_collector_bing.parse(response, 1)

                    count = 0
                    for result in results:
                        if isinstance(result, LicenseItem) or isinstance(result, Media):
                            self.fail()

                        self.assertTrue(isinstance(result, scrapy.Request))
                        self.assertEqual(expected[count], result.url)
                        self.assertEqual(self.data_collector_bing.parse, result.callback)
                        self.assertEqual(dict(depth=2), result.cb_kwargs)

                        count += 1

                    self.assertTrue(count == 2)

    def test_extract_license_for_creative_common(self):
        url = 'https://creativecommons.org/v2/license'
        license_urls = [url]
        source_domain = "test.com"
        language = "Gujarati"

        results = self.data_collector_bing.extract_license(license_urls, source_domain)

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

        results = self.data_collector_bing.extract_license(license_urls, source_domain)

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

        results = self.data_collector_bing.extract_license(license_urls, source_domain)

        for result in results:
            self.assertTrue(isinstance(result, scrapy.Request))
            self.assertEqual(url, result.url)
            self.assertEqual(self.data_collector_bing.parse_license, result.callback)
            self.assertEqual(dict(source=source_domain), result.cb_kwargs)

    def test_parse_license_for_creative_commons(self):
        attrs1 = {'extract.return_value': ["i don't know", 'creativecommons is here']}
        extract_mock = MagicMock(**attrs1)
        url = 'https://test.com/test.mp4'
        attrs = {'xpath.return_value': extract_mock, 'url': url}
        response = MagicMock(**attrs)
        source = "test"
        language = 'Gujarati'

        result = self.data_collector_bing.parse_license(response, source)
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

        result = self.data_collector_bing.parse_license(response, source)
        for item in result:
            self.assertEqual([], item["file_urls"])
            self.assertEqual('html_page', item["key_name"])
            self.assertEqual(language, item["language"])
            self.assertEqual(source, item["source"])
            self.assertEqual(content, item["content"])
