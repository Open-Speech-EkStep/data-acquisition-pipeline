import os
from unittest import TestCase
from unittest.mock import patch, call, MagicMock

from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from crawl_google import GoogleCrawler


class TestGoogleCrawler(TestCase):

    @patch('crawl_google.read_config')
    @patch('crawl_google.read_archive')
    def setUp(self, mock_read_archive, mock_read_config):
        mock_read_archive.return_value = []
        mock_read_config.return_value = {
            "language": "gujarati",
            "language_code": "gu",
            "keywords": [

            ],
            "words_to_ignore": [

            ],
            "extensions_to_ignore": [

            ],
            "max_pages": 1,
            "headless": False
        }
        self.mock_read_archive = mock_read_archive
        self.mock_read_config = mock_read_config
        self.google_crawler = GoogleCrawler("config.json")

    def test_is_present_in_archive_with_url_present(self):
        url = "https://www.google.com/download/test.mp4"
        self.google_crawler.archive = [url]

        result = self.google_crawler.is_present_in_archive(url)

        self.assertTrue(result)

    def test_is_present_in_archive_with_url_not_present(self):
        url = "https://www.google.com/download/test.mp4"

        result = self.google_crawler.is_present_in_archive(url)

        self.assertFalse(result)

    def test_sanitize_with_(self):
        test_word = '  test word \n '
        self.assertEqual('test word', self.google_crawler.sanitize(test_word))

    def test_is_unwanted_present_with_match_found(self):
        url = "https://www.google.com/music/test.mp4"
        self.google_crawler.word_to_ignore = ["music"]

        result = self.google_crawler.is_unwanted_present(url)

        self.assertTrue(result)

    def test_is_unwanted_present_with_match_not_found(self):
        url = "https://www.google.com/download/test.mp4"
        self.google_crawler.word_to_ignore = ["music"]

        result = self.google_crawler.is_unwanted_present(url)

        self.assertFalse(result)

    def test_is_unwanted_extension_present_with_match_found(self):
        url = "https://www.google.com/music/test.pdf"
        self.google_crawler.extensions_to_ignore = [".pdf"]

        result = self.google_crawler.is_unwanted_extension_present(url)

        self.assertTrue(result)

    def test_is_unwanted_extension_present_with_match_not_found(self):
        url = "https://www.google.com/music/test.mp4"
        self.google_crawler.extensions_to_ignore = [".pdf"]

        result = self.google_crawler.is_unwanted_extension_present(url)

        self.assertFalse(result)

    def test_is_unwanted_wiki_with_different_wiki_domain(self):
        result = self.google_crawler.is_unwanted_wiki("https://wiki.org/test.mp4")

        self.assertTrue(result)

    def test_is_unwanted_wiki_starts_with_unknown_language_code(self):
        self.google_crawler.language_code = "ta"

        result = self.google_crawler.is_unwanted_wiki("https://af.wikipedia.org/test.mp4")

        self.assertTrue(result)

    def test_is_unwanted_wiki_starts_with_matched_language_code(self):
        self.google_crawler.language_code = "ta"

        result = self.google_crawler.is_unwanted_wiki("https://ta.wikipedia.org/test.mp4")

        self.assertFalse(result)

    def test_is_unwanted_wiki_starts_with_default_language_code(self):
        result = self.google_crawler.is_unwanted_wiki("https://en.wikipedia.org/test.mp4")

        self.assertFalse(result)

    @patch('crawl_google.webdriver.Firefox')
    @patch('crawl_google.EC')
    @patch('crawl_google.WebDriverWait')
    def test_extract_and_move_next_with_no_timeout(self, mock_web_driver_wait, mock_ec, mock_firefox):
        class_name = "yuRUbf"
        mock_find_by_class_name = mock_firefox.return_value.find_elements_by_class_name
        link_elements = ["hello", "hey"]
        mock_find_by_class_name.return_value = link_elements
        with patch.object(self.google_crawler, 'extract_links') as mock_extract_links:
            with patch.object(self.google_crawler, 'move_to_next_page') as mock_move_to_next_page:
                current_page = 1
                self.google_crawler.extract_and_move_next(mock_firefox.return_value, current_page)
                mock_extract_links.assert_called_once_with(link_elements, 'urls.txt')
                mock_move_to_next_page.assert_called_once_with(mock_firefox.return_value, current_page)

        mock_find_by_class_name.assert_called_once_with(class_name)
        mock_web_driver_wait.assert_called_once_with(mock_firefox.return_value, 20)
        mock_web_driver_wait.return_value.until.assert_called_once()
        mock_ec.presence_of_element_located.assert_called_once_with((By.CLASS_NAME, class_name))

    @patch('crawl_google.webdriver.Firefox')
    @patch('crawl_google.EC')
    @patch('crawl_google.WebDriverWait')
    def test_extract_and_move_next_with_timeout(self, mock_web_driver_wait, mock_ec, mock_firefox):
        class_name = "yuRUbf"

        def raise_exception(value): raise TimeoutException()

        mock_web_driver_wait.return_value.until.side_effect = raise_exception

        mock_find_by_class_name = mock_firefox.return_value.find_elements_by_class_name

        with patch.object(self.google_crawler, 'extract_links') as mock_extract_links:
            with patch.object(self.google_crawler, 'move_to_next_page') as mock_move_to_next_page:
                current_page = 1
                self.google_crawler.extract_and_move_next(mock_firefox.return_value, current_page)
                mock_extract_links.assert_not_called()
                mock_move_to_next_page.assert_not_called()

        mock_web_driver_wait.assert_called_once_with(mock_firefox.return_value, 20)
        mock_web_driver_wait.return_value.until.assert_called_once()
        mock_ec.presence_of_element_located.assert_called_once_with((By.CLASS_NAME, class_name))
        mock_find_by_class_name.assert_not_called()

    @patch('crawl_google.webdriver.Firefox')
    def test_crawl_with_no_keywords(self, mock_firefox_web_driver):
        mock_firefox_web_driver = mock_firefox_web_driver.return_value
        urls = []
        self.google_crawler.archive = urls

        archive_file_name = 'archives.txt'
        if os.path.exists(archive_file_name):
            os.system('rm '+archive_file_name)

        self.google_crawler.crawl(archive_file_name)

        mock_firefox_web_driver.maximize_window.assert_called_once()
        mock_firefox_web_driver.quit.assert_called_once()
        self.assertTrue(os.path.exists(archive_file_name))
        with open(archive_file_name, 'r') as f:
            results = f.read().splitlines()
            self.assertEqual(urls, results)
        os.system('rm ' + archive_file_name)

    @patch('crawl_google.webdriver.Firefox')
    def test_crawl_with_keywords(self, mock_firefox_web_driver):
        mock_firefox_web_driver = mock_firefox_web_driver.return_value
        urls = ['https://en.wikipedia.org/test.mp4']
        self.google_crawler.archive = urls
        language = "gujarati"
        self.google_crawler.language = language
        keywords = ["gujaratinews", "gujaratimovies"]
        self.google_crawler.keywords = keywords
        search_element_mock = mock_firefox_web_driver.find_element_by_class_name
        attrs1 = {'send_keys.return_value': ""}
        attrs2 = {'send_keys.return_value': ""}
        driver1 = MagicMock(**attrs1)
        driver2 = MagicMock(**attrs2)
        search_element_mock.side_effect = [driver1, driver2]
        archive_file_name = 'archive.txt'
        if os.path.exists(archive_file_name):
            os.system('rm '+archive_file_name)

        with patch.object(self.google_crawler, 'extract_and_move_next') as mock_extract:
            self.google_crawler.crawl(archive_file_name)
            mock_extract.assert_has_calls([call(mock_firefox_web_driver, 1), call(mock_firefox_web_driver, 1)])

        mock_firefox_web_driver.maximize_window.assert_called_once()
        mock_firefox_web_driver.get.assert_has_calls([call("https://www.google.com/"), call("https://www.google.com/")])
        search_element_mock.assert_has_calls([call("gLFyf.gsfi"), call("gLFyf.gsfi")])
        driver1.send_keys.assert_called_once_with(language + ' ' + keywords[0] + Keys.RETURN)
        driver2.send_keys.assert_called_once_with(language + ' ' + keywords[1] + Keys.RETURN)
        mock_firefox_web_driver.quit.assert_called_once()
        with open(archive_file_name, 'r') as f:
            results = f.read().splitlines()
            self.assertEqual(urls, results)

        os.system('rm ' + archive_file_name)

    @patch('crawl_google.webdriver.Firefox')
    def test_move_to_next_page_with_next_page_less_than_max_pages_can_proceed(self, mock_firefox):
        current_page = 1
        self.google_crawler.max_pages = 3
        mock_find_element_by_id = mock_firefox.return_value.find_element_by_id
        attrs1 = {'click.return_value': ""}
        next_btn = MagicMock(**attrs1)
        mock_find_element_by_id.return_value = next_btn

        with patch.object(self.google_crawler, 'extract_and_move_next') as mock_extract_and_move_next:
            self.google_crawler.move_to_next_page(mock_firefox.return_value, current_page)
            mock_extract_and_move_next.assert_called_once_with(mock_firefox.return_value, current_page + 1)

        mock_find_element_by_id.assert_called_once_with("pnnext")
        next_btn.click.assert_called_once()

    @patch('crawl_google.webdriver.Firefox')
    def test_move_to_next_page_with_next_page_equal_to_max_pages_should_not_proceed(self, mock_firefox):
        current_page = 1
        self.google_crawler.max_pages = 2
        mock_find_element_by_id = mock_firefox.return_value.find_element_by_id
        attrs1 = {'click.return_value': ""}
        next_btn = MagicMock(**attrs1)
        mock_find_element_by_id.return_value = next_btn

        with patch.object(self.google_crawler, 'extract_and_move_next') as mock_extract_and_move_next:
            self.google_crawler.move_to_next_page(mock_firefox.return_value, current_page)
            mock_extract_and_move_next.assert_not_called()

        mock_find_element_by_id.assert_called_once_with("pnnext")
        next_btn.click.assert_called_once()

    @patch('crawl_google.webdriver.Firefox')
    def test_move_to_next_page_with_next_page_greater_than_max_pages_should_not_proceed(self, mock_firefox):
        current_page = 2
        self.google_crawler.max_pages = 2
        mock_find_element_by_id = mock_firefox.return_value.find_element_by_id
        attrs1 = {'click.return_value': ""}
        next_btn = MagicMock(**attrs1)
        mock_find_element_by_id.return_value = next_btn

        with patch.object(self.google_crawler, 'extract_and_move_next') as mock_extract_and_move_next:
            self.google_crawler.move_to_next_page(mock_firefox.return_value, current_page)
            mock_extract_and_move_next.assert_not_called()

        mock_find_element_by_id.assert_called_once_with("pnnext")
        next_btn.click.assert_called_once()

    @patch('crawl_google.webdriver.Firefox')
    def test_move_to_next_page_with_not_found_exception_should_not_proceed(self, mock_firefox):
        current_page = 2
        self.google_crawler.max_pages = 2
        mock_find_element_by_id = mock_firefox.return_value.find_element_by_id

        def raise_exception(value): raise NoSuchElementException()

        mock_find_element_by_id.side_effect = raise_exception

        with patch.object(self.google_crawler, 'extract_and_move_next') as mock_extract_and_move_next:
            self.google_crawler.move_to_next_page(mock_firefox.return_value, current_page)
            mock_extract_and_move_next.assert_not_called()

        mock_find_element_by_id.assert_called_once_with("pnnext")

    def test_extract_links_with_link_ignored_by_filter(self):
        link = "https://www.test.com/test.mp4"
        attrs1 = {'get_attribute.return_value': link}
        a_element = MagicMock(**attrs1)
        attrs1 = {'find_element_by_tag_name.return_value': a_element}
        link_element = MagicMock(**attrs1)

        with patch.object(self.google_crawler, 'is_present_in_archive') as mock_is_present_in_archive:
            with patch.object(self.google_crawler, 'is_unwanted_present') as mock_is_unwanted_present:
                with patch.object(self.google_crawler,
                                  'is_unwanted_extension_present') as mock_is_unwanted_extension_present:
                    with patch.object(self.google_crawler, 'is_unwanted_wiki') as mock_is_unwanted_wiki:
                        mock_is_present_in_archive.return_value = True
                        mock_is_unwanted_present.return_value = True
                        mock_is_unwanted_extension_present.return_value = True
                        mock_is_unwanted_wiki.return_value = True
                        self.google_crawler.extract_links([link_element], 'url.txt')
                        mock_is_present_in_archive.assert_called_once_with(link)

        link_element.find_element_by_tag_name.assert_called_once_with("a")
        a_element.get_attribute.assert_called_once_with('href')

    def test_extract_links_with_link_not_ignored_by_filter(self):
        link = "https://www.test.com/test.mp4"
        attrs1 = {'get_attribute.return_value': link}
        a_element = MagicMock(**attrs1)
        attrs1 = {'find_element_by_tag_name.return_value': a_element}
        link_element = MagicMock(**attrs1)
        file_name = 'url.txt'
        if os.path.exists(file_name):
            os.system("rm " + file_name)

        with patch.object(self.google_crawler, 'is_present_in_archive') as mock_is_present_in_archive:
            with patch.object(self.google_crawler, 'is_unwanted_present') as mock_is_unwanted_present:
                with patch.object(self.google_crawler,
                                  'is_unwanted_extension_present') as mock_is_unwanted_extension_present:
                    with patch.object(self.google_crawler, 'is_unwanted_wiki') as mock_is_unwanted_wiki:
                        mock_is_present_in_archive.return_value = False
                        mock_is_unwanted_present.return_value = False
                        mock_is_unwanted_extension_present.return_value = False
                        mock_is_unwanted_wiki.return_value = False
                        self.google_crawler.extract_links([link_element], file_name)
                        mock_is_present_in_archive.assert_called_once_with(link)
                        mock_is_unwanted_present.assert_called_once_with(link)
                        mock_is_unwanted_extension_present.assert_called_once_with(link)
                        mock_is_unwanted_wiki.assert_called_once_with(link)

        link_element.find_element_by_tag_name.assert_called_once_with("a")
        a_element.get_attribute.assert_called_once_with('href')

        with open(file_name, 'r') as f:
            results = f.read().splitlines()
            self.assertEqual([link], results)

        os.system("rm " + file_name)
