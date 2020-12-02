from unittest import TestCase
from unittest.mock import patch

from selenium.webdriver.common.by import By

from selenium_youtube_crawler.browser_utils import BrowserUtils


class TestBrowserUtils(TestCase):

    @patch('selenium_youtube_crawler.browser_utils.Options')
    @patch('selenium_youtube_crawler.browser_utils.webdriver')
    def setUp(self, mock_web_driver, mock_options):
        self.browser_utils = BrowserUtils()
        self.mock_web_driver = mock_web_driver

    def test_get(self):
        url = "http://google.com"

        self.browser_utils.get(url)

        self.mock_web_driver.Firefox.return_value.get.assert_called_once_with(url)

    @patch('selenium_youtube_crawler.browser_utils.EC')
    @patch('selenium_youtube_crawler.browser_utils.WebDriverWait')
    def test_wait_by_class_name(self, mock_web_driver_wait, ec_mock):
        class_name = "tester"

        self.browser_utils.wait_by_class_name(class_name)

        mock_web_driver_wait.assert_called_once_with(self.mock_web_driver.Firefox.return_value, 30)
        mock_web_driver_wait.return_value.until.assert_called_once()
        ec_mock.presence_of_element_located.assert_called_once_with((By.CLASS_NAME, class_name))

    def test_find_element_by_id(self):
        id = "pnnext"
        element_result = 'Test Element'
        mock_find_element = self.mock_web_driver.Firefox.return_value.find_element_by_id
        mock_find_element.return_value = element_result

        result = self.browser_utils.find_element_by_id(id)

        mock_find_element.assert_called_once_with(id)
        self.assertEqual(element_result, result)

    def test_quit_with_no_exception(self):
        self.browser_utils.quit()

        self.mock_web_driver.Firefox.return_value.quit.assert_called_once()

    def test_quit_with_exception(self):
        def raise_error():
            raise OSError()

        self.mock_web_driver.Firefox.return_value.quit.side_effect = raise_error

        self.browser_utils.quit()
