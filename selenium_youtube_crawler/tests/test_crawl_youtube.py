from concurrent import futures
from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from selenium import webdriver

from selenium_youtube_crawler.crawl_youtube import YoutubeCrawler, CrawlInput


class TestYoutubeCrawler(TestCase):

    @patch('selenium_youtube_crawler.crawl_youtube.geckodriver_autoinstaller')
    @patch('selenium_youtube_crawler.crawl_youtube.ThreadPoolExecutor')
    @patch('selenium_youtube_crawler.crawl_youtube.set_gcs_credentials')
    def setUp(self, mock_set_gcs_credentials, mock_thread_pool_executor, mock_gecko):
        self.bucket_name = "test_bucket"
        self.bucket_path = "test"
        self.language = "english"
        self.mock_thread_pool_executor = mock_thread_pool_executor
        self.youtube_crawler = YoutubeCrawler(self.bucket_name, self.bucket_path, self.language)
        mock_set_gcs_credentials.assert_called_once()

    @patch('selenium_youtube_crawler.crawl_youtube.Downloader')
    def test_start_download(self, downloader_mock):
        download_url = "https://tester.com/test.mp4"
        video_id = "sdfs324"
        source = "test"

        self.youtube_crawler.start_download(download_url, video_id, source)

        downloader_mock.return_value.download(download_url, video_id, source)

    # incomplete test...have to test a tags loop as well
    @patch('selenium_youtube_crawler.crawl_youtube.BrowserUtils')
    def test_initiate_video_id_crawl_with_element_found_second(self, mock_browser_utils):
        video_id = "sdljfsdf"
        playlist_name = "test"
        redirect_url = "https://www.ssyoutube.com/watch?v=" + video_id
        download_url = "https://test.com/download?v=" + video_id
        attrs1 = {'get_attribute.return_value': "video format"}
        attrs2 = {'get_attribute.side_effect': ["video format: 360", download_url]}
        driver1 = MagicMock(**attrs1)
        driver2 = MagicMock(**attrs2)
        browser_utils_mock_obj = mock_browser_utils.return_value
        mock_find_element_by_id = browser_utils_mock_obj.find_element_by_id
        result_div_mock = MagicMock(spec=webdriver.Firefox).return_value
        result_div_mock.find_elements_by_tag_name.return_value = [driver1, driver2]
        mock_find_element_by_id.return_value = result_div_mock

        self.youtube_crawler.initiate_video_id_crawl(browser_utils_mock_obj, video_id, playlist_name)

        driver1.get_attribute.assert_called_once_with("title")
        driver2.get_attribute.assert_has_calls([call("title"), call("href")])
        browser_utils_mock_obj.get.assert_called_once_with(redirect_url)
        browser_utils_mock_obj.wait_by_class_name.assert_called_once_with("subname")
        mock_find_element_by_id.assert_called_once_with("sf_result")
        result_div_mock.find_elements_by_tag_name.assert_called_once_with("a")
        self.mock_thread_pool_executor.return_value.submit.assert_called_once_with(self.youtube_crawler.start_download,
                                                                              download_url, video_id, playlist_name)

    @patch('selenium_youtube_crawler.crawl_youtube.BrowserUtils')
    def test_initiate_video_id_crawl_with_element_found_first_and_loop_stopped(self, mock_browser_utils):
        video_id = "sdljfsdf"
        playlist_name = "test"
        redirect_url = "https://www.ssyoutube.com/watch?v=" + video_id
        download_url = "https://test.com/download?v=" + video_id
        attrs1 = {'get_attribute.side_effect': ["video format: 360", download_url]}
        attrs2 = {'get_attribute.return_value': "video format"}
        driver1 = MagicMock(**attrs1)
        driver2 = MagicMock(**attrs2)
        browser_utils_mock_obj = mock_browser_utils.return_value
        mock_find_element_by_id = browser_utils_mock_obj.find_element_by_id
        result_div_mock = MagicMock(spec=webdriver.Firefox).return_value
        result_div_mock.find_elements_by_tag_name.return_value = [driver1, driver2]
        mock_find_element_by_id.return_value = result_div_mock

        self.youtube_crawler.initiate_video_id_crawl(browser_utils_mock_obj, video_id, playlist_name)

        driver1.get_attribute.assert_has_calls([call("title"), call("href")])
        driver2.get_attribute.assert_not_called()
        browser_utils_mock_obj.get.assert_called_once_with(redirect_url)
        browser_utils_mock_obj.wait_by_class_name.assert_called_once_with("subname")
        mock_find_element_by_id.assert_called_once_with("sf_result")
        result_div_mock.find_elements_by_tag_name.assert_called_once_with("a")
        self.mock_thread_pool_executor.return_value.submit.assert_called_once_with(self.youtube_crawler.start_download,
                                                                                   download_url, video_id,
                                                                                   playlist_name)

    @patch('selenium_youtube_crawler.crawl_youtube.GCSHelper')
    @patch('selenium_youtube_crawler.crawl_youtube.BrowserUtils')
    def test_initiate_playlist_crawl_with_archives_present(self, mock_browser_utils, mock_gcs_helper):
        playlists = {"test1": ["sjkfjdks", "sjkfjdkds"]}
        playlist_name = "test1"
        validate_and_download_archive = mock_gcs_helper.return_value.validate_and_download_archive
        validate_and_download_archive.return_value = ["youtube sjkfjdks"]

        with patch.object(self.youtube_crawler, 'initiate_video_id_crawl') as mock_initiate_video_id_crawl:
            self.youtube_crawler.initiate_playlist_crawl(playlists, playlist_name)
            mock_initiate_video_id_crawl.assert_called_once_with(mock_browser_utils.return_value, "sjkfjdkds",
                                                                 playlist_name)

        validate_and_download_archive.assert_called_once_with(playlist_name)
        mock_browser_utils.return_value.quit.assert_called_once()

    @patch('selenium_youtube_crawler.crawl_youtube.GCSHelper')
    @patch('selenium_youtube_crawler.crawl_youtube.BrowserUtils')
    def test_initiate_playlist_crawl_with_no_archives_present(self, mock_browser_utils, mock_gcs_helper):
        playlists = {"test1": ["sjkfjdks", "sjkfjdkds"]}
        playlist_name = "test1"
        validate_and_download_archive = mock_gcs_helper.return_value.validate_and_download_archive
        validate_and_download_archive.return_value = []

        with patch.object(self.youtube_crawler, 'initiate_video_id_crawl') as mock_initiate_video_id_crawl:
            self.youtube_crawler.initiate_playlist_crawl(playlists, playlist_name)
            call1 = call(mock_browser_utils.return_value, "sjkfjdks", playlist_name)
            call2 = call(mock_browser_utils.return_value, "sjkfjdkds",
                         playlist_name)
            mock_initiate_video_id_crawl.assert_has_calls([call1, call2])

        validate_and_download_archive.assert_called_once_with(playlist_name)
        mock_browser_utils.return_value.quit.assert_called_once()

    @patch('selenium_youtube_crawler.crawl_youtube.as_completed')
    @patch('selenium_youtube_crawler.crawl_youtube.GCSHelper')
    @patch('selenium_youtube_crawler.crawl_youtube.read_playlist_from_file')
    def test_crawl_for_file_mode(self, mock_read_playlist, mock_gcs_helper, mock_as_completed):
        input_type = CrawlInput.FILE
        mock_read_playlist.return_value = {"test1": [], "test2": []}

        result1 = MagicMock(spec=futures.Future)
        result1.result.return_value = ()
        result2 = MagicMock(spec=futures.Future)
        result2.result.return_value = ()
        self.youtube_crawler.playlist_executor.submit.side_effect = [result1, result2]
        mock_as_completed.return_value = [result1, result2]

        self.youtube_crawler.crawl(input_type)

        result1.result.assert_called_once()
        result2.result.assert_called_once()
        mock_read_playlist.assert_called_once_with("playlists")
        mock_gcs_helper.return_value.upload_token_to_bucket.assert_not_called()

    @patch('selenium_youtube_crawler.crawl_youtube.as_completed')
    @patch('selenium_youtube_crawler.crawl_youtube.GCSHelper')
    @patch('selenium_youtube_crawler.crawl_youtube.read_playlist_from_youtube_api')
    def test_crawl_for_youtube_api_mode(self, mock_read_playlist, mock_gcs_helper, mock_as_completed):
        input_type = CrawlInput.YOUTUBE_API
        mock_read_playlist.return_value = {"test1": [], "test2": []}

        result1 = MagicMock(spec=futures.Future)
        result1.result.return_value = ()
        result2 = MagicMock(spec=futures.Future)
        result2.result.return_value = ()
        self.youtube_crawler.playlist_executor.submit.side_effect = [result1, result2]
        mock_as_completed.return_value = [result1, result2]

        self.youtube_crawler.crawl(input_type)

        result1.result.assert_called_once()
        result2.result.assert_called_once()
        mock_read_playlist.assert_called_once()
        mock_gcs_helper.return_value.download_token_from_bucket.assert_called_once()
        mock_gcs_helper.return_value.upload_token_to_bucket.assert_called_once()
