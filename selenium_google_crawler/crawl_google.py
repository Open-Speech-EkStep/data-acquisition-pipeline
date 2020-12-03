import os

import geckodriver_autoinstaller
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from loader_util import read_archive, read_config

geckodriver_autoinstaller.install()


class GoogleCrawler:

    def __init__(self, config_path):
        self.archive = []
        self.links_count = 0
        self.archive = read_archive()
        config = read_config(config_path)
        self.language = config["language"]
        self.language_code = config["language_code"] if config["language_code"] != "" else "en"
        self.max_pages = config["max_pages"]
        self.word_to_ignore = config["words_to_ignore"]
        self.extensions_to_ignore = config["extensions_to_ignore"]
        self.keywords = config["keywords"]
        self.headless = config['headless']

    def is_present_in_archive(self, url):
        url = self.sanitize(url)
        return url in self.archive

    def sanitize(self, word):
        return word.rstrip().lstrip().lower()

    def is_unwanted_present(self, url):
        for keyword in self.word_to_ignore:
            if self.sanitize(keyword) in self.sanitize(url):
                return True
        return False

    def is_unwanted_extension_present(self, url):
        for ext in self.extensions_to_ignore:
            if self.sanitize(url).endswith(ext):
                return True
        return False

    def is_unwanted_wiki(self, url):
        url = self.sanitize(url)
        if "wikipedia.org" in url or "wikimedia.org" in url:
            url = url.replace("https://", "").replace("http://", "")
            if not url.startswith("en") and not url.startswith(self.language_code) and not url.startswith("wiki"):
                return True
            else:
                return False
        return True

    def extract_and_move_next(self, browser, current_page):
        try:
            WebDriverWait(browser, 20).until(EC.presence_of_element_located((By.CLASS_NAME, "yuRUbf")))
        except TimeoutException:
            print("Load failed")
            return
        link_elements = browser.find_elements_by_class_name('yuRUbf')
        file_name = 'urls.txt'
        self.extract_links(link_elements, file_name)
        self.move_to_next_page(browser, current_page)

    def move_to_next_page(self, browser, current_page):
        try:
            next_btn = browser.find_element_by_id("pnnext")
            next_btn.click()
            current_page += 1
            if current_page < self.max_pages:
                self.extract_and_move_next(browser, current_page)
        except Exception:
            print("Can't move to next page! Not Found")

    def extract_links(self, link_elements, file_name):
        for link_element in link_elements:
            a_element = link_element.find_element_by_tag_name("a")
            link = a_element.get_attribute('href')
            if self.is_present_in_archive(link) or self.is_unwanted_present(link) or self.is_unwanted_extension_present(
                    link) or self.is_unwanted_wiki(link):
                with open("ignored.txt",'a') as f:
                    f.write(link + "\n")
                continue
            self.links_count += 1
            self.archive.append(link + "\n")
            with open(file_name, 'a') as f:
                f.write(link + "\n")

    def crawl(self, archive_file_name):
        options = Options()
        options.headless = self.headless
        browser = webdriver.Firefox(options=options)
        browser.maximize_window()

        for keyword in self.keywords:
            browser.get("https://www.google.com/")
            # Get search bar
            search_element = browser.find_element_by_class_name("gLFyf.gsfi")
            search_element.send_keys(self.language + ' ' + keyword + Keys.RETURN)
            start_page = 1
            self.extract_and_move_next(browser, start_page)

        print("Extracted %s urls" % str(self.links_count))
        with open(archive_file_name, 'w') as f:
            f.writelines(self.archive)
        browser.quit()


if __name__ == "__main__":
    config_file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "config.json")
    google_crawler = GoogleCrawler(config_file_path)
    google_crawler.crawl('archive.txt')
