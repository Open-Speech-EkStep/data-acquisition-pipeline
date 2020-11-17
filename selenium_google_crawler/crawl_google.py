from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import os
import json
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import geckodriver_autoinstaller

geckodriver_autoinstaller.install()


class GoogleCrawler:

    def __init__(self, config_path):
        self.archive = []
        self.links_count = 0
        if os.path.exists('archive.txt'):
            with open('archive.txt', 'r') as f:
                self.archive = f.read().splitlines()
        with open(config_path, 'r') as f:
            config = json.load(f)
            self.language = config["language"]
            self.language_code = config["language_code"]
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
            if not url.startswith("en") or not url.startswith(self.language_code) or not url.startswith("wiki"):
                return True
        return False

    def extract_and_move_next(self, browser, current_page):
        try:
            WebDriverWait(browser, 20).until(EC.presence_of_element_located((By.CLASS_NAME, "yuRUbf")))
        except:
            print("Load failed")
            return
        link_elements = browser.find_elements_by_class_name('yuRUbf')
        for link_element in link_elements:
            a_element = link_element.find_element_by_tag_name("a")
            link = a_element.get_attribute('href')
            if self.is_present_in_archive(link) or self.is_unwanted_present(link) or self.is_unwanted_extension_present(
                    link) or self.is_unwanted_wiki(link):
                continue
            self.links_count += 1
            self.archive.append(link + "\n")
            with open('urls.txt', 'a') as f:
                f.write(link + "\n")
        try:
            next_btn = browser.find_element_by_id("pnnext")
            next_btn.click()
            current_page += 1
            if current_page < self.max_pages:
                self.extract_and_move_next(browser, current_page)
        except:
            pass

    def crawl(self):
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
        with open('archive.txt', 'w') as f:
            f.writelines(self.archive)
        browser.quit()


if __name__ == "__main__":
    config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "config.json")
    google_crawler = GoogleCrawler(config_path)
    google_crawler.crawl()
