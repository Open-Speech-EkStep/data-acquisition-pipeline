from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


class BrowserUtils:

    def __init__(self):
        options = Options()
        options.headless = True
        self.browser = webdriver.Firefox(options=options)
        self.browser.maximize_window()

    def get(self, url):
        self.browser.get(url)

    def wait_by_class_name(self, class_name):
        WebDriverWait(self.browser, 30).until(
            EC.presence_of_element_located((By.CLASS_NAME, class_name))
        )

    def find_element_by_id(self, id):
        return self.browser.find_element_by_id(id)

    def quit(self):
        try:
            self.browser.quit()
        except Exception:
            pass

    def __del__(self):
        self.quit()
