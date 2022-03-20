"""Custom client handling, including SoFIFAStream base class."""

from core.scraper import ScraperStream
from selenium.webdriver.common.by import By

class SoFIFAStream(ScraperStream):
    """Stream class for SoFIFA streams."""

    url_base = "https://www.sofifa.com/"
    
    def _agree_cookies(self) -> None:
        self.driver.find_element(By.LINK_TEXT, 'Continue to Site').click()