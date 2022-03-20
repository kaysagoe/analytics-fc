"""Custom client handling, including StatbunkerStream base class."""

from core.scraper import ScraperStream


class StatbunkerStream(ScraperStream):
    """Stream class for Statbunker streams."""

    url_base = "https://www.statbunker.com/"
    
    def _agree_cookies(self) -> None:
        pass
