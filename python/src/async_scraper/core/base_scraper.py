from abc import ABC, abstractmethod
import asyncio
from typing import Dict, List, Any, Optional
import logging


class BaseScraper(ABC):
    """
    Absctract base scraper class, defining common functions and methods
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize scraper

        Args:
            config: dict, including parameters for scraper
        """
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self.parser = None
        self.storage = None
        self.proxy_manager = None

    @abstractmethod
    async def fetch(self, url: str, **kwargs) -> Any:
        """
        Absctract method to fetch data

        Returns:
            Raw data
        """
        pass

    async def scrape(self, url: str, **kwargs) -> Any:
        """
        Flow control of scraping

        Returns:
            Processed data
        """
        raw_data = await self.fetch(url, **kwargs)
        if raw_data and self.parser:
            parsed_data = await self.parser.parse(raw_data)
            if self.storage:
                await self.storage.save(parsed_data)
            return parsed_data
        return raw_data

    async def scrape_many(self, urls: List[str], concurrency: int = 5, **kwargs) -> List[Any]:
        """
        Scrape many URLs in a batch

        Returns:
            Processed data list
        """
        semaphore = asyncio.Semaphore(concurrency)

        async def _scrape_with_semaphore(url):
            async with semaphore:
                try:
                    return await self.scrape(url, **kwargs)
                except Exception as e:
                    self.logger.error(f"Error scraping {url}: {e}")
                    return None

        tasks = [_scrape_with_semaphore(url) for url in urls]
        return await asyncio.gather(*tasks)

    def set_parser(self, parser):
        self.parser = parser
        return self

    def set_storage(self, storage):
        self.storage = storage
        return self

    def set_proxy_manager(self, proxy_manager):
        self.proxy_manager = proxy_manager
        return self
