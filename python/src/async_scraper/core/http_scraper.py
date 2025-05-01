import aiohttp
import asyncio
import random
from typing import Dict, Any, Optional, List, Union
import time

from .base_scraper import BaseScraper
from ..proxy.proxy_manager import ProxyManager
from ..utils.user_agent import UserAgentManager

class HttpScraper(BaseScraper):
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Args:
            config: Config dict including parameters:
                - timeout: seconds
                - retry_times 
                - retry_delay: seconds
                - headers
        """
        super().__init__(config)
        self.timeout = aiohttp.ClientTimeout(total=self.config.get('timeout', 10))
        self.retry_times = self.config.get('retry_times', 3)
        self.retry_delay = self.config.get('retry_delay', 2)
        self.headers = self.config.get('headers', {})
        self.user_agent_manager = UserAgentManager()
        self.session = None
    
    async def _ensure_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=self.timeout)
        return self.session
    
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def fetch(self, url: str, **kwargs) -> Optional[str]:
        """
        Returns:
            Web Content in string, return None otherwise
        """
        session = await self._ensure_session()
        headers = dict(self.headers)
        headers.update({"User-Agent": self.user_agent_manager.get_random()})
        
        for attempt in range(self.retry_times):
            proxy = None
            if self.proxy_manager:
                proxy = await self.proxy_manager.get_proxy()
            
            try:
                self.logger.debug(f"Fetching {url} [Attempt {attempt+1}/{self.retry_times}]")
                if proxy:
                    self.logger.debug(f"Using proxy: {proxy}")
                
                request_kwargs = {
                    "headers": headers,
                    "proxy": proxy,
                    **kwargs
                }
                
                async with session.get(url, **request_kwargs) as response:
                    if response.status == 200:
                        self.logger.debug(f"Successfully fetched {url}")
                        return await response.text()
                    
                    if response.status == 429:  # Too Many Requests
                        self.logger.warning(f"Rate limited (429) for {url}")
                        if proxy and self.proxy_manager:
                            self.proxy_manager.report_proxy_failure(proxy)
                        wait_time = 5 + attempt * 10
                        self.logger.info(f"Waiting {wait_time}s before retry")
                        await asyncio.sleep(wait_time)
                        continue
                    
                    if response.status == 403:  # Forbidden
                        self.logger.warning(f"Access forbidden (403) for {url}")
                        if proxy and self.proxy_manager:
                            self.proxy_manager.report_proxy_failure(proxy)
                        await asyncio.sleep(self.retry_delay)
                        continue
                    
                    # Other errors
                    self.logger.error(f"HTTP error {response.status} for {url}")
                    if proxy and self.proxy_manager:
                        self.proxy_manager.report_proxy_failure(proxy)
            
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                self.logger.error(f"Request error for {url}: {str(e)}")
                if proxy and self.proxy_manager:
                    self.proxy_manager.report_proxy_failure(proxy)
            
            # Retry after random time
            retry_delay = self.retry_delay * (1 + attempt * 0.5)
            retry_delay += random.uniform(0, 1)
            await asyncio.sleep(retry_delay)
        
        self.logger.error(f"Max retries reached for {url}")
        return None

    def set_scraper(self, parser, storage, proxy_manager):
        self.set_parser(parser)
        self.set_storage(storage)
        self.set_proxy_manager(proxy_manager)
