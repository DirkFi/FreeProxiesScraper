import aiohttp
import asyncio
import logging
from bs4 import BeautifulSoup
from typing import List, Optional

from .proxy_provider import ProxyProvider
from ..utils.user_agent import UserAgentManager

class FreeProxyProvider(ProxyProvider):
    """
    Retrieve proxies from a free proxy website.
    """
    
    def __init__(self, url: str = "https://www.free-proxy-list.net/", check_url: str = "https://www.google.com", country: str = "US"):
        """
        Initialize the free proxy provider.
        
        Args:
            url: URL of the proxy listing website.
            check_url: URL used to verify proxies.
            country: Country code to filter proxies by.
        """
        self.url = url
        self.check_url = check_url
        self.logger = logging.getLogger("FreeProxyProvider")
        self.user_agent_manager = UserAgentManager()
        self.country = country
    
    async def get_proxies(self) -> List[str]:
        """
        Fetch and validate free proxies.
        
        Returns:
            List[str]: List of valid proxy URLs.
        """
        raw_proxies = await self._scrape_proxies()
        valid_proxies = await self._validate_proxies(raw_proxies)
        self.logger.info(f"Found {len(valid_proxies)} valid proxies out of {len(raw_proxies)} scraped")
        return valid_proxies
    
    async def _scrape_proxies(self) -> List[str]:
        """
        Scrape proxies from the proxy listing website.
        
        Returns:
            List[str]: List of proxy URLs.
        """
        try:
            headers = {"User-Agent": self.user_agent_manager.get_random()}
            async with aiohttp.ClientSession() as session:
                async with session.get(self.url, headers=headers) as response:
                    if response.status != 200:
                        self.logger.error(f"Failed to fetch proxies, status code: {response.status}")
                        return []
                    
                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")
                    proxies = []
                    
                    # Parsing logic for free-proxy-list.net
                    table = soup.find("table", {"class": "table-striped"})
                    if not table:
                        self.logger.error("Proxy table not found")
                        return []
                    
                    for row in table.tbody.find_all("tr"):
                        cols = row.find_all("td")
                        if len(cols) >= 7:
                            ip = cols[0].text.strip()
                            port = cols[1].text.strip()
                            code = cols[2].text.strip()
                            https = cols[6].text.strip()
                            if code == self.country and https == "yes":
                                proxy = f"http://{ip}:{port}"
                                proxies.append(proxy)
                    
                    return proxies
        except Exception as e:
            self.logger.error(f"Error scraping proxies: {e}")
            return []
    
    async def _validate_proxies(self, proxies: List[str], timeout: int = 5, concurrent: int = 10) -> List[str]:
        """
        Validate proxies to ensure they are usable.
        
        Args:
            proxies: List of proxy URLs to validate.
            timeout: Timeout in seconds for each proxy check.
            concurrent: Number of concurrent validation requests.
        
        Returns:
            List[str]: List of valid proxy URLs.
        """
        valid_proxies: List[str] = []
        semaphore = asyncio.Semaphore(concurrent)
        
        async def _check_proxy(proxy: str) -> Optional[str]:
            async with semaphore:
                try:
                    headers = {"User-Agent": self.user_agent_manager.get_random()}
                    async with aiohttp.ClientSession() as session:
                        async with session.get(
                            self.check_url,
                            proxy=proxy,
                            timeout=aiohttp.ClientTimeout(total=timeout),
                            headers=headers
                        ) as response:
                            if response.status == 200:
                                self.logger.debug(f"Valid proxy: {proxy}")
                                return proxy
                except Exception:
                    pass
                return None
        
        tasks = [_check_proxy(proxy) for proxy in proxies]
        results = await asyncio.gather(*tasks)
        valid_proxies = [p for p in results if p]
        
        return valid_proxies
