import aiohttp
import asyncio
from bs4 import BeautifulSoup
from typing import List
import logging

from .proxy_provider import ProxyProvider
from ..utils.user_agent import UserAgentManager

class FreeProxyProvider(ProxyProvider):
    """从免费代理网站获取代理"""
    
    def __init__(self, url="https://www.free-proxy-list.net/", check_url="https://www.google.com", country="US"):
        """
        初始化免费代理提供者
        
        Args:
            url: 代理列表网站URL
            check_url: 用于验证代理的URL
        """
        self.url = url
        self.check_url = check_url
        self.logger = logging.getLogger("FreeProxyProvider")
        self.user_agent_manager = UserAgentManager()
        self.country = country
    
    async def get_proxies(self) -> List[str]:
        """
        获取并验证免费代理
        
        Returns:
            可用代理URL列表
        """
        raw_proxies = await self._scrape_proxies()
        valid_proxies = await self._validate_proxies(raw_proxies)
        self.logger.info(f"Found {len(valid_proxies)} valid proxies out of {len(raw_proxies)} scraped")
        return valid_proxies
    
    async def _scrape_proxies(self) -> List[str]:
        """
        从代理网站抓取代理
        
        Returns:
            代理URL列表
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
                    
                    # 适用于free-proxy-list.net的解析逻辑
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
                            if code == self.country:
                                proxy = f"http://{ip}:{port}"
                                proxies.append(proxy)
                    
                    return proxies
        except Exception as e:
            self.logger.error(f"Error scraping proxies: {e}")
            return []
    
    async def _validate_proxies(self, proxies: List[str], timeout=5, concurrent=10) -> List[str]:
        """
        验证代理是否可用
        
        Args:
            proxies: 代理URL列表
            timeout: 验证超时时间（秒）
            concurrent: 并发验证数量
            
        Returns:
            可用代理URL列表
        """
        valid_proxies = []
        semaphore = asyncio.Semaphore(concurrent)
        
        async def _check_proxy(proxy):
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
                except:
                    pass
                return None
        
        tasks = [_check_proxy(proxy) for proxy in proxies]
        results = await asyncio.gather(*tasks)
        valid_proxies = [p for p in results if p]
        
        return valid_proxies
