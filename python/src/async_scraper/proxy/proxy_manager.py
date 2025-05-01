import asyncio
import time
from typing import Optional
import logging
import random

from .free_proxy_provider import FreeProxyProvider

class ProxyManager:
    """代理管理器，负责管理和提供代理"""
    
    def __init__(self, check_url="https://www.google.com/", countries=["US", "CA"], cooldown_period=120, check_interval=600):
        """
        初始化代理管理器
        
        Args:
            providers: 代理提供者列表
            cooldown_period: 代理失败后的冷却时间（秒）
            check_interval: 定期检查代理的时间间隔（秒）
        """
        self.providers = []
        for country in countries:
           self.providers.append(FreeProxyProvider(check_url=check_url, country=country)) 
        self.proxies = {}  # {proxy_url: {"last_check": timestamp, "failures": count, "success": count}}
        self.cooldown_period = cooldown_period
        self.check_interval = check_interval
        self.logger = logging.getLogger("ProxyManager")
        self.update_lock = asyncio.Lock()
        self.last_update = 0
    
    async def get_proxy(self) -> Optional[str]:
        """
        获取可用代理
        
        Returns:
            代理URL字符串，如果没有可用代理则返回None
        """
        # 检查是否需要更新代理列表
        if time.time() - self.last_update > self.check_interval or not self.proxies:
            await self.update_proxies()
        
        available_proxies = []
        now = time.time()
        
        for proxy, data in self.proxies.items():
            # 检查代理是否不在冷却期
            if now - data.get("last_failure", 0) >= self.cooldown_period:
                available_proxies.append((proxy, data.get("success", 0) - data.get("failures", 0)))
        
        if not available_proxies:
            self.logger.warning("No available proxies. Using direct connection.")
            return None
        
        # 根据成功/失败记录进行加权选择
        weights = [max(1, score + 5) for _, score in available_proxies]
        total = sum(weights)
        if total <= 0:
            # 如果所有代理评分都很差，随机选择
            return random.choice([p for p, _ in available_proxies])
        
        # 加权随机选择
        r = random.uniform(0, total)
        cumulative = 0
        for (proxy, _), weight in zip(available_proxies, weights):
            cumulative += weight
            if r <= cumulative:
                return proxy
        
        # 如果加权选择失败，返回第一个可用代理
        return available_proxies[0][0] if available_proxies else None
    
    def report_proxy_success(self, proxy: str):
        """
        报告代理成功使用
        
        Args:
            proxy: 代理URL
        """
        if proxy in self.proxies:
            self.proxies[proxy]["success"] = self.proxies[proxy].get("success", 0) + 1
            self.proxies[proxy]["last_success"] = time.time()
    
    def report_proxy_failure(self, proxy: str):
        """
        报告代理失败
        
        Args:
            proxy: 代理URL
        """
        if proxy in self.proxies:
            self.proxies[proxy]["failures"] = self.proxies[proxy].get("failures", 0) + 1
            self.proxies[proxy]["last_failure"] = time.time()
    
    async def update_proxies(self):
        """更新代理列表"""
        async with self.update_lock:
            if time.time() - self.last_update < self.check_interval:
                return  # 防止多次并发更新

            self.logger.info("Updating proxy list...")
            new_proxies = self.proxies.copy()

            # 并发创建所有 provider 的 fetch 协程
            coros = [provider.get_proxies() for provider in self.providers]
            # 并发执行、收集结果或异常
            results = await asyncio.gather(*coros, return_exceptions=True)

            for provider, res in zip(self.providers, results):
                if isinstance(res, Exception):
                    self.logger.error(
                        f"Error getting proxies from provider {provider.__class__.__name__}: {res}"
                    )
                    continue

                for proxy in res:
                    if proxy not in new_proxies:
                        new_proxies[proxy] = {
                            "last_check": time.time(),
                            "failures": 0,
                            "success": 0
                        }

            self.proxies = new_proxies
            self.last_update = time.time()
            self.logger.info(f"Proxy list updated. Total proxies: {len(self.proxies)}")
