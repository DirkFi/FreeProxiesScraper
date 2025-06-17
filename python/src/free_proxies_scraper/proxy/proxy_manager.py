import asyncio
import time
import logging
import random
from typing import Optional

from .free_proxy_provider import FreeProxyProvider


class ProxyManager:
    """
    Proxy manager responsible for managing and providing proxies.
    """

    def __init__(
        self,
        check_url: str = "https://www.google.com/",
        countries: list = ["US", "CA"],
        cooldown_period: int = 120,
        check_interval: int = 600
    ):
        """
        Initialize the proxy manager.

        Args:
            check_url: URL used to verify proxies.
            countries: List of country codes to fetch proxies from.
            cooldown_period: Cooldown time after a proxy failure (in seconds).
            check_interval: Interval for periodic proxy list refresh (in seconds).
        """
        self.providers = []
        for country in countries:
            self.providers.append(
                FreeProxyProvider(check_url=check_url, country=country)
            )
        # Store proxy metadata: {proxy_url: {"last_check": timestamp, "failures": count, "success": count}}
        self.proxies = {}
        self.cooldown_period = cooldown_period
        self.check_interval = check_interval
        self.logger = logging.getLogger("ProxyManager")
        self.update_lock = asyncio.Lock()
        self.last_update = 0

    async def get_proxy(self) -> Optional[str]:
        """
        Get an available proxy.

        Returns:
            A proxy URL string, or None if no proxies are available.
        """
        # Refresh proxy list if it's stale or empty
        if time.time() - self.last_update > self.check_interval or not self.proxies:
            await self.update_proxies()

        available = []
        now = time.time()

        for proxy, stats in self.proxies.items():
            # Only include proxies not in cooldown
            if now - stats.get("last_failure", 0) >= self.cooldown_period:
                score = stats.get("success", 0) - stats.get("failures", 0)
                available.append((proxy, score))

        if not available:
            self.logger.warning(
                "No available proxies. Falling back to direct connection.")
            return None

        # Weighted selection based on success/failure history
        weights = [max(1, score + 5) for _, score in available]
        total = sum(weights)
        if total <= 0:
            # If all scores are poor, pick randomly
            return random.choice([p for p, _ in available])

        choice = random.uniform(0, total)
        cumulative = 0
        for (proxy, _), weight in zip(available, weights):
            cumulative += weight
            if choice <= cumulative:
                return proxy

        # Fallback to first available proxy
        return available[0][0]

    def report_proxy_success(self, proxy: str):
        """
        Report a successful use of the given proxy.

        Args:
            proxy: The proxy URL that succeeded.
        """
        if proxy in self.proxies:
            self.proxies[proxy]["success"] = self.proxies[proxy].get("success", 0) + 1
            self.proxies[proxy]["last_success"] = time.time()

    def report_proxy_failure(self, proxy: str):
        """
        Report a failure of the given proxy.

        Args:
            proxy: The proxy URL that failed.
        """
        if proxy in self.proxies:
            self.proxies[proxy]["failures"] = self.proxies[proxy].get("failures", 0) + 1
            self.proxies[proxy]["last_failure"] = time.time()

    async def update_proxies(self):
        """
        Refresh the list of proxies from all providers.
        """
        async with self.update_lock:
            # Avoid concurrent updates
            if time.time() - self.last_update < self.check_interval:
                return

            self.logger.info("Updating proxy list...")
            new_proxies = self.proxies.copy()

            # Gather proxies from all providers concurrently
            coroutines = [provider.get_proxies()
                          for provider in self.providers]
            results = await asyncio.gather(*coroutines, return_exceptions=True)

            for provider, result in zip(self.providers, results):
                if isinstance(result, Exception):
                    self.logger.error(
                        f"Error getting proxies from {provider.__class__.__name__}: {result}"
                    )
                    continue

                for proxy in result:
                    if proxy not in new_proxies:
                        new_proxies[proxy] = {
                            "last_check": time.time(),
                            "failures": 0,
                            "success": 0
                        }

            self.proxies = new_proxies
            self.last_update = time.time()
            self.logger.info(f"Proxy list updated. Total proxies: {len(self.proxies)}")
