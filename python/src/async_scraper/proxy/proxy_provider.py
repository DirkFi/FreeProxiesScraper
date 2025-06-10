from abc import ABC, abstractmethod
from typing import List


class ProxyProvider(ABC):
    @abstractmethod
    async def get_proxies(self) -> List[str]:
        """
        Get proxies list

        Returns:
            URL list
        """
        pass
