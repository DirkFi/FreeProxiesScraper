from abc import ABC, abstractmethod
from typing import Any


class BaseParser(ABC):
    @abstractmethod
    async def parse(self, content: Any) -> Any:
        """
        Parse content

        Args:
            content

        Returns:
            Parsed Data
        """
        pass
