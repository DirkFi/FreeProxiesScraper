from abc import ABC, abstractmethod
from typing import Any


class BaseStorage(ABC):
    """BaseStorage Class"""

    @abstractmethod
    async def save(self, data: Any) -> bool:
        """
        Save Data

        Args:
            data 

        Returns:
            If the method is successful
        """
        pass

    @abstractmethod
    async def load(self, **kwargs) -> Any:
        """
        Load Data

        Args:
            **kwargs: Loaded parameters

        Returns:
            Loaded Data
        """
        pass
