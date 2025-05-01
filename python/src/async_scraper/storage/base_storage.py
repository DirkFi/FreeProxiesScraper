from abc import ABC, abstractmethod
from typing import Any

class BaseStorage(ABC):
    """存储基类"""
    
    @abstractmethod
    async def save(self, data: Any) -> bool:
        """
        保存数据
        
        Args:
            data: 要保存的数据
            
        Returns:
            保存是否成功
        """
        pass
    
    @abstractmethod
    async def load(self, **kwargs) -> Any:
        """
        加载数据
        
        Args:
            **kwargs: 加载参数
            
        Returns:
            加载的数据
        """
        pass
