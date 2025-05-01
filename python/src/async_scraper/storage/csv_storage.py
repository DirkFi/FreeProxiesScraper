import csv
import asyncio
import os
from typing import List, Dict, Any, Optional

from .base_storage import BaseStorage

class CsvStorage(BaseStorage):
    """CSV存储实现"""
    
    def __init__(self, file_path: str, fieldnames: Optional[List[str]] = None, mode: str = "a"):
        """
        初始化CSV存储
        
        Args:
            file_path: CSV文件路径
            fieldnames: CSV列名
            mode: 文件打开模式，"a"为追加，"w"为覆盖
        """
        self.file_path = file_path
        self.fieldnames = fieldnames
        self.mode = mode
    
    async def save(self, data: Any) -> bool:
        """
        保存数据到CSV
        
        Args:
            data: 要保存的数据，可以是字典或字典列表
            
        Returns:
            保存是否成功
        """
        if not data:
            return False
        
        # 确保数据是列表形式
        if not isinstance(data, list):
            data = [data]
        
        # 如果没有提供列名，则使用第一个字典的键作为列名
        if not self.fieldnames and isinstance(data[0], dict):
            self.fieldnames = list(data[0].keys())
        
        loop = asyncio.get_event_loop()
        try:
            # 检查文件是否存在，如果不存在，创建一个带有标题的文件
            file_exists = os.path.exists(self.file_path)
            
            # 写入模式
            write_mode = self.mode
            if not file_exists:
                # 确保目录存在
                os.makedirs(os.path.dirname(os.path.abspath(self.file_path)), exist_ok=True)
                write_mode = "w"  # 如果文件不存在，总是创建新文件
            
            # 使用executor避免阻塞事件循环
            await loop.run_in_executor(None, self._write_to_csv, data, write_mode, not file_exists)
            return True
        except Exception as e:
            print(f"Error saving to CSV: {e}")
            return False
    
    def _write_to_csv(self, data, write_mode, write_header):
        """在线程中执行CSV写入操作"""
        with open(self.file_path, write_mode, newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            if write_header:
                writer.writeheader()
            writer.writerows(data)
    
    async def load(self, **kwargs) -> List[Dict[str, Any]]:
        """
        从CSV加载数据
        
        Returns:
            字典列表，每个字典代表一行数据
        """
        if not os.path.exists(self.file_path):
            return []
        
        loop = asyncio.get_event_loop()
        try:
            # 使用executor避免阻塞事件循环
            return await loop.run_in_executor(None, self._read_from_csv)
        except Exception as e:
            print(f"Error loading from CSV: {e}")
            return []
    
    def _read_from_csv(self):
        """在线程中执行CSV读取操作"""
        data = []
        with open(self.file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            self.fieldnames = reader.fieldnames
            for row in reader:
                data.append(row)
        return data
