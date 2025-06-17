from bs4 import BeautifulSoup
from typing import Any, Optional, Callable
import asyncio

from .base_parser import BaseParser


class HtmlParser(BaseParser):
    def __init__(self, selector: Optional[str] = None, parser: str = "html.parser", parse_func: Optional[Callable] = None):
        """
        Args:
            selector: CSS selector
            parser: Beautiful Soup Parser
            parse_func: Custom parse function
        """
        self.selector = selector
        self.parser_type = parser
        self.parse_func = parse_func

    async def parse(self, content: str = "", *args, **kwargs) -> Any:
        """
        Args:
            content: HTML String

        Returns:
            Parsed Data
        """
        if not content:
            return None

        # Create async task to protect from blocking
        if not content:
            return None
        loop = asyncio.get_event_loop()
        soup = await loop.run_in_executor(None, lambda: BeautifulSoup(content, self.parser_type))

        if self.parse_func:
            return await loop.run_in_executor(None, lambda: self.parse_func(soup, *args, **kwargs))

        if self.selector:
            return await loop.run_in_executor(None, lambda: soup.select(self.selector))

        return soup
