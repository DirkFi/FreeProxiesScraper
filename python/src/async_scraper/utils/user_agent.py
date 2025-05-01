import random

class UserAgentManager:
    """User-Agent管理器"""
    
    def __init__(self, user_agents=None):
        """
        初始化User-Agent管理器
        
        Args:
            user_agents: User-Agent字符串列表，如果为None则使用默认列表
        """
        self.user_agents = user_agents or [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/109.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/113.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
        ]
    
    def get_random(self) -> str:
        """
        获取随机User-Agent
        
        Returns:
            随机User-Agent字符串
        """
        return random.choice(self.user_agents)
    
    def add_user_agent(self, user_agent: str):
        """
        添加新的User-Agent
        
        Args:
            user_agent: User-Agent字符串
        """
        if user_agent not in self.user_agents:
            self.user_agents.append(user_agent)
