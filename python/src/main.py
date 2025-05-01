import asyncio
import logging
import sys
from bs4 import BeautifulSoup
from async_scraper.core.http_scraper import HttpScraper
from async_scraper.proxy.proxy_manager import ProxyManager
from async_scraper.proxy.free_proxy_provider import FreeProxyProvider
from async_scraper.parser.html_parser import HtmlParser
from async_scraper.storage.csv_storage import CsvStorage

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("NBA_Scraper")

# 自定义NBA团队数据解析函数
async def parse_team_data(soup, year):
    """解析NBA团队数据"""
    team_data = []
    table = soup.find('table', {'id': 'advanced-team'})
    
    if not table:
        logger.warning(f"No team table found for {year}")
        return team_data
    
    rows = table.find_all('tr')
    for row in rows[2:]:  # 跳过标题行
        cols = row.find_all('td')
        if not cols:
            continue  # 跳过无效行
        
        try:
            team = {
                'year': year,
                'name': cols[0].text.strip('*'),
                'wins': cols[2].text.strip(),
                'losses': cols[3].text.strip(),
                'Margin_of_Victory': cols[6].text.strip(),
                'True_Shooting_Percentage': cols[15].text.strip(),
                'Simple_Rating_System': cols[8].text.strip()
            }
            team_data.append(team)
        except Exception as e:
            logger.error(f"Error parsing team row: {e}")
    
    return team_data

# 自定义NBA球员数据解析函数
async def parse_player_data(soup, team):
    """解析NBA球员数据"""
    player_data = []
    table = soup.find('table', {'id': 'per_game_stats'})
    
    if not table:
        logger.warning(f"No player table found for {team}")
        return player_data
    
    rows = table.find_all('tr')
    for row in rows[1:]:  # 跳过标题行
        cols = row.find_all('td')
        if not cols:
            continue  # 跳过无效行
        
        try:
            player = {
                'team': team,
                'year': 2025,  # 假设是当前赛季
                'name': cols[0].text.strip(),
                'position': cols[2].text.strip() if len(cols) > 2 else "",
                'minutes_per_game': cols[5].text.strip() if len(cols) > 5 else "",
                'field_goal_percentage': cols[8].text.strip() if len(cols) > 8 else "",
                '3_point_percentage': cols[11].text.strip() if len(cols) > 11 else "",
                'rebounds_per_game': cols[21].text.strip() if len(cols) > 21 else "",
                'assists_per_game': cols[22].text.strip() if len(cols) > 22 else "",
                'points_per_game': cols[27].text.strip() if len(cols) > 27 else ""
            }
            player_data.append(player)
        except Exception as e:
            logger.error(f"Error parsing player row: {e}")
    
    return player_data

# 自定义解析函数，根据URL选择合适的解析方法
async def my_custom_parse_function(soup, url=None):
    """
    根据URL选择合适的解析方法
    
    Args:
        soup: BeautifulSoup对象
        url: 目标URL
        
    Returns:
        解析后的数据
    """
    if not url:
        logger.warning("No URL provided for parsing")
        return []
    
    # 判断URL类型并选择合适的解析方法
    if "leagues/NBA" in url:
        # 从URL中提取年份
        try:
            year = int(url.split("NBA_")[1].split(".")[0])
            return await parse_team_data(soup, year)
        except Exception as e:
            logger.error(f"Error extracting year from URL: {e}")
            return []
    elif "/teams/" in url:
        # 从URL中提取球队缩写
        try:
            team = url.split("/teams/")[1].split("/")[0]
            return await parse_player_data(soup, team)
        except Exception as e:
            logger.error(f"Error extracting team from URL: {e}")
            return []
    else:
        logger.warning(f"Unknown URL pattern: {url}")
        return []

# 主函数
async def main():
    # 定义要爬取的URL
    # 团队数据URL (2020-2025)
    team_urls = [f"https://www.basketball-reference.com/leagues/NBA_{year}.html" 
                 for year in range(2020, 2026)]
    
    # 球员数据URL (各队2025赛季)
    TEAMS = ['ATL', 'BOS', 'BRK', 'CHI', 'CHO', 'CLE', 'DAL', 'DEN', 'DET', 'GSW', 
             'HOU', 'IND', 'LAC', 'LAL', 'MEM', 'MIA', 'MIL', 'MIN', 'NOP', 'NYK', 
             'OKC', 'ORL', 'PHI', 'PHO', 'POR', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS']
    player_urls = [f"https://www.basketball-reference.com/teams/{team}/2025.html" 
                  for team in TEAMS]
    
    # 所有要爬取的URL
    all_urls = team_urls + player_urls
    
    # 创建代理管理器
    # proxy_provider = FreeProxyProvider(check_url="https://www.basketball-reference.com", country="CA")
    proxy_manager = ProxyManager(check_url="https://www.basketball-reference.com")
    
    # 创建爬虫
    scraper = HttpScraper(config={
        'retry_times': 5,
        'retry_delay': 2,
        'timeout': 10
    })
    
    # 创建解析器 (URL作为参数传递给解析函数)
    parser = HtmlParser(parse_func=my_custom_parse_function)
    
    # 创建团队和球员数据的存储
    team_storage = CsvStorage('team_data.csv')
    player_storage = CsvStorage('player_data.csv')
    
    # 先设置团队数据存储
    scraper.set_scraper(parser, team_storage, proxy_manager)
    
    # 分别爬取团队数据和球员数据
    logger.info("开始爬取NBA团队数据...")
    team_results = []
    
    # 团队数据爬取
    for url in team_urls:
        # TODO: wrap this piece of code to a convenient function
        logger.info(f"爬取URL: {url}")
        # 为每次爬取提供URL作为额外参数
        result = await scraper.fetch(url)
        if result:
            soup = BeautifulSoup(result, 'html.parser')
            data = await my_custom_parse_function(soup, url)
            if data:
                team_results.extend(data)
                logger.info(f"成功从 {url} 解析了 {len(data)} 条团队记录")
    
    # 保存团队数据
    if team_results:
        await team_storage.save(team_results)
        logger.info(f"保存了 {len(team_results)} 条团队数据记录")
    
    # 切换到球员数据存储
    scraper.set_storage(player_storage)
    
    # 使用并发爬取球员数据
    logger.info("开始爬取NBA球员数据...")
    player_results = []
    
    # 创建爬取任务，限制并发为5
    concurrency = 5
    semaphore = asyncio.Semaphore(concurrency)
    
    async def fetch_with_semaphore(url):
        async with semaphore:
            logger.info(f"爬取URL: {url}")
            result = await scraper.fetch(url)
            if result:
                soup = BeautifulSoup(result, 'html.parser')
                data = await my_custom_parse_function(soup, url)
                if data:
                    logger.info(f"成功从 {url} 解析了 {len(data)} 条球员记录")
                    return data
            return []
    
    # 创建并发任务
    tasks = [fetch_with_semaphore(url) for url in player_urls]
    player_data_lists = await asyncio.gather(*tasks)
    
    # 合并所有球员数据
    for data_list in player_data_lists:
        player_results.extend(data_list)
    
    # 保存球员数据
    if player_results:
        await player_storage.save(player_results)
        logger.info(f"保存了 {len(player_results)} 条球员数据记录")
    
    # 关闭资源
    await scraper.close()
    logger.info("爬取完成!")

# 运行主函数
if __name__ == "__main__":
    asyncio.run(main())
