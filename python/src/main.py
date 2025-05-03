import asyncio
import logging
import sys
from bs4 import BeautifulSoup
from async_scraper.core.http_scraper import HttpScraper
from async_scraper.proxy.proxy_manager import ProxyManager
from async_scraper.proxy.free_proxy_provider import FreeProxyProvider
from async_scraper.parser.html_parser import HtmlParser
from async_scraper.storage.csv_storage import CsvStorage
from typing import Any, List, Dict, Optional
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
def parse_team_data(soup: BeautifulSoup, year: int) -> List[Dict[str, Any]]:
    """解析 NBA 团队数据（同步版）"""
    team_data: List[Dict[str, Any]] = []
    table = soup.find('table', {'id': 'advanced-team'})

    if not table:
        logger.warning(f"No team table found for {year}")
        return team_data

    rows = table.find_all('tr')
    for row in rows[2:]:  # 跳过标题行
        cols = row.find_all('td')
        if not cols:
            continue

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
            logger.error(f"Error parsing team row for {year}: {e}")

    return team_data


# 同步版：解析 NBA 球员数据
def parse_player_data(soup: BeautifulSoup, team: str) -> List[Dict[str, Any]]:
    """解析 NBA 球员数据（同步版）"""
    player_data: List[Dict[str, Any]] = []
    table = soup.find('table', {'id': 'per_game_stats'})

    if not table:
        logger.warning(f"No player table found for {team}")
        return player_data

    rows = table.find_all('tr')
    for row in rows[1:]:  # 跳过标题行
        cols = row.find_all('td')
        if not cols:
            continue

        try:
            player = {
                'team': team,
                'year': 2025,  # 假设当前赛季
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
            logger.error(f"Error parsing player row for {team}: {e}")

    return player_data


# 同步版：根据 URL 选择解析函数
def my_custom_parse_function(soup: BeautifulSoup, url: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    根据 URL 选择合适的解析方法（同步版）

    Args:
        soup: BeautifulSoup 对象
        url: 目标页面 URL

    Returns:
        解析后的数据列表
    """
    if not url:
        logger.warning("No URL provided for parsing")
        return []

    if "leagues/NBA" in url:
        try:
            year = int(url.split("NBA_")[1].split(".")[0])
            return parse_team_data(soup, year)
        except Exception as e:
            logger.error(f"Error extracting year from URL '{url}': {e}")
            return []

    elif "/teams/" in url:
        try:
            team = url.split("/teams/")[1].split("/")[0]
            return parse_player_data(soup, team)
        except Exception as e:
            logger.error(f"Error extracting team from URL '{url}': {e}")
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
    TEAMS = ['ATL', 'BOS']
    player_urls = [f"https://www.basketball-reference.com/teams/{team}/2025.html" 
                  for team in TEAMS]
    # 创建爬虫
    scraper = HttpScraper(parse_func=my_custom_parse_function, save_file='team_data.csv', 
                          check_url="https://www.basketball-reference.com",
                          config={
                            'retry_times': 5,
                            'retry_delay': 2,
                            'timeout': 10
    })
    
    player_storage = CsvStorage('player_data.csv')
    
    # logger.info("开始爬取NBA团队数据...")
    # team_results = []
    #
    # # 团队数据爬取
    # for url in team_urls:
    #     # TODO: wrap this piece of code to a convenient function
    #     logger.info(f"爬取URL: {url}")
    #     # 为每次爬取提供URL作为额外参数
    #     result = await scraper.fetch(url)
    #     if result:
    #         soup = BeautifulSoup(result, 'html.parser')
    #         data = await scraper.parser.parse(result)
    #         if data:
    #             team_results.extend(data)
    #             logger.info(f"成功从 {url} 解析了 {len(data)} 条团队记录")
    #
    # # 保存团队数据
    # if team_results:
    #     await scraper.storage.save(team_results)
    #     logger.info(f"保存了 {len(team_results)} 条团队数据记录")
    
    # 切换到球员数据存储
    scraper.set_storage(player_storage)

    # 使用并发爬取球员数据
    logger.info("开始爬取NBA球员数据...")
    try:
        data_with_urls = await scraper.get_parsed_data(urls=player_urls)
        all_players = [item for sublist in data_with_urls for item in sublist]
        if all_players:
            await player_storage.save(all_players)
            scraper.logger.info(f"共保存球员数据 {len(all_players)} 条")
    finally:
        # ▶ 保证 session 会被关闭
        await scraper.close()

# 运行主函数
if __name__ == "__main__":
    asyncio.run(main())
