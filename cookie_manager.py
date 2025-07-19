import requests
import time
import random
import logging
from urllib.parse import urlparse, parse_qs

logger = logging.getLogger(__name__)

class CookieManager:
    """自动获取和管理51job网站的cookies"""
    
    def __init__(self):
        self.base_url = "https://we.51job.com"
        self.search_url = "https://we.51job.com/pc/search"
        
    def get_fresh_cookies(self):
        """获取最新的cookies"""
        try:
            logger.info("开始获取最新cookies")
            
            # 构造基础headers
            headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "DNT": "1",
                "Host": "we.51job.com",
                "Pragma": "no-cache",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
            
            # 创建session来保持cookies
            session = requests.Session()
            
            # 第一步：访问首页获取基础cookies
            logger.info("访问51job首页获取基础cookies")
            response = session.get(self.base_url, headers=headers, timeout=30)
            
            if response.status_code != 200:
                logger.error(f"访问首页失败，状态码: {response.status_code}")
                return None
                
            # 第二步：访问搜索页面获取更多cookies
            search_params = {
                "jobArea": "000000",
                "keyword": "数据分析",
                "searchType": "2"
            }
            
            logger.info("访问搜索页面获取完整cookies")
            response = session.get(self.search_url, params=search_params, headers=headers, timeout=30)
            
            if response.status_code != 200:
                logger.error(f"访问搜索页面失败，状态码: {response.status_code}")
                return None
            
            # 提取cookies
            cookies = {}
            for cookie in session.cookies:
                cookies[cookie.name] = cookie.value
            
            # 添加一些必要的cookies如果不存在
            current_time = str(int(time.time()))
            
            if 'privacy' not in cookies:
                cookies['privacy'] = current_time
                
            if 'guid' not in cookies:
                # 生成一个随机的guid
                import uuid
                cookies['guid'] = str(uuid.uuid4()).replace('-', '')
            
            logger.info(f"成功获取cookies: {list(cookies.keys())}")
            return cookies
            
        except Exception as e:
            logger.error(f"获取cookies失败: {e}")
            return None
    
    def get_fallback_cookies(self):
        """获取备用的cookies（当自动获取失败时使用）"""
        logger.warning("使用备用cookies")
        return {
            "privacy": str(int(time.time())),
            "guid": "21ae369c1fecc95608a454bacdd16b41",
            "acw_tc": "ac11000117130121464015687e00d6b80852550fe03e0fd08cf69bf654d5a3",
            "JSESSIONID": "2F0AFC4C5819D899810516F3424C7A87",
            "NSC_ohjoy-bmjzvo-200-159": "ffffffffc3a0d42e45525d5f4f58455e445a4a423660",
        }
    
    def get_cookies(self):
        """获取cookies的主方法，优先尝试自动获取，失败则使用备用cookies"""
        cookies = self.get_fresh_cookies()
        if cookies:
            return cookies
        else:
            logger.warning("自动获取cookies失败，使用备用cookies")
            return self.get_fallback_cookies()
    
    def refresh_cookies(self):
        """刷新cookies，强制重新获取"""
        logger.info("强制刷新cookies")
        return self.get_fresh_cookies() or self.get_fallback_cookies()