import time
import random
import logging
import threading
import schedule
from datetime import datetime, timedelta
from cookie_manager import CookieManager
import requests
import json
import urllib.parse

logger = logging.getLogger(__name__)

class AuthManager:
    """认证信息管理器，负责定时更新登录状态和认证信息"""
    
    def __init__(self, update_interval_hours=24):
        """
        初始化认证管理器
        
        Args:
            update_interval_hours (int): 更新间隔（小时），默认24小时
        """
        self.update_interval_hours = update_interval_hours
        self.cookie_manager = CookieManager()
        self.current_cookies = None
        self.current_headers = None
        self.last_update_time = None
        self.is_running = False
        self.scheduler_thread = None
        
        # 初始化认证信息
        self.refresh_auth_info()
        
        logger.info(f"AuthManager初始化完成，更新间隔: {update_interval_hours}小时")
    
    def get_fresh_property_value(self, page_url=None, keyword=None, job_area=None):
        """获取最新的property值
        
        Args:
            page_url (str, optional): 页面URL，如果不提供则动态生成
            keyword (str, optional): 搜索关键词，用于生成URL
            job_area (str, optional): 工作地区代码，用于生成URL
        """
        try:
            # 如果没有提供page_url，则动态生成
            if page_url is None:
                if keyword and job_area:
                    # URL编码关键词
                    encoded_keyword = urllib.parse.quote(keyword)
                    page_url = f"https://we.51job.com/pc/search?jobArea={job_area}&keyword={encoded_keyword}&searchType=2"
                else:
                    # 使用默认的搜索页面
                    page_url = "https://we.51job.com/pc/search?searchType=2"
            
            # 构造property对象
            property_obj = {
                "partner": "",
                "webId": 2,
                "fromdomain": "51job_web",
                "frompageUrl": "https://we.51job.com/",
                "pageUrl": page_url,
                "identityType": "",
                "userType": "",
                "isLogin": "是",
                "accountid": "96938878",
                "keywordType": ""
            }
            
            # 转换为JSON字符串并URL编码
            property_json = json.dumps(property_obj, separators=(',', ':'), ensure_ascii=False)
            property_encoded = urllib.parse.quote(property_json)
            
            logger.info("生成新的property值")
            return property_encoded
            
        except Exception as e:
            logger.error(f"生成property值失败: {e}")
            # 返回备用的property值
            return "%7B%22partner%22%3A%22%22%2C%22webId%22%3A2%2C%22fromdomain%22%3A%2251job_web%22%2C%22frompageUrl%22%3A%22https%3A%2F%2Fwe.51job.com%2F%22%2C%22pageUrl%22%3A%22https%3A%2F%2Fwe.51job.com%2Fpc%2Fsearch%3FjobArea%3D020000%26keyword%3D%25E6%2595%25B0%25E6%258D%25AE%25E5%2588%2586%25E6%259E%2590%25E5%B8%88%26searchType%3D2%22%2C%22identityType%22%3A%22%22%2C%22userType%22%3A%22%22%2C%22isLogin%22%3A%22%E6%98%AF%22%2C%22accountid%22%3A%2296938878%22%2C%22keywordType%22%3A%22%22%7D"
    
    def get_fresh_sign_value(self):
        """获取最新的sign值（这里使用固定值，实际项目中可能需要动态计算）"""
        # 在实际项目中，sign值可能需要根据特定算法动态生成
        # 这里先使用固定值，后续可以根据需要优化
        return "f858c5f418f30e4dd65f3c7f86d03a8e87e87302695161c82a9b37f04d790287"
    
    def get_fresh_uuid_value(self):
        """获取最新的uuid值"""
        # uuid通常是固定的设备标识，这里使用固定值
        return "21ae369c1fecc95608a454bacdd16b4"
    
    def refresh_auth_info(self, keyword=None, job_area=None, page_url=None):
        """刷新所有认证信息
        
        Args:
            keyword (str, optional): 搜索关键词，用于生成动态URL
            job_area (str, optional): 工作地区代码，用于生成动态URL
            page_url (str, optional): 直接指定页面URL
        """
        try:
            logger.info("开始刷新认证信息")
            
            # 1. 刷新cookies
            fresh_cookies = self.cookie_manager.refresh_cookies()
            if fresh_cookies:
                self.current_cookies = fresh_cookies
                logger.info(f"cookies刷新成功: {list(fresh_cookies.keys())}")
            else:
                logger.warning("cookies刷新失败，使用备用cookies")
                self.current_cookies = self.cookie_manager.get_fallback_cookies()
            
            # 2. 生成动态的referer URL
            if page_url:
                referer_url = page_url
            elif keyword and job_area:
                encoded_keyword = urllib.parse.quote(keyword)
                referer_url = f"https://we.51job.com/pc/search?jobArea={job_area}&keyword={encoded_keyword}&searchType=2"
            else:
                referer_url = "https://we.51job.com/pc/search?searchType=2"
            
            # 3. 生成新的headers
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
            ]
            
            selected_ua = random.choice(user_agents)
            
            self.current_headers = {
                "Accept": "application/json, text/plain, */*",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
                "DNT": "1",
                "From-Domain": "51job_web",
                "Host": "we.51job.com",
                "Referer": referer_url,
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "User-Agent": selected_ua,
                "account-id": "96938878",
                "partner": "",
                "property": self.get_fresh_property_value(page_url, keyword, job_area),
                "sec-ch-ua": '"Google Chrome";v="131","Chromium";v="131","Not_A Brand";v="24"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sign": self.get_fresh_sign_value(),
                "user-token": "",
                "uuid": self.get_fresh_uuid_value(),
            }
            
            self.last_update_time = datetime.now()
            logger.info(f"认证信息刷新完成，时间: {self.last_update_time}")
            return True
            
        except Exception as e:
            logger.error(f"刷新认证信息失败: {e}")
            return False
    
    def get_current_auth_info(self, keyword=None, job_area=None, page_url=None):
        """获取当前的认证信息
        
        Args:
            keyword (str, optional): 搜索关键词，用于生成动态URL
            job_area (str, optional): 工作地区代码，用于生成动态URL
            page_url (str, optional): 直接指定页面URL
        """
        # 如果传入了搜索参数，需要重新生成认证信息以匹配当前搜索
        need_refresh = False
        
        if keyword or job_area or page_url:
            # 有搜索参数时，总是刷新以生成匹配的认证信息
            need_refresh = True
            logger.info(f"检测到搜索参数，重新生成认证信息: keyword={keyword}, job_area={job_area}")
        elif self.should_update():
            # 无搜索参数但认证信息过期时，使用默认参数刷新
            need_refresh = True
            logger.info("检测到认证信息需要更新")
        
        if need_refresh:
            self.refresh_auth_info(keyword, job_area, page_url)
        
        return {
            'cookies': self.current_cookies,
            'headers': self.current_headers,
            'last_update': self.last_update_time
        }
    
    def should_update(self):
        """检查是否需要更新认证信息"""
        if self.last_update_time is None:
            return True
        
        time_diff = datetime.now() - self.last_update_time
        return time_diff.total_seconds() > (self.update_interval_hours * 3600)
    
    def start_auto_update(self):
        """启动自动更新任务"""
        if self.is_running:
            logger.warning("自动更新任务已在运行")
            return
        
        self.is_running = True
        
        # 配置定时任务
        schedule.clear()  # 清除之前的任务
        schedule.every(self.update_interval_hours).hours.do(self._scheduled_update)
        
        # 启动调度器线程
        self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.scheduler_thread.start()
        
        logger.info(f"自动更新任务已启动，更新间隔: {self.update_interval_hours}小时")
    
    def stop_auto_update(self):
        """停止自动更新任务"""
        self.is_running = False
        schedule.clear()
        logger.info("自动更新任务已停止")
    
    def _scheduled_update(self):
        """定时更新任务"""
        logger.info("执行定时认证信息更新")
        success = self.refresh_auth_info()
        if success:
            logger.info("定时更新成功")
        else:
            logger.error("定时更新失败")
    
    def _run_scheduler(self):
        """运行调度器"""
        while self.is_running:
            try:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
            except Exception as e:
                logger.error(f"调度器运行异常: {e}")
                time.sleep(60)
    
    def get_status(self):
        """获取认证管理器状态"""
        return {
            'is_running': self.is_running,
            'last_update_time': self.last_update_time,
            'update_interval_hours': self.update_interval_hours,
            'cookies_count': len(self.current_cookies) if self.current_cookies else 0,
            'next_update_time': self.last_update_time + timedelta(hours=self.update_interval_hours) if self.last_update_time else None
        }

# 全局认证管理器实例
auth_manager = AuthManager(update_interval_hours=2)

def get_auth_manager():
    """获取全局认证管理器实例"""
    return auth_manager