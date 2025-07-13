#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cookie处理器
用于处理51job的acw_sc__v2验证机制
"""

import re
import time
import logging
from typing import Dict, Optional, Any
from dataclasses import dataclass
from .retry_handler import retry_on_network_error

logger = logging.getLogger(__name__)


@dataclass
class CookieStats:
    """Cookie统计信息"""
    total_requests: int = 0
    cookie_updates: int = 0
    validation_failures: int = 0
    last_update_time: Optional[float] = None


class CookieHandler:
    """Cookie处理器"""
    
    def __init__(self):
        """初始化Cookie处理器"""
        self.cookies: Dict[str, str] = {}
        self.stats = CookieStats()
        self._init_default_cookies()
    
    def _init_default_cookies(self):
        """初始化默认Cookie"""
        current_time = int(time.time())
        self.cookies = {
            "privacy": str(current_time),
            "guid": "21ae369c1fecc95608a454bacdd16b41",
            "acw_tc": f"ac11000117130121464015687e00d6b80852550fe03e0fd08cf69bf654d5a3",
            "JSESSIONID": "2F0AFC4C5819D899810516F3424C7A87",
            "NSC_ohjoy-bmjzvo-200-159": "ffffffffc3a0d42e45525d5f4f58455e445a4a423660",
        }
        logger.info("初始化默认Cookie")
    
    def get_cookies(self) -> Dict[str, str]:
        """
        获取当前Cookie字典
        
        Returns:
            Cookie字典
        """
        return self.cookies.copy()
    
    def update_cookie(self, key: str, value: str):
        """
        更新指定Cookie
        
        Args:
            key: Cookie键
            value: Cookie值
        """
        old_value = self.cookies.get(key)
        self.cookies[key] = value
        
        if old_value != value:
            self.stats.cookie_updates += 1
            self.stats.last_update_time = time.time()
            logger.info(f"更新Cookie: {key} = {value[:20]}...")
    
    def extract_acw_sc_v2(self, response_text: str) -> Optional[str]:
        """
        从响应文本中提取acw_sc__v2参数
        
        Args:
            response_text: HTTP响应文本
            
        Returns:
            提取的arg1参数，用于生成acw_sc__v2
        """
        try:
            # 查找arg1参数
            pattern = r"var arg1='([A-F0-9]+)';"
            match = re.search(pattern, response_text)
            
            if match:
                arg1 = match.group(1)
                logger.debug(f"提取到arg1参数: {arg1}")
                return arg1
            else:
                logger.debug("未找到arg1参数")
                return None
                
        except Exception as e:
            logger.error(f"提取acw_sc_v2参数失败: {e}")
            return None
    
    def generate_acw_sc_v2(self, arg1: str) -> str:
        """
        生成acw_sc__v2 Cookie值
        
        Args:
            arg1: 从响应中提取的参数
            
        Returns:
            生成的acw_sc__v2值
        """
        try:
            # 使用原有的算法生成acw_sc__v2
            import sys
            import os
            
            # 添加项目根目录到路径
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(current_dir)
            if project_root not in sys.path:
                sys.path.insert(0, project_root)
            
            from acwCookie import getAcwScV2
            acw_sc_v2 = getAcwScV2(arg1)
            logger.debug(f"生成acw_sc__v2: {acw_sc_v2}")
            return acw_sc_v2
            
        except Exception as e:
            logger.error(f"生成acw_sc__v2失败: {e}")
            raise
    
    def process_response(self, response) -> bool:
        """
        处理HTTP响应，检查是否需要更新Cookie
        
        Args:
            response: HTTP响应对象
            
        Returns:
            是否需要重新请求
        """
        self.stats.total_requests += 1
        
        try:
            # 检查是否需要验证
            if not self._needs_cookie_validation(response):
                return False
            
            # 提取arg1参数
            arg1 = self.extract_acw_sc_v2(response.text)
            if not arg1:
                logger.warning("未能提取arg1参数，可能不需要Cookie验证")
                return False
            
            # 生成新的acw_sc__v2
            acw_sc_v2 = self.generate_acw_sc_v2(arg1)
            
            # 更新Cookie
            self.update_cookie('acw_sc__v2', acw_sc_v2)
            
            logger.info("Cookie验证处理完成，需要重新请求")
            return True
            
        except Exception as e:
            self.stats.validation_failures += 1
            logger.error(f"处理响应Cookie失败: {e}")
            return False
    
    def _needs_cookie_validation(self, response) -> bool:
        """
        检查响应是否需要Cookie验证
        
        Args:
            response: HTTP响应对象
            
        Returns:
            是否需要验证
        """
        # 检查状态码
        if response.status_code != 200:
            return False
        
        # 检查响应内容
        if not response.text:
            return False
        
        # 检查是否包含验证脚本
        validation_indicators = [
            "var arg1=",
            "acw_sc__v2",
            "document.cookie"
        ]
        
        return any(indicator in response.text for indicator in validation_indicators)
    
    def refresh_cookies(self):
        """
        刷新Cookie (更新时间戳等)
        """
        current_time = int(time.time())
        self.update_cookie('privacy', str(current_time))
        
        # 可以添加其他需要刷新的Cookie
        logger.info("Cookie已刷新")
    
    def clear_acw_cookie(self):
        """
        清除acw_sc__v2 Cookie
        """
        if 'acw_sc__v2' in self.cookies:
            del self.cookies['acw_sc__v2']
            logger.info("已清除acw_sc__v2 Cookie")
    
    def is_cookie_valid(self) -> bool:
        """
        检查Cookie是否有效
        
        Returns:
            Cookie是否有效
        """
        required_cookies = ['privacy', 'guid']
        return all(key in self.cookies for key in required_cookies)
    
    def get_cookie_age(self) -> Optional[float]:
        """
        获取Cookie的年龄 (秒)
        
        Returns:
            Cookie年龄，如果无法确定则返回None
        """
        if self.stats.last_update_time:
            return time.time() - self.stats.last_update_time
        return None
    
    def should_refresh_cookies(self, max_age: int = 3600) -> bool:
        """
        检查是否应该刷新Cookie
        
        Args:
            max_age: Cookie最大年龄 (秒)
            
        Returns:
            是否应该刷新
        """
        age = self.get_cookie_age()
        if age is None:
            return True  # 如果无法确定年龄，建议刷新
        
        return age > max_age
    
    def get_stats(self) -> CookieStats:
        """
        获取Cookie统计信息
        
        Returns:
            Cookie统计信息
        """
        return self.stats
    
    def reset_stats(self):
        """重置统计信息"""
        self.stats = CookieStats()
        logger.info("Cookie统计信息已重置")
    
    def export_cookies(self) -> str:
        """
        导出Cookie为字符串格式
        
        Returns:
            Cookie字符串
        """
        cookie_pairs = [f"{key}={value}" for key, value in self.cookies.items()]
        return "; ".join(cookie_pairs)
    
    def import_cookies(self, cookie_string: str):
        """
        从字符串导入Cookie
        
        Args:
            cookie_string: Cookie字符串
        """
        try:
            pairs = cookie_string.split(';')
            for pair in pairs:
                if '=' in pair:
                    key, value = pair.strip().split('=', 1)
                    self.cookies[key] = value
            
            logger.info(f"导入了 {len(pairs)} 个Cookie")
            
        except Exception as e:
            logger.error(f"导入Cookie失败: {e}")
    
    def __str__(self) -> str:
        """字符串表示"""
        return f"CookieHandler(cookies={len(self.cookies)}, updates={self.stats.cookie_updates})"
    
    def __repr__(self) -> str:
        """对象表示"""
        return self.__str__()


class EnhancedCookieHandler(CookieHandler):
    """增强的Cookie处理器"""
    
    def __init__(self, auto_refresh: bool = True, max_cookie_age: int = 3600):
        """
        初始化增强Cookie处理器
        
        Args:
            auto_refresh: 是否自动刷新Cookie
            max_cookie_age: Cookie最大年龄 (秒)
        """
        super().__init__()
        self.auto_refresh = auto_refresh
        self.max_cookie_age = max_cookie_age
        self.last_refresh_time = time.time()
    
    @retry_on_network_error
    def smart_cookie_update(self, response) -> bool:
        """
        智能Cookie更新 (带重试)
        
        Args:
            response: HTTP响应对象
            
        Returns:
            是否需要重新请求
        """
        # 检查是否需要自动刷新
        if self.auto_refresh and self.should_refresh_cookies(self.max_cookie_age):
            self.refresh_cookies()
            self.last_refresh_time = time.time()
        
        # 处理响应
        return self.process_response(response)
    
    def get_enhanced_stats(self) -> Dict[str, Any]:
        """
        获取增强统计信息
        
        Returns:
            增强统计信息字典
        """
        base_stats = self.get_stats()
        enhanced_stats = {
            'base_stats': base_stats,
            'auto_refresh': self.auto_refresh,
            'max_cookie_age': self.max_cookie_age,
            'last_refresh_time': self.last_refresh_time,
            'cookie_age': self.get_cookie_age(),
            'needs_refresh': self.should_refresh_cookies()
        }
        
        return enhanced_stats