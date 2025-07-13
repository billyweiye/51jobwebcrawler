#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
网络请求处理器
整合Cookie处理、代理管理、用户代理轮换和重试机制
"""

import time
import random
import logging
import requests
from typing import Dict, Optional, Any, Union, Tuple
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .cookie_handler import CookieHandler, EnhancedCookieHandler
from .proxy_manager import ProxyManager, ProxyInfo
from .user_agent import UserAgentRotator
from .retry_handler import RetryHandler, retry_on_network_error
from .config_manager import ConfigManager

logger = logging.getLogger(__name__)


@dataclass
class RequestStats:
    """请求统计信息"""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    retried_requests: int = 0
    proxy_requests: int = 0
    cookie_updates: int = 0
    total_response_time: float = 0.0
    
    @property
    def success_rate(self) -> float:
        """成功率"""
        if self.total_requests == 0:
            return 0.0
        return self.successful_requests / self.total_requests
    
    @property
    def average_response_time(self) -> float:
        """平均响应时间"""
        if self.successful_requests == 0:
            return 0.0
        return self.total_response_time / self.successful_requests


class RequestHandler:
    """网络请求处理器"""
    
    def __init__(self,
                 config_manager: Optional[ConfigManager] = None,
                 cookie_handler: Optional[CookieHandler] = None,
                 proxy_manager: Optional[ProxyManager] = None,
                 user_agent_rotator: Optional[UserAgentRotator] = None,
                 retry_handler: Optional[RetryHandler] = None):
        """
        初始化请求处理器
        
        Args:
            config_manager: 配置管理器
            cookie_handler: Cookie处理器
            proxy_manager: 代理管理器
            user_agent_rotator: 用户代理轮换器
            retry_handler: 重试处理器
        """
        self.config_manager = config_manager
        self.cookie_handler = cookie_handler or EnhancedCookieHandler()
        self.proxy_manager = proxy_manager
        self.user_agent_rotator = user_agent_rotator or UserAgentRotator()
        self.retry_handler = retry_handler or RetryHandler()
        
        # 统计信息
        self.stats = RequestStats()
        
        # 会话配置
        self.session = self._create_session()
        
        # 请求配置
        self._load_request_config()
        
        logger.info("请求处理器初始化完成")
    
    def _create_session(self) -> requests.Session:
        """创建请求会话"""
        session = requests.Session()
        
        # 配置重试策略
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _load_request_config(self):
        """加载请求配置"""
        if self.config_manager:
            request_config = self.config_manager.get_request_config()
            self.default_timeout = request_config.timeout
            self.download_delay = request_config.download_delay
            self.max_retries = request_config.max_retries
            self.respect_robots_txt = request_config.respect_robots_txt
        else:
            self.default_timeout = 30
            self.download_delay = (1, 3)
            self.max_retries = 3
            self.respect_robots_txt = True
        
        logger.debug(f"请求配置加载完成: timeout={self.default_timeout}, delay={self.download_delay}")
    
    def _get_delay(self) -> float:
        """获取下载延迟"""
        if isinstance(self.download_delay, (list, tuple)) and len(self.download_delay) == 2:
            return random.uniform(self.download_delay[0], self.download_delay[1])
        elif isinstance(self.download_delay, (int, float)):
            return float(self.download_delay)
        else:
            return 1.0
    
    def _prepare_headers(self, headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """准备请求头"""
        default_headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }
        
        # 添加用户代理
        if self.user_agent_rotator.is_enabled():
            default_headers['User-Agent'] = self.user_agent_rotator.get_random()
        
        # 合并自定义头
        if headers:
            default_headers.update(headers)
        
        return default_headers
    
    def _prepare_cookies(self) -> Dict[str, str]:
        """准备Cookie"""
        return self.cookie_handler.get_cookies()
    
    def _prepare_proxies(self) -> Optional[Dict[str, str]]:
        """准备代理"""
        if not self.proxy_manager or not self.proxy_manager.is_enabled():
            return None
        
        proxy_info = self.proxy_manager.get_proxy()
        if not proxy_info:
            return None
        
        return {
            'http': proxy_info.url,
            'https': proxy_info.url
        }
    
    def _handle_response(self, response: requests.Response, 
                        proxy_info: Optional[ProxyInfo] = None,
                        start_time: Optional[float] = None) -> Tuple[bool, bool]:
        """
        处理响应
        
        Args:
            response: HTTP响应
            proxy_info: 使用的代理信息
            start_time: 请求开始时间
            
        Returns:
            (是否成功, 是否需要重试)
        """
        response_time = time.time() - start_time if start_time else None
        
        # 检查Cookie验证
        needs_retry = False
        if hasattr(self.cookie_handler, 'smart_cookie_update'):
            needs_retry = self.cookie_handler.smart_cookie_update(response)
        else:
            needs_retry = self.cookie_handler.process_response(response)
        
        if needs_retry:
            self.stats.cookie_updates += 1
            logger.info("检测到Cookie验证，需要重试")
            return False, True
        
        # 检查响应状态
        if response.status_code == 200:
            # 成功
            if proxy_info and self.proxy_manager:
                self.proxy_manager.mark_proxy_success(proxy_info, response_time)
            
            self.stats.successful_requests += 1
            if response_time:
                self.stats.total_response_time += response_time
            
            return True, False
        
        elif response.status_code in [403, 429]:
            # 可能被封禁
            if proxy_info and self.proxy_manager:
                self.proxy_manager.mark_proxy_banned(proxy_info)
            
            logger.warning(f"请求被拒绝: {response.status_code}")
            return False, True
        
        elif response.status_code >= 500:
            # 服务器错误，可以重试
            if proxy_info and self.proxy_manager:
                self.proxy_manager.mark_proxy_failure(proxy_info, f"HTTP {response.status_code}")
            
            logger.warning(f"服务器错误: {response.status_code}")
            return False, True
        
        else:
            # 其他错误
            if proxy_info and self.proxy_manager:
                self.proxy_manager.mark_proxy_failure(proxy_info, f"HTTP {response.status_code}")
            
            logger.warning(f"请求失败: {response.status_code}")
            return False, False
    
    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        执行单次请求
        
        Args:
            method: HTTP方法
            url: 请求URL
            **kwargs: 其他请求参数
            
        Returns:
            HTTP响应
        """
        # 准备请求参数
        headers = self._prepare_headers(kwargs.pop('headers', None))
        cookies = self._prepare_cookies()
        proxies = self._prepare_proxies()
        timeout = kwargs.pop('timeout', self.default_timeout)
        
        # 记录使用的代理
        proxy_info = None
        if proxies and self.proxy_manager:
            # 从代理URL找到对应的ProxyInfo
            proxy_url = proxies.get('http', '')
            for proxy in self.proxy_manager.get_proxy_list():
                if proxy.url == proxy_url:
                    proxy_info = proxy
                    break
        
        # 执行请求
        start_time = time.time()
        
        try:
            response = self.session.request(
                method=method,
                url=url,
                headers=headers,
                cookies=cookies,
                proxies=proxies,
                timeout=timeout,
                **kwargs
            )
            
            logger.debug(f"请求完成: {method} {url} -> {response.status_code}")
            return response
            
        except Exception as e:
            # 标记代理失败
            if proxy_info and self.proxy_manager:
                self.proxy_manager.mark_proxy_failure(proxy_info, str(e))
            
            logger.error(f"请求异常: {method} {url} -> {e}")
            raise
    
    @retry_on_network_error
    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        执行HTTP请求 (带重试)
        
        Args:
            method: HTTP方法
            url: 请求URL
            **kwargs: 其他请求参数
            
        Returns:
            HTTP响应
            
        Raises:
            requests.RequestException: 请求失败
        """
        self.stats.total_requests += 1
        
        # 下载延迟
        delay = self._get_delay()
        if delay > 0:
            time.sleep(delay)
        
        max_attempts = kwargs.pop('max_retries', self.max_retries)
        attempt = 0
        
        while attempt < max_attempts:
            try:
                start_time = time.time()
                response = self._make_request(method, url, **kwargs)
                
                # 处理响应
                success, needs_retry = self._handle_response(response, start_time=start_time)
                
                if success:
                    return response
                
                if not needs_retry:
                    # 不需要重试的错误
                    self.stats.failed_requests += 1
                    response.raise_for_status()
                
                # 需要重试
                attempt += 1
                if attempt < max_attempts:
                    self.stats.retried_requests += 1
                    retry_delay = self.retry_handler.calculate_delay(attempt)
                    logger.info(f"请求重试 {attempt}/{max_attempts}，延迟 {retry_delay:.2f}s")
                    time.sleep(retry_delay)
                
            except Exception as e:
                attempt += 1
                if attempt >= max_attempts:
                    self.stats.failed_requests += 1
                    logger.error(f"请求最终失败: {method} {url} -> {e}")
                    raise
                
                self.stats.retried_requests += 1
                retry_delay = self.retry_handler.calculate_delay(attempt)
                logger.warning(f"请求异常重试 {attempt}/{max_attempts}，延迟 {retry_delay:.2f}s: {e}")
                time.sleep(retry_delay)
        
        # 所有重试都失败
        self.stats.failed_requests += 1
        raise requests.RequestException(f"请求失败，已重试 {max_attempts} 次")
    
    def get(self, url: str, **kwargs) -> requests.Response:
        """GET请求"""
        return self.request('GET', url, **kwargs)
    
    def post(self, url: str, **kwargs) -> requests.Response:
        """POST请求"""
        return self.request('POST', url, **kwargs)
    
    def put(self, url: str, **kwargs) -> requests.Response:
        """PUT请求"""
        return self.request('PUT', url, **kwargs)
    
    def delete(self, url: str, **kwargs) -> requests.Response:
        """DELETE请求"""
        return self.request('DELETE', url, **kwargs)
    
    def head(self, url: str, **kwargs) -> requests.Response:
        """HEAD请求"""
        return self.request('HEAD', url, **kwargs)
    
    def options(self, url: str, **kwargs) -> requests.Response:
        """OPTIONS请求"""
        return self.request('OPTIONS', url, **kwargs)
    
    def get_json(self, url: str, **kwargs) -> Dict[str, Any]:
        """
        GET请求并返回JSON数据
        
        Args:
            url: 请求URL
            **kwargs: 其他请求参数
            
        Returns:
            JSON数据字典
        """
        response = self.get(url, **kwargs)
        try:
            return response.json()
        except ValueError as e:
            logger.error(f"解析JSON失败: {e}")
            logger.debug(f"响应内容: {response.text[:500]}...")
            raise
    
    def post_json(self, url: str, json_data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """
        POST JSON数据并返回JSON响应
        
        Args:
            url: 请求URL
            json_data: 要发送的JSON数据
            **kwargs: 其他请求参数
            
        Returns:
            JSON响应数据
        """
        headers = kwargs.get('headers', {})
        headers['Content-Type'] = 'application/json'
        kwargs['headers'] = headers
        
        response = self.post(url, json=json_data, **kwargs)
        try:
            return response.json()
        except ValueError as e:
            logger.error(f"解析JSON响应失败: {e}")
            logger.debug(f"响应内容: {response.text[:500]}...")
            raise
    
    def download_file(self, url: str, file_path: str, chunk_size: int = 8192, **kwargs) -> bool:
        """
        下载文件
        
        Args:
            url: 文件URL
            file_path: 保存路径
            chunk_size: 块大小
            **kwargs: 其他请求参数
            
        Returns:
            是否下载成功
        """
        try:
            kwargs['stream'] = True
            response = self.get(url, **kwargs)
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
            
            logger.info(f"文件下载成功: {url} -> {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"文件下载失败: {url} -> {e}")
            return False
    
    def check_robots_txt(self, base_url: str, user_agent: str = '*') -> bool:
        """
        检查robots.txt
        
        Args:
            base_url: 网站基础URL
            user_agent: 用户代理
            
        Returns:
            是否允许访问
        """
        if not self.respect_robots_txt:
            return True
        
        try:
            robots_url = urljoin(base_url, '/robots.txt')
            response = self.get(robots_url, timeout=10)
            
            if response.status_code == 200:
                # 简单的robots.txt解析
                lines = response.text.split('\n')
                current_user_agent = None
                
                for line in lines:
                    line = line.strip()
                    if line.startswith('User-agent:'):
                        current_user_agent = line.split(':', 1)[1].strip()
                    elif line.startswith('Disallow:') and current_user_agent in ['*', user_agent]:
                        disallowed_path = line.split(':', 1)[1].strip()
                        if disallowed_path == '/':
                            logger.warning(f"robots.txt禁止访问: {base_url}")
                            return False
                
                logger.debug(f"robots.txt检查通过: {base_url}")
                return True
            
            # robots.txt不存在，允许访问
            return True
            
        except Exception as e:
            logger.warning(f"检查robots.txt失败: {e}，默认允许访问")
            return True
    
    def get_stats(self) -> RequestStats:
        """
        获取请求统计信息
        
        Returns:
            请求统计信息
        """
        return self.stats
    
    def reset_stats(self):
        """重置统计信息"""
        self.stats = RequestStats()
        logger.info("请求统计信息已重置")
    
    def get_detailed_stats(self) -> Dict[str, Any]:
        """
        获取详细统计信息
        
        Returns:
            详细统计信息字典
        """
        stats = {
            'request_stats': self.stats,
            'cookie_stats': self.cookie_handler.get_stats() if hasattr(self.cookie_handler, 'get_stats') else None,
            'proxy_stats': self.proxy_manager.get_stats() if self.proxy_manager else None,
            'user_agent_stats': self.user_agent_rotator.get_stats() if hasattr(self.user_agent_rotator, 'get_stats') else None
        }
        
        return stats
    
    def close(self):
        """关闭会话"""
        if self.session:
            self.session.close()
            logger.info("请求会话已关闭")
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close()
    
    def __str__(self) -> str:
        """字符串表示"""
        return f"RequestHandler(requests={self.stats.total_requests}, success_rate={self.stats.success_rate:.2%})"
    
    def __repr__(self) -> str:
        """对象表示"""
        return self.__str__()


def create_request_handler_from_config(config_manager: ConfigManager) -> RequestHandler:
    """
    从配置管理器创建请求处理器
    
    Args:
        config_manager: 配置管理器实例
        
    Returns:
        请求处理器实例
    """
    # 创建各个组件
    cookie_handler = EnhancedCookieHandler()
    
    # 创建代理管理器 (如果启用)
    proxy_manager = None
    proxy_config = config_manager.get_proxy_config()
    if proxy_config.get('enabled', False):
        from .proxy_manager import create_proxy_manager_from_config
        proxy_manager = create_proxy_manager_from_config(config_manager)
    
    # 创建用户代理轮换器
    from .user_agent import create_user_agent_rotator
    user_agent_rotator = create_user_agent_rotator(config_manager)
    
    # 创建重试处理器
    from .retry_handler import RetryConfig
    retry_config = config_manager.get_request_config()
    retry_handler_config = RetryConfig(
        max_attempts=retry_config.max_retries,
        base_delay=retry_config.retry_delay,
        max_delay=retry_config.max_retry_delay
    )
    retry_handler = RetryHandler(retry_handler_config)
    
    # 创建请求处理器
    request_handler = RequestHandler(
        config_manager=config_manager,
        cookie_handler=cookie_handler,
        proxy_manager=proxy_manager,
        user_agent_rotator=user_agent_rotator,
        retry_handler=retry_handler
    )
    
    logger.info(f"从配置创建请求处理器: {request_handler}")
    return request_handler