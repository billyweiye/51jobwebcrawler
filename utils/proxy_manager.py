#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
代理管理器
用于管理和轮换代理IP，支持代理池、健康检查和故障转移
"""

import time
import random
import logging
import requests
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed
from .retry_handler import retry_on_network_error

logger = logging.getLogger(__name__)


class ProxyStatus(Enum):
    """代理状态枚举"""
    ACTIVE = "active"          # 活跃
    INACTIVE = "inactive"      # 不活跃
    FAILED = "failed"          # 失败
    TESTING = "testing"        # 测试中
    BANNED = "banned"          # 被封禁


@dataclass
class ProxyInfo:
    """代理信息"""
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    protocol: str = "http"  # http, https, socks4, socks5
    status: ProxyStatus = ProxyStatus.INACTIVE
    last_used: Optional[float] = None
    last_checked: Optional[float] = None
    success_count: int = 0
    failure_count: int = 0
    response_time: Optional[float] = None
    error_message: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    
    @property
    def url(self) -> str:
        """获取代理URL"""
        if self.username and self.password:
            auth = f"{self.username}:{self.password}@"
        else:
            auth = ""
        return f"{self.protocol}://{auth}{self.host}:{self.port}"
    
    @property
    def success_rate(self) -> float:
        """获取成功率"""
        total = self.success_count + self.failure_count
        if total == 0:
            return 0.0
        return self.success_count / total
    
    @property
    def is_healthy(self) -> bool:
        """检查代理是否健康"""
        return (
            self.status == ProxyStatus.ACTIVE and
            self.success_rate >= 0.7 and
            self.failure_count < 10
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'host': self.host,
            'port': self.port,
            'username': self.username,
            'password': self.password,
            'protocol': self.protocol,
            'status': self.status.value,
            'last_used': self.last_used,
            'last_checked': self.last_checked,
            'success_count': self.success_count,
            'failure_count': self.failure_count,
            'response_time': self.response_time,
            'error_message': self.error_message,
            'tags': self.tags,
            'url': self.url,
            'success_rate': self.success_rate,
            'is_healthy': self.is_healthy
        }
    
    def __str__(self) -> str:
        return f"Proxy({self.host}:{self.port}, {self.status.value}, {self.success_rate:.2%})"


@dataclass
class ProxyStats:
    """代理统计信息"""
    total_proxies: int = 0
    active_proxies: int = 0
    failed_proxies: int = 0
    banned_proxies: int = 0
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    last_check_time: Optional[float] = None
    average_response_time: Optional[float] = None
    
    @property
    def success_rate(self) -> float:
        """总体成功率"""
        if self.total_requests == 0:
            return 0.0
        return self.successful_requests / self.total_requests
    
    @property
    def health_rate(self) -> float:
        """健康率"""
        if self.total_proxies == 0:
            return 0.0
        return self.active_proxies / self.total_proxies


class ProxyManager:
    """代理管理器"""
    
    def __init__(self, 
                 check_url: str = "http://httpbin.org/ip",
                 check_timeout: int = 10,
                 max_failures: int = 5,
                 health_check_interval: int = 300):
        """
        初始化代理管理器
        
        Args:
            check_url: 健康检查URL
            check_timeout: 检查超时时间
            max_failures: 最大失败次数
            health_check_interval: 健康检查间隔 (秒)
        """
        self.proxies: List[ProxyInfo] = []
        self.check_url = check_url
        self.check_timeout = check_timeout
        self.max_failures = max_failures
        self.health_check_interval = health_check_interval
        self.stats = ProxyStats()
        self.current_index = 0
        self.last_health_check = 0
        self._enabled = True
        
        logger.info("代理管理器初始化完成")
    
    def add_proxy(self, host: str, port: int, 
                  username: Optional[str] = None, 
                  password: Optional[str] = None,
                  protocol: str = "http",
                  tags: Optional[List[str]] = None) -> bool:
        """
        添加代理
        
        Args:
            host: 代理主机
            port: 代理端口
            username: 用户名
            password: 密码
            protocol: 协议类型
            tags: 标签列表
            
        Returns:
            是否添加成功
        """
        try:
            # 检查是否已存在
            for proxy in self.proxies:
                if proxy.host == host and proxy.port == port:
                    logger.warning(f"代理 {host}:{port} 已存在")
                    return False
            
            proxy_info = ProxyInfo(
                host=host,
                port=port,
                username=username,
                password=password,
                protocol=protocol,
                tags=tags or []
            )
            
            self.proxies.append(proxy_info)
            self.stats.total_proxies += 1
            
            logger.info(f"添加代理: {proxy_info}")
            return True
            
        except Exception as e:
            logger.error(f"添加代理失败: {e}")
            return False
    
    def add_proxies_from_list(self, proxy_list: List[Dict[str, Any]]) -> int:
        """
        从列表批量添加代理
        
        Args:
            proxy_list: 代理列表
            
        Returns:
            成功添加的代理数量
        """
        added_count = 0
        
        for proxy_data in proxy_list:
            try:
                if self.add_proxy(**proxy_data):
                    added_count += 1
            except Exception as e:
                logger.error(f"添加代理失败: {proxy_data}, 错误: {e}")
        
        logger.info(f"批量添加代理完成，成功添加 {added_count}/{len(proxy_list)} 个")
        return added_count
    
    def remove_proxy(self, host: str, port: int) -> bool:
        """
        移除代理
        
        Args:
            host: 代理主机
            port: 代理端口
            
        Returns:
            是否移除成功
        """
        for i, proxy in enumerate(self.proxies):
            if proxy.host == host and proxy.port == port:
                removed_proxy = self.proxies.pop(i)
                self.stats.total_proxies -= 1
                
                # 更新统计
                if removed_proxy.status == ProxyStatus.ACTIVE:
                    self.stats.active_proxies -= 1
                elif removed_proxy.status == ProxyStatus.FAILED:
                    self.stats.failed_proxies -= 1
                elif removed_proxy.status == ProxyStatus.BANNED:
                    self.stats.banned_proxies -= 1
                
                logger.info(f"移除代理: {removed_proxy}")
                return True
        
        logger.warning(f"未找到代理 {host}:{port}")
        return False
    
    def get_proxy(self, strategy: str = "round_robin") -> Optional[ProxyInfo]:
        """
        获取代理
        
        Args:
            strategy: 选择策略 (round_robin, random, best_performance)
            
        Returns:
            代理信息，如果没有可用代理则返回None
        """
        if not self._enabled:
            return None
        
        # 自动健康检查
        if self._should_health_check():
            self.health_check_all()
        
        # 获取健康的代理
        healthy_proxies = [p for p in self.proxies if p.is_healthy]
        
        if not healthy_proxies:
            logger.warning("没有可用的健康代理")
            return None
        
        # 根据策略选择代理
        if strategy == "round_robin":
            proxy = self._get_proxy_round_robin(healthy_proxies)
        elif strategy == "random":
            proxy = random.choice(healthy_proxies)
        elif strategy == "best_performance":
            proxy = self._get_best_performance_proxy(healthy_proxies)
        else:
            logger.warning(f"未知的代理选择策略: {strategy}，使用轮询")
            proxy = self._get_proxy_round_robin(healthy_proxies)
        
        if proxy:
            proxy.last_used = time.time()
            logger.debug(f"选择代理: {proxy}")
        
        return proxy
    
    def _get_proxy_round_robin(self, proxies: List[ProxyInfo]) -> Optional[ProxyInfo]:
        """轮询选择代理"""
        if not proxies:
            return None
        
        proxy = proxies[self.current_index % len(proxies)]
        self.current_index = (self.current_index + 1) % len(proxies)
        return proxy
    
    def _get_best_performance_proxy(self, proxies: List[ProxyInfo]) -> Optional[ProxyInfo]:
        """选择性能最佳的代理"""
        if not proxies:
            return None
        
        # 按成功率和响应时间排序
        def score_proxy(proxy: ProxyInfo) -> float:
            success_rate = proxy.success_rate
            response_time = proxy.response_time or 999.0
            # 成功率权重70%，响应时间权重30%
            return success_rate * 0.7 + (1.0 / (1.0 + response_time)) * 0.3
        
        return max(proxies, key=score_proxy)
    
    def mark_proxy_success(self, proxy: ProxyInfo, response_time: Optional[float] = None):
        """
        标记代理请求成功
        
        Args:
            proxy: 代理信息
            response_time: 响应时间
        """
        proxy.success_count += 1
        proxy.status = ProxyStatus.ACTIVE
        proxy.error_message = None
        
        if response_time is not None:
            proxy.response_time = response_time
        
        self.stats.successful_requests += 1
        self.stats.total_requests += 1
        
        logger.debug(f"代理请求成功: {proxy}")
    
    def mark_proxy_failure(self, proxy: ProxyInfo, error_message: str = ""):
        """
        标记代理请求失败
        
        Args:
            proxy: 代理信息
            error_message: 错误信息
        """
        proxy.failure_count += 1
        proxy.error_message = error_message
        
        # 根据失败次数更新状态
        if proxy.failure_count >= self.max_failures:
            proxy.status = ProxyStatus.FAILED
            logger.warning(f"代理失败次数过多，标记为失败: {proxy}")
        
        self.stats.failed_requests += 1
        self.stats.total_requests += 1
        
        logger.debug(f"代理请求失败: {proxy}, 错误: {error_message}")
    
    def mark_proxy_banned(self, proxy: ProxyInfo):
        """
        标记代理被封禁
        
        Args:
            proxy: 代理信息
        """
        proxy.status = ProxyStatus.BANNED
        logger.warning(f"代理被封禁: {proxy}")
    
    @retry_on_network_error
    def check_proxy(self, proxy: ProxyInfo) -> bool:
        """
        检查单个代理的健康状态
        
        Args:
            proxy: 代理信息
            
        Returns:
            代理是否健康
        """
        proxy.status = ProxyStatus.TESTING
        proxy.last_checked = time.time()
        
        try:
            start_time = time.time()
            
            proxies = {
                'http': proxy.url,
                'https': proxy.url
            }
            
            response = requests.get(
                self.check_url,
                proxies=proxies,
                timeout=self.check_timeout,
                verify=False
            )
            
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                proxy.status = ProxyStatus.ACTIVE
                proxy.response_time = response_time
                proxy.error_message = None
                logger.debug(f"代理检查成功: {proxy}, 响应时间: {response_time:.2f}s")
                return True
            else:
                proxy.status = ProxyStatus.FAILED
                proxy.error_message = f"HTTP {response.status_code}"
                logger.debug(f"代理检查失败: {proxy}, 状态码: {response.status_code}")
                return False
                
        except Exception as e:
            proxy.status = ProxyStatus.FAILED
            proxy.error_message = str(e)
            logger.debug(f"代理检查异常: {proxy}, 错误: {e}")
            return False
    
    def health_check_all(self, max_workers: int = 10) -> Dict[str, int]:
        """
        检查所有代理的健康状态
        
        Args:
            max_workers: 最大并发数
            
        Returns:
            检查结果统计
        """
        if not self.proxies:
            logger.info("没有代理需要检查")
            return {'total': 0, 'healthy': 0, 'unhealthy': 0}
        
        logger.info(f"开始健康检查，共 {len(self.proxies)} 个代理")
        
        healthy_count = 0
        unhealthy_count = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有检查任务
            future_to_proxy = {
                executor.submit(self.check_proxy, proxy): proxy 
                for proxy in self.proxies
            }
            
            # 收集结果
            for future in as_completed(future_to_proxy):
                proxy = future_to_proxy[future]
                try:
                    is_healthy = future.result()
                    if is_healthy:
                        healthy_count += 1
                    else:
                        unhealthy_count += 1
                except Exception as e:
                    logger.error(f"检查代理 {proxy} 时发生异常: {e}")
                    unhealthy_count += 1
        
        # 更新统计
        self._update_stats()
        self.last_health_check = time.time()
        
        result = {
            'total': len(self.proxies),
            'healthy': healthy_count,
            'unhealthy': unhealthy_count
        }
        
        logger.info(f"健康检查完成: {result}")
        return result
    
    def _should_health_check(self) -> bool:
        """检查是否应该进行健康检查"""
        return (time.time() - self.last_health_check) > self.health_check_interval
    
    def _update_stats(self):
        """更新统计信息"""
        self.stats.total_proxies = len(self.proxies)
        self.stats.active_proxies = len([p for p in self.proxies if p.status == ProxyStatus.ACTIVE])
        self.stats.failed_proxies = len([p for p in self.proxies if p.status == ProxyStatus.FAILED])
        self.stats.banned_proxies = len([p for p in self.proxies if p.status == ProxyStatus.BANNED])
        self.stats.last_check_time = time.time()
        
        # 计算平均响应时间
        response_times = [p.response_time for p in self.proxies if p.response_time is not None]
        if response_times:
            self.stats.average_response_time = sum(response_times) / len(response_times)
    
    def get_stats(self) -> ProxyStats:
        """
        获取统计信息
        
        Returns:
            代理统计信息
        """
        self._update_stats()
        return self.stats
    
    def get_proxy_list(self, status_filter: Optional[ProxyStatus] = None) -> List[ProxyInfo]:
        """
        获取代理列表
        
        Args:
            status_filter: 状态过滤器
            
        Returns:
            代理列表
        """
        if status_filter is None:
            return self.proxies.copy()
        
        return [p for p in self.proxies if p.status == status_filter]
    
    def clear_failed_proxies(self) -> int:
        """
        清除失败的代理
        
        Returns:
            清除的代理数量
        """
        failed_proxies = [p for p in self.proxies if p.status == ProxyStatus.FAILED]
        
        for proxy in failed_proxies:
            self.remove_proxy(proxy.host, proxy.port)
        
        logger.info(f"清除了 {len(failed_proxies)} 个失败的代理")
        return len(failed_proxies)
    
    def reset_proxy_stats(self):
        """重置代理统计"""
        for proxy in self.proxies:
            proxy.success_count = 0
            proxy.failure_count = 0
            proxy.error_message = None
        
        self.stats = ProxyStats()
        logger.info("代理统计已重置")
    
    def enable(self):
        """启用代理管理器"""
        self._enabled = True
        logger.info("代理管理器已启用")
    
    def disable(self):
        """禁用代理管理器"""
        self._enabled = False
        logger.info("代理管理器已禁用")
    
    def is_enabled(self) -> bool:
        """检查是否启用"""
        return self._enabled
    
    def export_proxies(self) -> List[Dict[str, Any]]:
        """
        导出代理列表
        
        Returns:
            代理字典列表
        """
        return [proxy.to_dict() for proxy in self.proxies]
    
    def import_proxies(self, proxy_data: List[Dict[str, Any]]) -> int:
        """
        导入代理列表
        
        Args:
            proxy_data: 代理数据列表
            
        Returns:
            导入的代理数量
        """
        imported_count = 0
        
        for data in proxy_data:
            try:
                proxy = ProxyInfo(
                    host=data['host'],
                    port=data['port'],
                    username=data.get('username'),
                    password=data.get('password'),
                    protocol=data.get('protocol', 'http'),
                    tags=data.get('tags', [])
                )
                
                # 恢复统计信息
                if 'success_count' in data:
                    proxy.success_count = data['success_count']
                if 'failure_count' in data:
                    proxy.failure_count = data['failure_count']
                if 'response_time' in data:
                    proxy.response_time = data['response_time']
                
                self.proxies.append(proxy)
                imported_count += 1
                
            except Exception as e:
                logger.error(f"导入代理失败: {data}, 错误: {e}")
        
        self._update_stats()
        logger.info(f"导入了 {imported_count} 个代理")
        return imported_count
    
    def __len__(self) -> int:
        """返回代理数量"""
        return len(self.proxies)
    
    def __str__(self) -> str:
        """字符串表示"""
        return f"ProxyManager(total={len(self.proxies)}, active={self.stats.active_proxies})"
    
    def __repr__(self) -> str:
        """对象表示"""
        return self.__str__()


def create_proxy_manager_from_config(config_manager) -> ProxyManager:
    """
    从配置管理器创建代理管理器
    
    Args:
        config_manager: 配置管理器实例
        
    Returns:
        代理管理器实例
    """
    proxy_config = config_manager.get_proxy_config()
    
    manager = ProxyManager(
        check_url=proxy_config.get('check_url', 'http://httpbin.org/ip'),
        check_timeout=proxy_config.get('check_timeout', 10),
        max_failures=proxy_config.get('max_failures', 5),
        health_check_interval=proxy_config.get('health_check_interval', 300)
    )
    
    # 添加代理列表
    proxy_list = proxy_config.get('proxy_list', [])
    if proxy_list:
        manager.add_proxies_from_list(proxy_list)
    
    # 启用/禁用
    if not proxy_config.get('enabled', True):
        manager.disable()
    
    logger.info(f"从配置创建代理管理器: {manager}")
    return manager