#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
重试处理器
实现指数退避重试机制
"""

import time
import random
import logging
from typing import Callable, Any, Optional, List, Type
from functools import wraps
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class RetryConfig:
    """重试配置类"""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    backoff_factor: float = 1.0
    

class RetryHandler:
    """重试处理器"""
    
    def __init__(self, config: Optional[RetryConfig] = None):
        """
        初始化重试处理器
        
        Args:
            config: 重试配置
        """
        self.config = config or RetryConfig()
        self.attempt_count = 0
        self.total_delay = 0.0
    
    def calculate_delay(self, attempt: int) -> float:
        """
        计算延迟时间 (指数退避)
        
        Args:
            attempt: 当前尝试次数 (从1开始)
            
        Returns:
            延迟时间 (秒)
        """
        # 基础指数退避计算
        delay = self.config.base_delay * (self.config.exponential_base ** (attempt - 1))
        delay *= self.config.backoff_factor
        
        # 限制最大延迟
        delay = min(delay, self.config.max_delay)
        
        # 添加随机抖动 (避免雷群效应)
        if self.config.jitter:
            jitter_range = delay * 0.1  # 10%的抖动
            delay += random.uniform(-jitter_range, jitter_range)
        
        return max(0, delay)
    
    def should_retry(self, attempt: int, exception: Exception, 
                    retry_exceptions: Optional[List[Type[Exception]]] = None) -> bool:
        """
        判断是否应该重试
        
        Args:
            attempt: 当前尝试次数
            exception: 发生的异常
            retry_exceptions: 可重试的异常类型列表
            
        Returns:
            是否应该重试
        """
        # 检查是否超过最大尝试次数
        if attempt >= self.config.max_attempts:
            return False
        
        # 检查异常类型是否可重试
        if retry_exceptions:
            return any(isinstance(exception, exc_type) for exc_type in retry_exceptions)
        
        # 默认所有异常都可重试
        return True
    
    def execute_with_retry(self, func: Callable, *args, 
                          retry_exceptions: Optional[List[Type[Exception]]] = None,
                          **kwargs) -> Any:
        """
        执行函数并在失败时重试
        
        Args:
            func: 要执行的函数
            *args: 函数参数
            retry_exceptions: 可重试的异常类型列表
            **kwargs: 函数关键字参数
            
        Returns:
            函数执行结果
            
        Raises:
            最后一次执行的异常
        """
        last_exception = None
        
        for attempt in range(1, self.config.max_attempts + 1):
            try:
                self.attempt_count = attempt
                logger.debug(f"执行函数 {func.__name__}，第 {attempt} 次尝试")
                
                result = func(*args, **kwargs)
                
                if attempt > 1:
                    logger.info(f"函数 {func.__name__} 在第 {attempt} 次尝试后成功")
                
                return result
                
            except Exception as e:
                last_exception = e
                
                if not self.should_retry(attempt, e, retry_exceptions):
                    logger.error(f"函数 {func.__name__} 执行失败，不再重试: {e}")
                    raise e
                
                if attempt < self.config.max_attempts:
                    delay = self.calculate_delay(attempt)
                    self.total_delay += delay
                    
                    logger.warning(
                        f"函数 {func.__name__} 第 {attempt} 次尝试失败: {e}, "
                        f"将在 {delay:.2f} 秒后重试"
                    )
                    
                    time.sleep(delay)
                else:
                    logger.error(
                        f"函数 {func.__name__} 在 {attempt} 次尝试后仍然失败: {e}"
                    )
        
        # 如果所有尝试都失败了，抛出最后一个异常
        if last_exception:
            raise last_exception
    
    def reset_stats(self):
        """重置统计信息"""
        self.attempt_count = 0
        self.total_delay = 0.0
    
    def get_stats(self) -> dict:
        """
        获取重试统计信息
        
        Returns:
            统计信息字典
        """
        return {
            'attempt_count': self.attempt_count,
            'total_delay': self.total_delay,
            'config': self.config
        }


def retry_on_exception(max_attempts: int = 3, 
                      base_delay: float = 1.0,
                      max_delay: float = 60.0,
                      exponential_base: float = 2.0,
                      jitter: bool = True,
                      retry_exceptions: Optional[List[Type[Exception]]] = None):
    """
    重试装饰器
    
    Args:
        max_attempts: 最大尝试次数
        base_delay: 基础延迟时间
        max_delay: 最大延迟时间
        exponential_base: 指数底数
        jitter: 是否添加随机抖动
        retry_exceptions: 可重试的异常类型列表
        
    Returns:
        装饰器函数
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            config = RetryConfig(
                max_attempts=max_attempts,
                base_delay=base_delay,
                max_delay=max_delay,
                exponential_base=exponential_base,
                jitter=jitter
            )
            
            retry_handler = RetryHandler(config)
            return retry_handler.execute_with_retry(
                func, *args, retry_exceptions=retry_exceptions, **kwargs
            )
        
        return wrapper
    return decorator


# 常用的重试异常类型
class NetworkError(Exception):
    """网络错误"""
    pass


class TemporaryError(Exception):
    """临时错误"""
    pass


class RateLimitError(Exception):
    """频率限制错误"""
    pass


# 预定义的重试配置
NETWORK_RETRY_CONFIG = RetryConfig(
    max_attempts=5,
    base_delay=2.0,
    max_delay=120.0,
    exponential_base=2.0,
    jitter=True
)

API_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=1.0,
    max_delay=30.0,
    exponential_base=1.5,
    jitter=True
)

DATABASE_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=0.5,
    max_delay=10.0,
    exponential_base=2.0,
    jitter=False
)


# 便捷的重试装饰器
def retry_on_network_error(func: Callable) -> Callable:
    """网络错误重试装饰器"""
    return retry_on_exception(
        max_attempts=NETWORK_RETRY_CONFIG.max_attempts,
        base_delay=NETWORK_RETRY_CONFIG.base_delay,
        max_delay=NETWORK_RETRY_CONFIG.max_delay,
        exponential_base=NETWORK_RETRY_CONFIG.exponential_base,
        jitter=NETWORK_RETRY_CONFIG.jitter,
        retry_exceptions=[NetworkError, ConnectionError, TimeoutError]
    )(func)


def retry_on_api_error(func: Callable) -> Callable:
    """API错误重试装饰器"""
    return retry_on_exception(
        max_attempts=API_RETRY_CONFIG.max_attempts,
        base_delay=API_RETRY_CONFIG.base_delay,
        max_delay=API_RETRY_CONFIG.max_delay,
        exponential_base=API_RETRY_CONFIG.exponential_base,
        jitter=API_RETRY_CONFIG.jitter,
        retry_exceptions=[NetworkError, TemporaryError, RateLimitError]
    )(func)


def retry_on_database_error(func: Callable) -> Callable:
    """数据库错误重试装饰器"""
    return retry_on_exception(
        max_attempts=DATABASE_RETRY_CONFIG.max_attempts,
        base_delay=DATABASE_RETRY_CONFIG.base_delay,
        max_delay=DATABASE_RETRY_CONFIG.max_delay,
        exponential_base=DATABASE_RETRY_CONFIG.exponential_base,
        jitter=DATABASE_RETRY_CONFIG.jitter,
        retry_exceptions=[ConnectionError, TimeoutError]
    )(func)