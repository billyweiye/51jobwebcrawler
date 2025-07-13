#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
工具模块
包含配置管理、重试处理、用户代理轮换、数据验证、Cookie处理、代理管理、请求处理、数据库管理等工具类
"""

from .config_manager import ConfigManager
from .retry_handler import RetryHandler
from .user_agent import UserAgentRotator
from .data_validator import DataValidator
from .cookie_handler import CookieHandler, EnhancedCookieHandler
from .proxy_manager import ProxyManager, ProxyInfo
from .request_handler import RequestHandler, create_request_handler_from_config
from .database_manager import DatabaseManager, JobListing, create_database_manager_from_config, create_sqlite_database_manager

__all__ = [
    'ConfigManager',
    'RetryHandler', 
    'UserAgentRotator',
    'DataValidator',
    'CookieHandler',
    'EnhancedCookieHandler',
    'ProxyManager',
    'ProxyInfo',
    'RequestHandler',
    'create_request_handler_from_config',
    'DatabaseManager',
    'JobListing',
    'create_database_manager_from_config',
    'create_sqlite_database_manager'
]

__version__ = '1.1.0'
__author__ = '51job Crawler Team'