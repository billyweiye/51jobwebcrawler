#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置管理器
负责加载和管理YAML配置文件
"""

import os
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    """数据库配置类"""
    host: str = ""
    port: int = 0
    database: str = ""
    username: str = ""
    password: str = ""
    charset: str = "utf8mb4"
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30
    pool_recycle: int = 3600
    db_type: str = "sqlite"  # 添加数据库类型字段
    database_path: str = ""  # 添加SQLite数据库路径字段
    enabled: bool = True  # 添加enabled字段


@dataclass
class RequestConfig:
    """请求配置类"""
    download_delay: int = 3
    randomize_delay: float = 0.5
    timeout: int = 30
    connect_timeout: int = 10
    retry_times: int = 3
    max_retries: int = 3  # 添加最大重试次数字段
    retry_delay: float = 1.0  # 添加重试延迟字段
    max_retry_delay: float = 60.0  # 添加最大重试延迟字段
    retry_http_codes: list = None
    concurrent_requests: int = 8
    concurrent_requests_per_domain: int = 2
    respect_robots_txt: bool = True
    
    def __post_init__(self):
        if self.retry_http_codes is None:
            self.retry_http_codes = [500, 502, 503, 504, 408, 429]


@dataclass
class SearchConfig:
    """搜索配置类"""
    keywords: list
    cities: list
    max_pages: int = 10
    page_size: int = 20
    search_interval: dict = None
    interval: float = 5.0  # 添加interval字段
    
    def __post_init__(self):
        if self.search_interval is None:
            self.search_interval = {"min": 5, "max": 15}


class ConfigManager:
    """配置管理器"""
    
    def __init__(self, config_dir: str = "./config"):
        """
        初始化配置管理器
        
        Args:
            config_dir: 配置文件目录路径
        """
        self.config_dir = Path(config_dir)
        self._settings = {}
        self._database_config = {}
        self._logging_config = {}
        
        # 确保配置目录存在
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        # 加载配置文件
        self._load_all_configs()
    
    def _load_yaml_file(self, file_path: Path) -> Dict[str, Any]:
        """
        加载YAML配置文件
        
        Args:
            file_path: 配置文件路径
            
        Returns:
            配置字典
        """
        try:
            if not file_path.exists():
                logger.warning(f"配置文件不存在: {file_path}")
                return {}
                
            with open(file_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f) or {}
                logger.info(f"成功加载配置文件: {file_path}")
                return config
                
        except yaml.YAMLError as e:
            logger.error(f"YAML解析错误 {file_path}: {e}")
            return {}
        except Exception as e:
            logger.error(f"加载配置文件失败 {file_path}: {e}")
            return {}
    
    def _load_all_configs(self):
        """加载所有配置文件"""
        # 加载主配置
        settings_file = self.config_dir / "settings.yaml"
        self._settings = self._load_yaml_file(settings_file)
        
        # 加载数据库配置
        database_file = self.config_dir / "database.yaml"
        self._database_config = self._load_yaml_file(database_file)
        
        # 加载日志配置
        logging_file = self.config_dir / "logging.yaml"
        self._logging_config = self._load_yaml_file(logging_file)
        
        logger.info("所有配置文件加载完成")
    
    def reload_configs(self):
        """重新加载所有配置文件"""
        logger.info("重新加载配置文件...")
        self._load_all_configs()
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        获取配置值 (支持点号分隔的嵌套键)
        
        Args:
            key: 配置键，支持 'section.subsection.key' 格式
            default: 默认值
            
        Returns:
            配置值
        """
        keys = key.split('.')
        
        # 如果是数据库配置，从_database_config中获取
        if keys[0] == 'database':
            value = self._database_config
            keys = keys[1:]  # 跳过'database'前缀
        else:
            value = self._settings
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_database_config(self, db_type: str = "sqlite", instance: str = "primary") -> Optional[DatabaseConfig]:
        """
        获取数据库配置
        
        Args:
            db_type: 数据库类型 (mysql, postgresql, sqlite)
            instance: 实例名称 (primary, replica)
            
        Returns:
            数据库配置对象
        """
        try:
            db_config = self._database_config.get(db_type, {})
            if isinstance(db_config.get(instance), dict):
                config_data = db_config[instance]
            else:
                config_data = db_config
            
            # 确保包含db_type字段
            config_data = config_data.copy()
            config_data['db_type'] = db_type
                
            return DatabaseConfig(**config_data)
        except Exception as e:
            logger.error(f"获取数据库配置失败: {e}")
            return None
    
    def get_request_config(self) -> RequestConfig:
        """
        获取请求配置
        
        Returns:
            请求配置对象
        """
        request_data = self._settings.get('request', {})
        return RequestConfig(**request_data)
    
    def get_search_config(self) -> SearchConfig:
        """
        获取搜索配置
        
        Returns:
            搜索配置对象
        """
        search_data = self._settings.get('search', {})
        return SearchConfig(**search_data)
    
    def get_proxy_config(self) -> Dict[str, Any]:
        """获取代理配置"""
        return self._settings.get('proxy', {})
    
    def get_config(self, key: str = None, default=None):
        """获取配置项
        
        Args:
            key: 配置键，如果为None则返回所有配置
            default: 默认值
            
        Returns:
            配置值或配置字典
        """
        if key is None:
            return self._settings
        
        # 支持点分隔的嵌套键
        if '.' in key:
            keys = key.split('.')
            value = self._settings
            for k in keys:
                if isinstance(value, dict) and k in value:
                    value = value[k]
                else:
                    return default if default is not None else {}
            return value
        
        return self._settings.get(key, default if default is not None else {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """
        获取日志配置
        
        Returns:
            日志配置字典
        """
        return self._logging_config.copy()
    
    def is_debug_mode(self) -> bool:
        """
        检查是否为调试模式
        
        Returns:
            是否为调试模式
        """
        return self.get('development.debug', False)
    
    def is_test_mode(self) -> bool:
        """
        检查是否为测试模式
        
        Returns:
            是否为测试模式
        """
        return self.get('development.test_mode', False)
    
    def get_test_limit(self) -> int:
        """
        获取测试模式下的限制数量
        
        Returns:
            测试限制数量
        """
        return self.get('development.test_limit', 100)
    
    def get_user_agents_file(self) -> str:
        """
        获取用户代理文件路径
        
        Returns:
            用户代理文件路径
        """
        return self.get('user_agent.agents_file', './config/user_agents.txt')
    
    def get_proxies_file(self) -> str:
        """
        获取代理文件路径
        
        Returns:
            代理文件路径
        """
        return self.get('proxy.proxy_file', './config/proxies.txt')
    
    def is_proxy_enabled(self) -> bool:
        """
        检查是否启用代理
        
        Returns:
            是否启用代理
        """
        return self.get('proxy.enabled', False)
    
    def is_user_agent_rotation_enabled(self) -> bool:
        """
        检查是否启用用户代理轮换
        
        Returns:
            是否启用用户代理轮换
        """
        return self.get('user_agent.rotation', True)
    
    def get_backup_path(self) -> str:
        """
        获取备份路径
        
        Returns:
            备份路径
        """
        return self.get('storage.backup_path', './data/backup')
    
    def is_monitoring_enabled(self) -> bool:
        """
        检查是否启用监控
        
        Returns:
            是否启用监控
        """
        return self.get('monitoring.enabled', True)
    
    def get_cache_dir(self) -> str:
        """
        获取缓存目录
        
        Returns:
            缓存目录路径
        """
        return self.get('development.cache_dir', './data/cache')
    
    def is_cache_enabled(self) -> bool:
        """
        检查是否启用缓存
        
        Returns:
            是否启用缓存
        """
        return self.get('development.cache_enabled', True)
    
    def get_target_api_url(self) -> str:
        """
        获取目标API URL
        
        Returns:
            API URL
        """
        return self.get('target.api_url', 'https://we.51job.com/api/job/search-pc')
    
    def __str__(self) -> str:
        """字符串表示"""
        return f"ConfigManager(config_dir={self.config_dir})"
    
    def __repr__(self) -> str:
        """对象表示"""
        return self.__str__()


# 全局配置管理器实例
config_manager = ConfigManager()