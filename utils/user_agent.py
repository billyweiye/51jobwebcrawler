#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
用户代理轮换器
实现User-Agent的随机轮换功能
"""

import random
import logging
from pathlib import Path
from typing import List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class UserAgentStats:
    """用户代理统计信息"""
    total_agents: int = 0
    current_agent: str = ""
    usage_count: int = 0
    rotation_enabled: bool = True


class UserAgentRotator:
    """用户代理轮换器"""
    
    # 内置的用户代理列表
    DEFAULT_USER_AGENTS = [
        # Chrome on Windows
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        
        # Chrome on macOS
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        
        # Firefox on Windows
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0",
        
        # Firefox on macOS
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:120.0) Gecko/20100101 Firefox/120.0",
        
        # Safari on macOS
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
        
        # Edge on Windows
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
        
        # Chrome on Linux
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        
        # Firefox on Linux
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",
    ]
    
    def __init__(self, agents_file: Optional[str] = None, 
                 rotation_enabled: bool = True,
                 fallback_to_default: bool = True):
        """
        初始化用户代理轮换器
        
        Args:
            agents_file: 用户代理文件路径
            rotation_enabled: 是否启用轮换
            fallback_to_default: 文件加载失败时是否回退到默认列表
        """
        self.agents_file = agents_file
        self.rotation_enabled = rotation_enabled
        self.fallback_to_default = fallback_to_default
        
        self.user_agents: List[str] = []
        self.current_agent: str = ""
        self.usage_count: int = 0
        self.last_index: int = -1
        
        # 加载用户代理列表
        self._load_user_agents()
        
        # 设置初始用户代理
        if self.user_agents:
            self.current_agent = self.user_agents[0]
    
    def _load_user_agents(self):
        """加载用户代理列表"""
        # 尝试从文件加载
        if self.agents_file:
            file_agents = self._load_from_file(self.agents_file)
            if file_agents:
                self.user_agents = file_agents
                logger.info(f"从文件加载了 {len(self.user_agents)} 个用户代理")
                return
        
        # 回退到默认列表
        if self.fallback_to_default:
            self.user_agents = self.DEFAULT_USER_AGENTS.copy()
            logger.info(f"使用默认用户代理列表，共 {len(self.user_agents)} 个")
        else:
            logger.warning("未能加载用户代理，且未启用默认回退")
    
    def _load_from_file(self, file_path: str) -> List[str]:
        """
        从文件加载用户代理列表
        
        Args:
            file_path: 文件路径
            
        Returns:
            用户代理列表
        """
        try:
            path = Path(file_path)
            if not path.exists():
                logger.warning(f"用户代理文件不存在: {file_path}")
                return []
            
            with open(path, 'r', encoding='utf-8') as f:
                agents = []
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        agents.append(line)
                
                logger.info(f"从文件 {file_path} 加载了 {len(agents)} 个用户代理")
                return agents
                
        except Exception as e:
            logger.error(f"加载用户代理文件失败 {file_path}: {e}")
            return []
    
    def get_random_agent(self) -> str:
        """
        获取随机用户代理
        
        Returns:
            随机用户代理字符串
        """
        if not self.user_agents:
            logger.warning("没有可用的用户代理")
            return ""
        
        if not self.rotation_enabled:
            return self.current_agent
        
        # 随机选择一个不同于上次的用户代理
        if len(self.user_agents) > 1:
            available_indices = [i for i in range(len(self.user_agents)) if i != self.last_index]
            index = random.choice(available_indices)
        else:
            index = 0
        
        self.last_index = index
        self.current_agent = self.user_agents[index]
        self.usage_count += 1
        
        logger.debug(f"选择用户代理 [{index}]: {self.current_agent[:50]}...")
        return self.current_agent
    
    def get_random(self) -> str:
        """
        获取随机用户代理（别名方法）
        
        Returns:
            随机用户代理字符串
        """
        return self.get_random_agent()
    
    def get_current_agent(self) -> str:
        """
        获取当前用户代理
        
        Returns:
            当前用户代理字符串
        """
        return self.current_agent
    
    def set_agent(self, agent: str):
        """
        设置指定的用户代理
        
        Args:
            agent: 用户代理字符串
        """
        self.current_agent = agent
        logger.info(f"设置用户代理: {agent[:50]}...")
    
    def add_agent(self, agent: str):
        """
        添加新的用户代理到列表
        
        Args:
            agent: 用户代理字符串
        """
        if agent and agent not in self.user_agents:
            self.user_agents.append(agent)
            logger.info(f"添加新用户代理: {agent[:50]}...")
    
    def remove_agent(self, agent: str) -> bool:
        """
        从列表中移除用户代理
        
        Args:
            agent: 要移除的用户代理字符串
            
        Returns:
            是否成功移除
        """
        try:
            self.user_agents.remove(agent)
            logger.info(f"移除用户代理: {agent[:50]}...")
            return True
        except ValueError:
            logger.warning(f"用户代理不存在，无法移除: {agent[:50]}...")
            return False
    
    def enable_rotation(self):
        """启用用户代理轮换"""
        self.rotation_enabled = True
        logger.info("已启用用户代理轮换")
    
    def disable_rotation(self):
        """禁用用户代理轮换"""
        self.rotation_enabled = False
        logger.info("已禁用用户代理轮换")
    
    def is_enabled(self) -> bool:
        """检查用户代理轮换是否启用"""
        return self.rotation_enabled
    
    def reload_agents(self):
        """重新加载用户代理列表"""
        logger.info("重新加载用户代理列表")
        old_count = len(self.user_agents)
        self._load_user_agents()
        new_count = len(self.user_agents)
        logger.info(f"用户代理数量: {old_count} -> {new_count}")
    
    def get_stats(self) -> UserAgentStats:
        """
        获取统计信息
        
        Returns:
            用户代理统计信息
        """
        return UserAgentStats(
            total_agents=len(self.user_agents),
            current_agent=self.current_agent,
            usage_count=self.usage_count,
            rotation_enabled=self.rotation_enabled
        )
    
    def reset_stats(self):
        """重置统计信息"""
        self.usage_count = 0
        self.last_index = -1
        logger.info("用户代理统计信息已重置")
    
    def save_to_file(self, file_path: str) -> bool:
        """
        将当前用户代理列表保存到文件
        
        Args:
            file_path: 保存文件路径
            
        Returns:
            是否保存成功
        """
        try:
            path = Path(file_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(path, 'w', encoding='utf-8') as f:
                f.write("# User Agent List\n")
                f.write(f"# Generated with {len(self.user_agents)} agents\n\n")
                
                for agent in self.user_agents:
                    f.write(f"{agent}\n")
            
            logger.info(f"用户代理列表已保存到: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"保存用户代理列表失败 {file_path}: {e}")
            return False
    
    def __len__(self) -> int:
        """返回用户代理数量"""
        return len(self.user_agents)
    
    def __str__(self) -> str:
        """字符串表示"""
        return f"UserAgentRotator(agents={len(self.user_agents)}, rotation={self.rotation_enabled})"
    
    def __repr__(self) -> str:
        """对象表示"""
        return self.__str__()


# 便捷函数
def create_user_agent_rotator(config_manager=None) -> UserAgentRotator:
    """
    根据配置创建用户代理轮换器
    
    Args:
        config_manager: 配置管理器实例
        
    Returns:
        用户代理轮换器实例
    """
    if config_manager:
        agents_file = config_manager.get_user_agents_file()
        rotation_enabled = config_manager.is_user_agent_rotation_enabled()
    else:
        agents_file = None
        rotation_enabled = True
    
    return UserAgentRotator(
        agents_file=agents_file,
        rotation_enabled=rotation_enabled,
        fallback_to_default=True
    )