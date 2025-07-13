#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
爬虫模块
包含核心爬虫组件
"""

from .job_spider import JobSpider, create_job_spider
from .data_processor import DataProcessor, ProcessingStats
from .scheduler import CrawlerScheduler, CrawlTask, TaskStatus, create_crawler_scheduler

__all__ = [
    'JobSpider',
    'create_job_spider',
    'DataProcessor', 
    'ProcessingStats',
    'CrawlerScheduler',
    'CrawlTask',
    'TaskStatus',
    'create_crawler_scheduler'
]

__version__ = '1.0.0'
__author__ = '51job爬虫团队'