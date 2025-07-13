#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
51job职位爬虫
核心爬虫类，负责从51job网站抓取职位数据
"""

import time
import json
import logging
import random
from typing import Dict, List, Optional, Any, Generator, Tuple
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup

from utils import (
    ConfigManager, RequestHandler, DataValidator, 
    DatabaseManager, create_request_handler_from_config,
    create_database_manager_from_config
)

logger = logging.getLogger(__name__)


@dataclass
class SearchParams:
    """搜索参数"""
    keyword: str
    city_code: str = ""
    city_name: str = ""
    page: int = 1
    page_size: int = 50
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    experience: Optional[str] = None
    education: Optional[str] = None
    company_type: Optional[str] = None
    industry: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        params = {
            'keyword': self.keyword,
            'curr_page': self.page,
            'pageSize': self.page_size
        }
        
        if self.city_code:
            params['city'] = self.city_code
        
        if self.salary_min is not None:
            params['salary_min'] = self.salary_min
        
        if self.salary_max is not None:
            params['salary_max'] = self.salary_max
        
        if self.experience:
            params['experience'] = self.experience
        
        if self.education:
            params['education'] = self.education
        
        if self.company_type:
            params['company_type'] = self.company_type
        
        if self.industry:
            params['industry'] = self.industry
        
        return params


@dataclass
class CrawlerStats:
    """爬虫统计信息"""
    total_searches: int = 0
    successful_searches: int = 0
    failed_searches: int = 0
    total_jobs_found: int = 0
    total_jobs_processed: int = 0
    total_jobs_saved: int = 0
    total_pages_crawled: int = 0
    total_time_spent: float = 0.0
    
    @property
    def success_rate(self) -> float:
        """搜索成功率"""
        if self.total_searches == 0:
            return 0.0
        return self.successful_searches / self.total_searches
    
    @property
    def average_time_per_search(self) -> float:
        """平均每次搜索耗时"""
        if self.successful_searches == 0:
            return 0.0
        return self.total_time_spent / self.successful_searches


class JobSpider:
    """51job职位爬虫"""
    
    def __init__(self, config_manager: ConfigManager):
        """
        初始化爬虫
        
        Args:
            config_manager: 配置管理器
        """
        self.config_manager = config_manager
        self.request_handler = create_request_handler_from_config(config_manager)
        self.database_manager = create_database_manager_from_config(config_manager)
        self.data_validator = DataValidator()
        
        # 爬虫配置
        self._load_crawler_config()
        
        # 统计信息
        self.stats = CrawlerStats()
        
        # 城市代码映射
        self.city_codes = self._load_city_codes()
        
        logger.info("JobSpider初始化完成")
    
    def _load_crawler_config(self):
        """加载爬虫配置"""
        search_config = self.config_manager.get_search_config()
        
        self.base_url = "https://we.51job.com"
        self.search_url = "https://we.51job.com/api/job/search-pc"
        self.detail_url_template = "https://jobs.51job.com/{job_id}.html"
        
        self.max_pages = search_config.max_pages
        self.page_size = search_config.page_size
        self.search_interval = search_config.interval
        
        logger.debug(f"爬虫配置加载完成: max_pages={self.max_pages}, page_size={self.page_size}")
    
    def _load_city_codes(self) -> Dict[str, str]:
        """加载城市代码映射"""
        # 常用城市代码映射
        city_codes = {
            '北京': '010000',
            '上海': '020000', 
            '广州': '030200',
            '深圳': '040000',
            '天津': '050000',
            '武汉': '170200',
            '西安': '200200',
            '南京': '080200',
            '成都': '090200',
            '重庆': '060000',
            '杭州': '070200',
            '大连': '230300',
            '沈阳': '230200',
            '长春': '250200',
            '哈尔滨': '260200',
            '济南': '180200',
            '青岛': '180300',
            '郑州': '160200',
            '苏州': '080300',
            '无锡': '080400',
            '宁波': '070300',
            '佛山': '030800',
            '东莞': '030900',
            '合肥': '150200',
            '福州': '110200',
            '厦门': '110300',
            '昆明': '280200',
            '石家庄': '190200',
            '太原': '210200',
            '长沙': '140200',
            '南昌': '130200',
            '贵阳': '300200',
            '南宁': '310200',
            '海口': '320200',
            '兰州': '270200',
            '银川': '290200',
            '西宁': '330200',
            '乌鲁木齐': '340200',
            '拉萨': '350200'
        }
        
        logger.info(f"加载城市代码映射: {len(city_codes)} 个城市")
        return city_codes
    
    def get_city_code(self, city_name: str) -> str:
        """
        获取城市代码
        
        Args:
            city_name: 城市名称
            
        Returns:
            城市代码
        """
        return self.city_codes.get(city_name, "")
    
    def search_jobs(self, search_params: SearchParams) -> Dict[str, Any]:
        """
        搜索职位
        
        Args:
            search_params: 搜索参数
            
        Returns:
            搜索结果字典
        """
        self.stats.total_searches += 1
        start_time = time.time()
        
        try:
            # 构建请求参数
            params = search_params.to_dict()
            
            # 添加固定参数
            params.update({
                'api_key': '51job',
                'timestamp': str(int(time.time() * 1000)),
                'keyword': search_params.keyword,
                'searchType': '2',
                'sortType': '0',
                'metro': '',
                'district': '',
                'subway': '',
                'salary': '',
                'workYear': '',
                'degree': '',
                'companyType': '',
                'companySize': '',
                'jobType': '',
                'issueDate': '',
                'isSchoolJob': '0'
            })
            
            logger.debug(f"搜索职位: {search_params.keyword}, 页码: {search_params.page}")
            
            # 发送请求
            response = self.request_handler.get(
                self.search_url,
                params=params,
                headers={
                    'Referer': 'https://we.51job.com/pc/search',
                    'Accept': 'application/json, text/plain, */*'
                }
            )
            
            # 解析响应
            if response.status_code == 200:
                try:
                    data = response.json()
                    
                    if data.get('status') == '1':
                        result_data = data.get('resultbody', {})
                        job_list = result_data.get('job', {}).get('items', [])
                        total_count = result_data.get('job', {}).get('totalCount', 0)
                        
                        # 调试：记录职位数量
                        if job_list:
                            logger.debug(f"获取到 {len(job_list)} 个职位数据")
                        
                        self.stats.successful_searches += 1
                        self.stats.total_jobs_found += len(job_list)
                        
                        search_time = time.time() - start_time
                        self.stats.total_time_spent += search_time
                        
                        logger.info(f"搜索成功: 找到 {len(job_list)} 个职位，总计 {total_count} 个")
                        
                        return {
                            'success': True,
                            'jobs': job_list,
                            'total_count': total_count,
                            'current_page': search_params.page,
                            'page_size': search_params.page_size,
                            'search_time': search_time
                        }
                    else:
                        error_msg = data.get('message', '未知错误')
                        logger.error(f"搜索API返回错误: {error_msg}")
                        self.stats.failed_searches += 1
                        return {
                            'success': False,
                            'error': error_msg,
                            'jobs': [],
                            'total_count': 0
                        }
                        
                except json.JSONDecodeError as e:
                    logger.error(f"解析搜索响应JSON失败: {e}")
                    logger.debug(f"响应内容: {response.text[:500]}...")
                    self.stats.failed_searches += 1
                    return {
                        'success': False,
                        'error': f'JSON解析失败: {e}',
                        'jobs': [],
                        'total_count': 0
                    }
            else:
                logger.error(f"搜索请求失败: HTTP {response.status_code}")
                self.stats.failed_searches += 1
                return {
                    'success': False,
                    'error': f'HTTP {response.status_code}',
                    'jobs': [],
                    'total_count': 0
                }
                
        except Exception as e:
            logger.error(f"搜索职位异常: {e}")
            self.stats.failed_searches += 1
            return {
                'success': False,
                'error': str(e),
                'jobs': [],
                'total_count': 0
            }
    
    def crawl_jobs(self, keyword: str, city_name: str = "", 
                   max_pages: Optional[int] = None) -> Generator[Dict[str, Any], None, None]:
        """
        爬取职位数据 (生成器)
        
        Args:
            keyword: 搜索关键词
            city_name: 城市名称
            max_pages: 最大页数
            
        Yields:
            职位数据字典
        """
        city_code = self.get_city_code(city_name) if city_name else ""
        max_pages = max_pages or self.max_pages
        
        logger.info(f"开始爬取职位: 关键词={keyword}, 城市={city_name}, 最大页数={max_pages}")
        
        page = 1
        total_jobs = 0
        
        while page <= max_pages:
            # 创建搜索参数
            search_params = SearchParams(
                keyword=keyword,
                city_code=city_code,
                city_name=city_name,
                page=page,
                page_size=self.page_size
            )
            
            # 搜索职位
            search_result = self.search_jobs(search_params)
            
            if not search_result['success']:
                logger.error(f"第 {page} 页搜索失败: {search_result.get('error')}")
                break
            
            jobs = search_result['jobs']
            if not jobs:
                logger.info(f"第 {page} 页没有更多职位，停止爬取")
                break
            
            # 处理每个职位
            for job_data in jobs:
                try:
                    processed_job = self._process_job_data(job_data, keyword, city_name)
                    if processed_job:
                        yield processed_job
                        total_jobs += 1
                        self.stats.total_jobs_processed += 1
                        
                except Exception as e:
                    logger.error(f"处理职位数据失败: {e}")
                    continue
            
            self.stats.total_pages_crawled += 1
            logger.info(f"完成第 {page} 页，本页 {len(jobs)} 个职位")
            
            # 检查是否还有更多页面
            total_count = search_result.get('total_count', 0)
            if page * self.page_size >= total_count:
                logger.info(f"已爬取所有页面，总计 {total_jobs} 个职位")
                break
            
            page += 1
            
            # 页面间隔
            if self.search_interval > 0:
                interval = random.uniform(self.search_interval[0], self.search_interval[1]) \
                    if isinstance(self.search_interval, (list, tuple)) else self.search_interval
                time.sleep(interval)
        
        logger.info(f"爬取完成: 关键词={keyword}, 总计 {total_jobs} 个职位")
    
    def _process_job_data(self, job_data: Dict[str, Any], 
                         keyword: str, city_name: str) -> Optional[Dict[str, Any]]:
        """
        处理职位数据
        
        Args:
            job_data: 原始职位数据
            keyword: 搜索关键词
            city_name: 城市名称
            
        Returns:
            处理后的职位数据
        """
        try:
            # 提取基本信息
            job_id = job_data.get('jobId', '')
            if not job_id:
                logger.warning("职位ID为空，跳过")
                return None
            
            # 构建职位数据
            processed_data = {
                'job_id': job_id,
                'title': job_data.get('jobName', ''),
                'company_name': job_data.get('companyName', ''),
                'company_size': job_data.get('companySizeString', ''),
                'company_type': job_data.get('companyTypeString', ''),
                'location_city': city_name or job_data.get('jobAreaString', ''),
              #  'location_district': job_data.get('workarea_text', ''),
                'job_url': self.detail_url_template.format(job_id=job_id),
                'company_url': job_data.get('companyHref', ''),
                'industry': job_data.get('industryType1Str', ''),
                'publish_time': job_data.get('issueDateString', ''),
                'update_time': job_data.get('updateDateTime', ''),
                'confirm_date': job_data.get('confirmDateString', ''),
                'raw_data': job_data
            }
            
            # 处理薪资信息
            salary_text = job_data.get('providesalary_text', '')
            processed_data['salary_text'] = salary_text
            
            if salary_text:
                salary_min, salary_max = self._parse_salary(salary_text)
                processed_data['salary_min'] = salary_min
                processed_data['salary_max'] = salary_max
            
            # 处理经验和学历要求
            processed_data['experience_required'] = job_data.get('workYear', '')
            processed_data['education_required'] = job_data.get('degreefrom', '')
            
            # 处理职位标签
            job_tags = job_data.get('attribute_text', [])
            if isinstance(job_tags, list):
                processed_data['job_tags'] = ','.join(job_tags)
            else:
                processed_data['job_tags'] = str(job_tags) if job_tags else ''
            
            # 处理福利待遇
            welfare = job_data.get('jobwelf', '')
            processed_data['welfare'] = welfare
            
            # 处理工作类型
            processed_data['work_type'] = job_data.get('jobtype', '')
            
            # 使用数据验证器处理原始数据
            validation_result = self.data_validator.validate_job_data(job_data)
            if not validation_result.is_valid:
                logger.warning(f"职位数据验证失败: {job_id}, 错误: {validation_result.errors}")
                return None
            
            return validation_result.cleaned_data
            
        except Exception as e:
            logger.error(f"处理职位数据异常: {e}")
            return None
    
    def _parse_salary(self, salary_text: str) -> Tuple[Optional[int], Optional[int]]:
        """
        解析薪资文本
        
        Args:
            salary_text: 薪资文本
            
        Returns:
            (最低薪资, 最高薪资)
        """
        try:
            # 使用数据验证器的薪资解析功能
            return self.data_validator.parse_salary(salary_text)
        except Exception as e:
            logger.debug(f"薪资解析失败: {salary_text}, 错误: {e}")
            return None, None
    
    def save_jobs(self, jobs: List[Dict[str, Any]]) -> Tuple[int, int]:
        """
        保存职位数据到数据库
        
        Args:
            jobs: 职位数据列表
            
        Returns:
            (成功保存数量, 跳过数量)
        """
        if not jobs:
            return 0, 0
        
        try:
            saved_count, skipped_count = self.database_manager.batch_insert_job_listings(jobs)
            self.stats.total_jobs_saved += saved_count
            
            logger.info(f"保存职位数据: 成功 {saved_count}, 跳过 {skipped_count}")
            return saved_count, skipped_count
            
        except Exception as e:
            logger.error(f"保存职位数据失败: {e}")
            raise
    
    def crawl_and_save(self, keyword: str, city_name: str = "", 
                      max_pages: Optional[int] = None, 
                      batch_size: int = 100) -> Dict[str, Any]:
        """
        爬取并保存职位数据
        
        Args:
            keyword: 搜索关键词
            city_name: 城市名称
            max_pages: 最大页数
            batch_size: 批量保存大小
            
        Returns:
            爬取结果统计
        """
        start_time = time.time()
        
        logger.info(f"开始爬取并保存: 关键词={keyword}, 城市={city_name}")
        
        jobs_batch = []
        total_saved = 0
        total_skipped = 0
        
        try:
            # 爬取职位数据
            for job_data in self.crawl_jobs(keyword, city_name, max_pages):
                jobs_batch.append(job_data)
                
                # 批量保存
                if len(jobs_batch) >= batch_size:
                    saved_count, skipped_count = self.save_jobs(jobs_batch)
                    total_saved += saved_count
                    total_skipped += skipped_count
                    jobs_batch = []
            
            # 保存剩余数据
            if jobs_batch:
                saved_count, skipped_count = self.save_jobs(jobs_batch)
                total_saved += saved_count
                total_skipped += skipped_count
            
            total_time = time.time() - start_time
            
            result = {
                'success': True,
                'keyword': keyword,
                'city_name': city_name,
                'total_processed': self.stats.total_jobs_processed,
                'total_saved': total_saved,
                'total_skipped': total_skipped,
                'pages_crawled': self.stats.total_pages_crawled,
                'total_time': total_time,
                'average_time_per_job': total_time / max(1, self.stats.total_jobs_processed)
            }
            
            logger.info(f"爬取完成: {result}")
            return result
            
        except Exception as e:
            logger.error(f"爬取并保存失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'keyword': keyword,
                'city_name': city_name,
                'total_processed': self.stats.total_jobs_processed,
                'total_saved': total_saved,
                'total_skipped': total_skipped
            }
    
    def get_stats(self) -> CrawlerStats:
        """
        获取爬虫统计信息
        
        Returns:
            爬虫统计信息
        """
        return self.stats
    
    def reset_stats(self):
        """重置统计信息"""
        self.stats = CrawlerStats()
        logger.info("爬虫统计信息已重置")
    
    def get_detailed_stats(self) -> Dict[str, Any]:
        """
        获取详细统计信息
        
        Returns:
            详细统计信息字典
        """
        return {
            'crawler_stats': self.stats,
            'request_stats': self.request_handler.get_stats(),
            'database_stats': self.database_manager.get_stats(),
            'validator_stats': self.data_validator.get_stats() if hasattr(self.data_validator, 'get_stats') else None
        }
    
    def close(self):
        """关闭爬虫"""
        if self.request_handler:
            self.request_handler.close()
        
        if self.database_manager:
            self.database_manager.close()
        
        logger.info("爬虫已关闭")
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close()
    
    def __str__(self) -> str:
        """字符串表示"""
        return f"JobSpider(searches={self.stats.total_searches}, jobs={self.stats.total_jobs_processed})"
    
    def __repr__(self) -> str:
        """对象表示"""
        return self.__str__()


def create_job_spider_from_config(config_path: str = None) -> JobSpider:
    """
    从配置文件创建爬虫实例
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        爬虫实例
    """
    config_manager = ConfigManager(config_path)
    spider = JobSpider(config_manager)
    
    logger.info(f"从配置创建爬虫: {spider}")
    return spider


def create_job_spider(config_path: str = None) -> JobSpider:
    """
    创建爬虫实例（别名函数）
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        爬虫实例
    """
    return create_job_spider_from_config(config_path)