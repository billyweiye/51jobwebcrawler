#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据处理器
用于清洗、转换和验证爬取到的职位数据
"""

import re
import time
import logging
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass
from datetime import datetime, timedelta
from collections import Counter

from utils import DataValidator

logger = logging.getLogger(__name__)


@dataclass
class ProcessingStats:
    """数据处理统计信息"""
    total_records: int = 0
    processed_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    duplicated_records: int = 0
    cleaned_fields: int = 0
    enriched_records: int = 0
    
    @property
    def success_rate(self) -> float:
        """处理成功率"""
        if self.total_records == 0:
            return 0.0
        return self.valid_records / self.total_records
    
    @property
    def duplicate_rate(self) -> float:
        """重复率"""
        if self.total_records == 0:
            return 0.0
        return self.duplicated_records / self.total_records


class DataProcessor:
    """数据处理器"""
    
    def __init__(self, enable_enrichment: bool = True, 
                 enable_deduplication: bool = True):
        """
        初始化数据处理器
        
        Args:
            enable_enrichment: 是否启用数据增强
            enable_deduplication: 是否启用去重
        """
        self.data_validator = DataValidator()
        self.enable_enrichment = enable_enrichment
        self.enable_deduplication = enable_deduplication
        
        # 统计信息
        self.stats = ProcessingStats()
        
        # 缓存
        self._seen_job_ids = set()
        self._company_cache = {}
        self._location_cache = {}
        
        # 数据字典
        self._load_data_dictionaries()
        
        logger.info("数据处理器初始化完成")
    
    def _load_data_dictionaries(self):
        """加载数据字典"""
        # 行业映射
        self.industry_mapping = {
            '计算机软件': 'IT/互联网',
            '互联网/电子商务': 'IT/互联网',
            '计算机硬件': 'IT/互联网',
            '通信/电信/网络设备': 'IT/互联网',
            '电子技术/半导体/集成电路': 'IT/互联网',
            '金融/投资/证券': '金融',
            '银行': '金融',
            '保险': '金融',
            '房地产': '房地产',
            '建筑/建材/工程': '建筑',
            '制药/生物工程': '医疗健康',
            '医疗/护理/卫生': '医疗健康',
            '教育/培训/院校': '教育',
            '咨询/人力资源/财会': '咨询服务',
            '广告/会展/公关': '广告传媒',
            '影视/媒体/艺术/文化传播': '广告传媒'
        }
        
        # 公司规模标准化
        self.company_size_mapping = {
            '1-49人': '小型',
            '50-149人': '小型',
            '150-499人': '中型',
            '500-999人': '中型', 
            '1000-4999人': '大型',
            '5000-9999人': '大型',
            '10000人以上': '超大型'
        }
        
        # 经验要求标准化
        self.experience_mapping = {
            '无工作经验': '0年',
            '1年经验': '1年',
            '2年经验': '2年',
            '3-4年经验': '3-4年',
            '5-7年经验': '5-7年',
            '8-9年经验': '8-9年',
            '10年以上经验': '10年以上'
        }
        
        # 学历要求标准化
        self.education_mapping = {
            '初中及以下': '初中',
            '高中/中专/技校': '高中',
            '大专': '大专',
            '本科': '本科',
            '硕士': '硕士',
            '博士': '博士',
            'MBA': '硕士',
            'EMBA': '硕士'
        }
        
        logger.debug("数据字典加载完成")
    
    def process_job_data(self, job_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        处理单条职位数据
        
        Args:
            job_data: 原始职位数据
            
        Returns:
            处理后的职位数据，如果处理失败则返回None
        """
        self.stats.total_records += 1
        
        try:
            # 1. 基础验证
            if not self._basic_validation(job_data):
                self.stats.invalid_records += 1
                return None
            
            # 2. 去重检查
            if self.enable_deduplication and self._is_duplicate(job_data):
                self.stats.duplicated_records += 1
                logger.debug(f"跳过重复职位: {job_data.get('job_id')}")
                return None
            
            # 3. 数据清洗
            cleaned_data = self._clean_job_data(job_data.copy())
            
            # 4. 数据标准化
            standardized_data = self._standardize_job_data(cleaned_data)
            
            # 5. 数据增强
            if self.enable_enrichment:
                enriched_data = self._enrich_job_data(standardized_data)
            else:
                enriched_data = standardized_data
            
            # 6. 最终验证
            # 从raw_data中获取原始数据进行验证
            raw_data = job_data.get('raw_data', {})
            
            if isinstance(raw_data, str):
                try:
                    import json
                    raw_data = json.loads(raw_data)
                except json.JSONDecodeError as e:
                    logger.error(f"JSON解析失败: {e}")
                    self.stats.invalid_records += 1
                    return None
            elif not isinstance(raw_data, dict):
                logger.error(f"raw_data类型不支持: {type(raw_data)}")
                self.stats.invalid_records += 1
                return None
                
            validation_result = self.data_validator.validate_job_data(raw_data)  # 使用原始API数据进行验证
            if validation_result.is_valid:
                self.stats.processed_records += 1
                self.stats.valid_records += 1
                
                # 记录已处理的job_id
                if self.enable_deduplication:
                    self._seen_job_ids.add(job_data.get('job_id'))
                
                return validation_result.cleaned_data
            else:
                logger.warning(f"职位数据最终验证失败: {job_data.get('job_id')}, 错误: {validation_result.errors}, 警告: {validation_result.warnings}")
                self.stats.invalid_records += 1
                return None
                
        except Exception as e:
            logger.error(f"处理职位数据异常: {e}")
            self.stats.invalid_records += 1
            return None
    
    def _basic_validation(self, job_data: Dict[str, Any]) -> bool:
        """
        基础验证
        
        Args:
            job_data: 职位数据（处理后格式）
            
        Returns:
            是否通过基础验证
        """
        # 检查必需字段（使用处理后字段名）
        required_fields = ['job_id', 'title', 'company_name']
        for field in required_fields:
            if not job_data.get(field):
                logger.debug(f"缺少必需字段: {field}")
                return False
        
        # 检查数据类型
        if not isinstance(job_data.get('job_id'), str):
            logger.debug("job_id必须是字符串")
            return False
        
        return True
    
    def _is_duplicate(self, job_data: Dict[str, Any]) -> bool:
        """
        检查是否重复
        
        Args:
            job_data: 职位数据（处理后格式）
            
        Returns:
            是否重复
        """
        job_id = job_data.get('job_id')
        return job_id in self._seen_job_ids
    
    def _clean_job_data(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        清洗职位数据
        
        Args:
            job_data: 原始职位数据
            
        Returns:
            清洗后的职位数据
        """
        cleaned_count = 0
        
        # 清洗文本字段
        text_fields = [
            'title', 'company_name', 'job_description', 'job_requirements',
            'location_address', 'welfare', 'job_tags'
        ]
        
        for field in text_fields:
            if field in job_data and job_data[field]:
                original_value = job_data[field]
                cleaned_value = self._clean_text(str(original_value))
                if cleaned_value != original_value:
                    job_data[field] = cleaned_value
                    cleaned_count += 1
        
        # 清洗薪资字段
        if 'salary_text' in job_data and job_data['salary_text']:
            original_salary = job_data['salary_text']
            cleaned_salary = self._clean_salary_text(original_salary)
            if cleaned_salary != original_salary:
                job_data['salary_text'] = cleaned_salary
                cleaned_count += 1
        
        # 清洗URL字段
        url_fields = ['job_url', 'company_url']
        for field in url_fields:
            if field in job_data and job_data[field]:
                original_url = job_data[field]
                cleaned_url = self._clean_url(original_url)
                if cleaned_url != original_url:
                    job_data[field] = cleaned_url
                    cleaned_count += 1
        
        self.stats.cleaned_fields += cleaned_count
        return job_data
    
    def _clean_text(self, text: str) -> str:
        """
        清洗文本
        
        Args:
            text: 原始文本
            
        Returns:
            清洗后的文本
        """
        if not text:
            return ""
        
        # 移除多余空白
        text = re.sub(r'\s+', ' ', text.strip())
        
        # 移除HTML标签
        text = re.sub(r'<[^>]+>', '', text)
        
        # 移除特殊字符
        text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', text)
        
        # 移除多余的标点符号
        text = re.sub(r'[。，；：！？]{2,}', '。', text)
        
        return text.strip()
    
    def _clean_salary_text(self, salary_text: str) -> str:
        """
        清洗薪资文本
        
        Args:
            salary_text: 原始薪资文本
            
        Returns:
            清洗后的薪资文本
        """
        if not salary_text:
            return ""
        
        # 标准化薪资格式
        salary_text = salary_text.replace('千', 'K').replace('万', 'W')
        salary_text = re.sub(r'[，,]', '-', salary_text)
        salary_text = re.sub(r'\s+', '', salary_text)
        
        return salary_text
    
    def _clean_url(self, url: str) -> str:
        """
        清洗URL
        
        Args:
            url: 原始URL
            
        Returns:
            清洗后的URL
        """
        if not url:
            return ""
        
        # 确保URL格式正确
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        return url.strip()
    
    def _standardize_job_data(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        标准化职位数据
        
        Args:
            job_data: 职位数据
            
        Returns:
            标准化后的职位数据
        """
        # 标准化行业
        if 'industry' in job_data and job_data['industry']:
            industry = job_data['industry']
            standardized_industry = self.industry_mapping.get(industry, industry)
            job_data['industry'] = standardized_industry
        
        # 标准化公司规模
        if 'company_size' in job_data and job_data['company_size']:
            company_size = job_data['company_size']
            standardized_size = self.company_size_mapping.get(company_size, company_size)
            job_data['company_size'] = standardized_size
        
        # 标准化经验要求
        if 'experience_required' in job_data and job_data['experience_required']:
            experience = job_data['experience_required']
            standardized_exp = self.experience_mapping.get(experience, experience)
            job_data['experience_required'] = standardized_exp
        
        # 标准化学历要求
        if 'education_required' in job_data and job_data['education_required']:
            education = job_data['education_required']
            standardized_edu = self.education_mapping.get(education, education)
            job_data['education_required'] = standardized_edu
        
        # 标准化时间字段
        time_fields = ['publish_time', 'update_time']
        for field in time_fields:
            if field in job_data and job_data[field]:
                standardized_time = self._standardize_time(job_data[field])
                if standardized_time:
                    job_data[field] = standardized_time
        
        return job_data
    
    def _standardize_time(self, time_input: Union[str, datetime]) -> Optional[datetime]:
        """
        标准化时间格式
        
        Args:
            time_input: 时间字符串或datetime对象
            
        Returns:
            标准化的datetime对象
        """
        if not time_input:
            return None
        
        # 如果已经是datetime对象，直接返回
        if isinstance(time_input, datetime):
            return time_input
        
        # 确保是字符串
        if not isinstance(time_input, str):
            return None
        
        time_str = time_input
        
        try:
            # 处理相对时间
            if '今天' in time_str:
                return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            elif '昨天' in time_str:
                return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
            elif '前天' in time_str:
                return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=2)
            elif '天前' in time_str:
                days_match = re.search(r'(\d+)天前', time_str)
                if days_match:
                    days = int(days_match.group(1))
                    return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days)
            
            # 处理标准时间格式
            time_formats = [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d %H:%M',
                '%Y-%m-%d',
                '%Y/%m/%d %H:%M:%S',
                '%Y/%m/%d %H:%M',
                '%Y/%m/%d',
                '%m-%d %H:%M',
                '%m/%d %H:%M'
            ]
            
            for fmt in time_formats:
                try:
                    return datetime.strptime(time_str, fmt)
                except ValueError:
                    continue
            
            logger.debug(f"无法解析时间格式: {time_str}")
            return None
            
        except Exception as e:
            logger.debug(f"时间标准化失败: {time_str}, 错误: {e}")
            return None
    
    def _enrich_job_data(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        增强职位数据
        
        Args:
            job_data: 职位数据
            
        Returns:
            增强后的职位数据
        """
        enriched = False
        
        # 提取技能标签
        if 'job_description' in job_data or 'job_requirements' in job_data:
            skills = self._extract_skills(job_data)
            if skills:
                job_data['extracted_skills'] = ','.join(skills)
                enriched = True
        
        # 计算薪资中位数
        if 'salary_min' in job_data and 'salary_max' in job_data:
            salary_min = job_data.get('salary_min')
            salary_max = job_data.get('salary_max')
            if salary_min is not None and salary_max is not None:
                job_data['salary_median'] = (salary_min + salary_max) / 2
                enriched = True
        
        # 分析职位级别
        if 'title' in job_data:
            job_level = self._analyze_job_level(job_data['title'])
            if job_level:
                job_data['job_level'] = job_level
                enriched = True
        
        # 分析工作模式
        if 'job_description' in job_data or 'welfare' in job_data:
            work_mode = self._analyze_work_mode(job_data)
            if work_mode:
                job_data['work_mode'] = work_mode
                enriched = True
        
        # 公司信息增强
        company_info = self._enrich_company_info(job_data.get('company_name', ''))
        if company_info:
            job_data.update(company_info)
            enriched = True
        
        if enriched:
            self.stats.enriched_records += 1
        
        return job_data
    
    def _extract_skills(self, job_data: Dict[str, Any]) -> List[str]:
        """
        提取技能标签
        
        Args:
            job_data: 职位数据
            
        Returns:
            技能列表
        """
        # 常见技能关键词
        skill_keywords = {
            'Python', 'Java', 'JavaScript', 'C++', 'C#', 'Go', 'Rust', 'PHP', 'Ruby',
            'React', 'Vue', 'Angular', 'Node.js', 'Spring', 'Django', 'Flask',
            'MySQL', 'PostgreSQL', 'MongoDB', 'Redis', 'Elasticsearch',
            'Docker', 'Kubernetes', 'AWS', 'Azure', 'GCP',
            'Git', 'Jenkins', 'CI/CD', 'DevOps',
            'Machine Learning', 'Deep Learning', 'AI', 'Data Science',
            'HTML', 'CSS', 'Bootstrap', 'jQuery',
            'Linux', 'Windows', 'macOS'
        }
        
        text_content = ""
        if 'job_description' in job_data:
            text_content += job_data['job_description'] + " "
        if 'job_requirements' in job_data:
            text_content += job_data['job_requirements'] + " "
        
        found_skills = []
        for skill in skill_keywords:
            if skill.lower() in text_content.lower():
                found_skills.append(skill)
        
        return found_skills
    
    def _analyze_job_level(self, title: str) -> Optional[str]:
        """
        分析职位级别
        
        Args:
            title: 职位标题
            
        Returns:
            职位级别
        """
        title_lower = title.lower()
        
        if any(keyword in title_lower for keyword in ['实习', 'intern']):
            return '实习生'
        elif any(keyword in title_lower for keyword in ['初级', 'junior', '助理']):
            return '初级'
        elif any(keyword in title_lower for keyword in ['中级', 'middle', '中等']):
            return '中级'
        elif any(keyword in title_lower for keyword in ['高级', 'senior', '资深']):
            return '高级'
        elif any(keyword in title_lower for keyword in ['主管', 'lead', '领导', '经理', 'manager']):
            return '管理层'
        elif any(keyword in title_lower for keyword in ['总监', 'director', 'vp', '副总']):
            return '高管'
        else:
            return '中级'  # 默认中级
    
    def _analyze_work_mode(self, job_data: Dict[str, Any]) -> Optional[str]:
        """
        分析工作模式
        
        Args:
            job_data: 职位数据
            
        Returns:
            工作模式
        """
        text_content = ""
        if 'job_description' in job_data:
            text_content += job_data['job_description'] + " "
        if 'welfare' in job_data:
            text_content += job_data['welfare'] + " "
        
        text_lower = text_content.lower()
        
        if any(keyword in text_lower for keyword in ['远程', '在家', 'remote', 'wfh']):
            return '远程'
        elif any(keyword in text_lower for keyword in ['混合', 'hybrid', '弹性']):
            return '混合'
        else:
            return '现场'
    
    def _enrich_company_info(self, company_name: str) -> Dict[str, Any]:
        """
        增强公司信息
        
        Args:
            company_name: 公司名称
            
        Returns:
            增强的公司信息
        """
        if not company_name:
            return {}
        
        # 检查缓存
        if company_name in self._company_cache:
            return self._company_cache[company_name]
        
        company_info = {}
        
        # 分析公司类型
        if any(keyword in company_name for keyword in ['科技', '技术', 'Technology', 'Tech']):
            company_info['company_category'] = '科技公司'
        elif any(keyword in company_name for keyword in ['金融', '银行', '投资', '证券']):
            company_info['company_category'] = '金融公司'
        elif any(keyword in company_name for keyword in ['教育', '培训', '学校']):
            company_info['company_category'] = '教育机构'
        elif any(keyword in company_name for keyword in ['医疗', '医院', '健康']):
            company_info['company_category'] = '医疗机构'
        else:
            company_info['company_category'] = '其他'
        
        # 缓存结果
        self._company_cache[company_name] = company_info
        
        return company_info
    
    def process_batch(self, job_data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        批量处理职位数据
        
        Args:
            job_data_list: 职位数据列表
            
        Returns:
            处理后的职位数据列表
        """
        if not job_data_list:
            return []
        
        logger.info(f"开始批量处理 {len(job_data_list)} 条职位数据")
        
        processed_jobs = []
        for job_data in job_data_list:
            processed_job = self.process_job_data(job_data)
            if processed_job:
                processed_jobs.append(processed_job)
        
        logger.info(f"批量处理完成: 输入 {len(job_data_list)}, 输出 {len(processed_jobs)}")
        return processed_jobs
    
    def process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        处理DataFrame格式的职位数据
        
        Args:
            df: 输入DataFrame
            
        Returns:
            处理后的DataFrame
        """
        if df.empty:
            return df
        
        logger.info(f"开始处理DataFrame: {len(df)} 行")
        
        # 转换为字典列表
        job_data_list = df.to_dict('records')
        
        # 批量处理
        processed_jobs = self.process_batch(job_data_list)
        
        # 转换回DataFrame
        if processed_jobs:
            result_df = pd.DataFrame(processed_jobs)
            logger.info(f"DataFrame处理完成: 输入 {len(df)}, 输出 {len(result_df)}")
            return result_df
        else:
            logger.warning("没有有效的处理结果")
            return pd.DataFrame()
    
    def get_processing_report(self) -> Dict[str, Any]:
        """
        获取处理报告
        
        Returns:
            处理报告字典
        """
        return {
            'stats': self.stats,
            'cache_info': {
                'seen_job_ids': len(self._seen_job_ids),
                'company_cache': len(self._company_cache),
                'location_cache': len(self._location_cache)
            },
            'settings': {
                'enable_enrichment': self.enable_enrichment,
                'enable_deduplication': self.enable_deduplication
            }
        }
    
    def get_stats(self) -> ProcessingStats:
        """
        获取处理统计信息
        
        Returns:
            处理统计信息
        """
        return self.stats
    
    def reset_stats(self):
        """重置统计信息"""
        self.stats = ProcessingStats()
        logger.info("数据处理统计信息已重置")
    
    def clear_cache(self):
        """清空缓存"""
        self._seen_job_ids.clear()
        self._company_cache.clear()
        self._location_cache.clear()
        logger.info("数据处理缓存已清空")
    
    def export_seen_job_ids(self) -> List[str]:
        """
        导出已处理的job_id列表
        
        Returns:
            job_id列表
        """
        return list(self._seen_job_ids)
    
    def import_seen_job_ids(self, job_ids: List[str]):
        """
        导入已处理的job_id列表
        
        Args:
            job_ids: job_id列表
        """
        self._seen_job_ids.update(job_ids)
        logger.info(f"导入了 {len(job_ids)} 个已处理的job_id")
    
    def __str__(self) -> str:
        """字符串表示"""
        return f"DataProcessor(processed={self.stats.processed_records}, valid={self.stats.valid_records})"
    
    def __repr__(self) -> str:
        """对象表示"""
        return self.__str__()