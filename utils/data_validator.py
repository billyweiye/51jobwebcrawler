#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据验证器
用于验证和清洗爬取的数据
"""

import re
import logging
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass
from datetime import datetime, date
import json


class DateTimeEncoder(json.JSONEncoder):
    """自定义JSON编码器，处理datetime对象"""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """验证结果类"""
    is_valid: bool
    cleaned_data: Dict[str, Any]
    errors: List[str]
    warnings: List[str]
    
    def add_error(self, message: str):
        """添加错误信息"""
        self.errors.append(message)
        self.is_valid = False
    
    def add_warning(self, message: str):
        """添加警告信息"""
        self.warnings.append(message)


class DataValidator:
    """数据验证器"""
    
    # 薪资范围正则表达式
    SALARY_PATTERNS = [
        r'(\d+)-(\d+)千/月',
        r'(\d+)-(\d+)万/年',
        r'(\d+)-(\d+)K',
        r'(\d+\.\d+)-(\d+\.\d+)万/月',
        r'(\d+)千-(\d+)万/月',
        r'面议',
        r'薪资面议'
    ]
    
    # 必需字段列表 (使用原始数据字段名)
    REQUIRED_FIELDS = ['jobId', 'jobName', 'companyName', 'jobAreaString']
    
    # 字段类型映射
    FIELD_TYPES = {
        'jobId': str,
        'jobName': str,
        'companyName': str,
        'jobAreaString': str,
        'provideSalaryString': str,
        'workYear': str,
        'degreeString': str,
        'jobTags': (list, str),
        'issueDateString': str,
        'confirmDateString': str,
        'industryType1Str': str,
        'companySizeString': str,
        'companyTypeString': str
    }
    
    def __init__(self):
        """初始化数据验证器"""
        self.validation_stats = {
            'total_validated': 0,
            'valid_count': 0,
            'invalid_count': 0,
            'warning_count': 0
        }
    
    def validate_job_data(self, data: Dict[str, Any]) -> ValidationResult:
        """
        验证职位数据
        
        Args:
            data: 原始职位数据
            
        Returns:
            验证结果
        """
        result = ValidationResult(
            is_valid=True,
            cleaned_data={},
            errors=[],
            warnings=[]
        )
        
        self.validation_stats['total_validated'] += 1
        
        try:
            # 检查必需字段
            self._validate_required_fields(data, result)
            
            # 验证和清洗各个字段
            self._validate_job_id(data, result)
            self._validate_job_name(data, result)
            self._validate_company_name(data, result)
            self._validate_location(data, result)
            self._validate_salary(data, result)
            self._validate_work_experience(data, result)
            self._validate_education(data, result)
            self._validate_tags(data, result)
            self._validate_coordinates(data, result)
            self._validate_dates(data, result)
            self._validate_urls(data, result)
            
            # 添加元数据
            self._add_metadata(data, result)
            
            # 更新统计信息
            if result.is_valid:
                self.validation_stats['valid_count'] += 1
            else:
                self.validation_stats['invalid_count'] += 1
            
            if result.warnings:
                self.validation_stats['warning_count'] += 1
            
        except Exception as e:
            result.add_error(f"验证过程中发生异常: {e}")
            logger.error(f"数据验证异常: {e}", exc_info=True)
        
        return result
    
    def _validate_required_fields(self, data: Dict[str, Any], result: ValidationResult):
        """验证必需字段"""
        field_mapping = {
            'jobId': 'job_id',
            'jobName': 'title', 
            'companyName': 'company_name',
            'jobAreaString': 'location_city'
        }
        
        for field in self.REQUIRED_FIELDS:
            if field not in data or not data[field]:
                mapped_field = field_mapping.get(field, field)
                result.add_error(f"缺少必需字段: {mapped_field}")
    
    def _validate_job_id(self, data: Dict[str, Any], result: ValidationResult):
        """验证职位ID"""
        job_id = data.get('jobId')
        if job_id:
            # 清理职位ID
            cleaned_id = str(job_id).strip()
            if cleaned_id:
                result.cleaned_data['job_id'] = cleaned_id
            else:
                result.add_error("职位ID为空")
    
    def _validate_job_name(self, data: Dict[str, Any], result: ValidationResult):
        """验证职位名称"""
        job_name = data.get('jobName')
        if job_name:
            # 清理职位名称
            cleaned_name = str(job_name).strip()
            # 移除多余的空格
            cleaned_name = re.sub(r'\s+', ' ', cleaned_name)
            
            if len(cleaned_name) > 200:
                result.add_warning(f"职位名称过长: {len(cleaned_name)} 字符")
                cleaned_name = cleaned_name[:200]
            
            result.cleaned_data['title'] = cleaned_name
    
    def _validate_company_name(self, data: Dict[str, Any], result: ValidationResult):
        """验证公司名称"""
        company_name = data.get('companyName') or data.get('fullCompanyName')
        if company_name:
            cleaned_name = str(company_name).strip()
            cleaned_name = re.sub(r'\s+', ' ', cleaned_name)
            
            if len(cleaned_name) > 200:
                result.add_warning(f"公司名称过长: {len(cleaned_name)} 字符")
                cleaned_name = cleaned_name[:200]
            
            result.cleaned_data['company_name'] = cleaned_name
            
        # 公司类型和规模
        result.cleaned_data['company_type'] = self._clean_string(data.get('companyTypeString'))
        result.cleaned_data['company_size'] = self._clean_string(data.get('companySizeString'))
    
    def _validate_location(self, data: Dict[str, Any], result: ValidationResult):
        """验证位置信息"""
        location = data.get('jobAreaString')
        if location:
            cleaned_location = str(location).strip()
            result.cleaned_data['location_city'] = cleaned_location
        
        # 详细地址
        result.cleaned_data['location_address'] = self._clean_string(data.get('jobAreaString'))
    
    def _validate_salary(self, data: Dict[str, Any], result: ValidationResult):
        """验证薪资信息"""
        salary_text = data.get('provideSalaryString', '')
        if salary_text:
            result.cleaned_data['salary_text'] = str(salary_text).strip()
            
            # 解析薪资范围
            try:
                salary_min, salary_max = self.parse_salary(salary_text)
                if salary_min is not None:
                    result.cleaned_data['salary_min'] = salary_min
                if salary_max is not None:
                    result.cleaned_data['salary_max'] = salary_max
            except Exception as e:
                result.add_warning(f"薪资解析失败: {salary_text}, 错误: {e}")
    
    def _parse_salary(self, salary_text: str) -> tuple:
        """
        解析薪资文本
        
        Args:
            salary_text: 薪资文本
            
        Returns:
            (最低薪资, 最高薪资) 单位：千元/月
        """
        if not salary_text:
            return None, None
        
        salary_text = salary_text.strip()
        
        # 面议情况
        if '面议' in salary_text:
            return None, None
        
        # 匹配各种薪资格式
        for pattern in self.SALARY_PATTERNS:
            match = re.search(pattern, salary_text)
            if match:
                try:
                    if '千/月' in pattern:
                        min_sal = float(match.group(1))
                        max_sal = float(match.group(2))
                        return min_sal, max_sal
                    elif '万/年' in pattern:
                        min_sal = float(match.group(1)) * 10 / 12  # 转换为千/月
                        max_sal = float(match.group(2)) * 10 / 12
                        return round(min_sal, 1), round(max_sal, 1)
                    elif 'K' in pattern:
                        min_sal = float(match.group(1))
                        max_sal = float(match.group(2))
                        return min_sal, max_sal
                    elif '万/月' in pattern:
                        min_sal = float(match.group(1)) * 10
                        max_sal = float(match.group(2)) * 10
                        return min_sal, max_sal
                    elif '千-' in pattern and '万/月' in pattern:
                        min_sal = float(match.group(1))
                        max_sal = float(match.group(2)) * 10
                        return min_sal, max_sal
                except (ValueError, IndexError):
                    continue
        
        return None, None
    
    def _validate_work_experience(self, data: Dict[str, Any], result: ValidationResult):
        """验证工作经验"""
        work_year = data.get('workYear') or data.get('workYearString')
        result.cleaned_data['experience_required'] = self._clean_string(work_year)
    
    def _validate_education(self, data: Dict[str, Any], result: ValidationResult):
        """验证学历要求"""
        education = data.get('degreeString')
        result.cleaned_data['education_required'] = self._clean_string(education)
    
    def _validate_tags(self, data: Dict[str, Any], result: ValidationResult):
        """验证标签信息"""
        # 职位标签
        job_tags = data.get('jobTags', [])
        if isinstance(job_tags, list):
            cleaned_tags = [str(tag).strip() for tag in job_tags if tag]
            result.cleaned_data['job_tags'] = json.dumps(cleaned_tags, ensure_ascii=False) if cleaned_tags else None
        elif isinstance(job_tags, str):
            # 如果是字符串，尝试解析
            try:
                if job_tags.startswith('[') and job_tags.endswith(']'):
                    parsed_tags = json.loads(job_tags)
                    result.cleaned_data['job_tags'] = json.dumps(parsed_tags, ensure_ascii=False) if parsed_tags else None
                else:
                    result.cleaned_data['job_tags'] = job_tags if job_tags else None
            except json.JSONDecodeError:
                result.cleaned_data['job_tags'] = job_tags if job_tags else None
        else:
            result.cleaned_data['job_tags'] = None
    
    def _validate_coordinates(self, data: Dict[str, Any], result: ValidationResult):
        """验证坐标信息"""
        lon = data.get('lon')
        lat = data.get('lat')
        
        # 验证经度
        if lon is not None and lon != '':
            try:
                lon_val = float(lon)
                if -180 <= lon_val <= 180:
                    result.cleaned_data['coordinates_lng'] = lon_val
                else:
                    result.add_warning(f"经度值超出范围: {lon_val}")
            except (ValueError, TypeError):
                result.add_warning(f"无效的经度值: {lon}")
        
        # 验证纬度
        if lat is not None and lat != '':
            try:
                lat_val = float(lat)
                if -90 <= lat_val <= 90:
                    result.cleaned_data['coordinates_lat'] = lat_val
                else:
                    result.add_warning(f"纬度值超出范围: {lat_val}")
            except (ValueError, TypeError):
                result.add_warning(f"无效的纬度值: {lat}")
    
    def _validate_dates(self, data: Dict[str, Any], result: ValidationResult):
        """验证日期信息"""
        # 发布日期
        publish_date = data.get('issueDateString')
        if publish_date:
            parsed_date = self._parse_date(publish_date)
            if parsed_date:
                result.cleaned_data['publish_time'] = parsed_date
            else:
                result.add_warning(f"无法解析发布日期: {publish_date}")
        
        # 更新日期
        update_date = data.get('updateDateTime')
        if update_date:
            parsed_date = self._parse_date(update_date)
            if parsed_date:
                result.cleaned_data['update_time'] = parsed_date
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """解析日期字符串"""
        if not date_str:
            return None
        
        # 常见日期格式
        date_formats = [
            '%Y-%m-%d',
            '%Y-%m-%d %H:%M:%S',
            '%Y/%m/%d',
            '%m-%d',
            '%m/%d'
        ]
        
        for fmt in date_formats:
            try:
                parsed = datetime.strptime(date_str, fmt)
                # 如果只有月日，补充当前年份
                if fmt in ['%m-%d', '%m/%d']:
                    parsed = parsed.replace(year=datetime.now().year)
                return parsed
            except ValueError:
                continue
        
        return None
    
    def _validate_urls(self, data: Dict[str, Any], result: ValidationResult):
        """验证URL信息"""
        job_href = data.get('jobHref')
        if job_href:
            result.cleaned_data['job_url'] = self._clean_url(job_href)
        
        company_href = data.get('companyHref')
        if company_href:
            result.cleaned_data['company_url'] = self._clean_url(company_href)
    
    def _clean_url(self, url: str) -> str:
        """清理URL"""
        if not url:
            return ''
        
        url = url.strip()
        # 如果是相对URL，添加域名
        if url.startswith('//'):
            url = 'https:' + url
        elif url.startswith('/'):
            url = 'https://51job.com' + url
        
        return url
    
    def _add_metadata(self, data: Dict[str, Any], result: ValidationResult):
        """添加元数据"""
        # 职位描述
        result.cleaned_data['job_description'] = self._clean_string(data.get('jobDescribe'))
        
        # 行业信息
        result.cleaned_data['industry'] = self._clean_string(data.get('industryType1Str'))
        
        # 原始数据 - 直接存储字典对象，数据库的JSON字段会自动处理序列化
        result.cleaned_data['raw_data'] = data
    
    def _clean_string(self, value: Any) -> Optional[str]:
        """清理字符串值"""
        if value is None or value == '':
            return None
        
        cleaned = str(value).strip()
        # 移除多余的空格
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        return cleaned if cleaned else None
    
    def get_validation_stats(self) -> Dict[str, Any]:
        """获取验证统计信息"""
        stats = self.validation_stats.copy()
        if stats['total_validated'] > 0:
            stats['valid_rate'] = stats['valid_count'] / stats['total_validated']
            stats['warning_rate'] = stats['warning_count'] / stats['total_validated']
        else:
            stats['valid_rate'] = 0.0
            stats['warning_rate'] = 0.0
        
        return stats
    
    def reset_stats(self):
        """重置统计信息"""
        self.validation_stats = {
            'total_validated': 0,
            'valid_count': 0,
            'invalid_count': 0,
            'warning_count': 0
        }
        logger.info("数据验证统计信息已重置")