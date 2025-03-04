import re
import json
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
import pandas as pd
import logging

class DataProcessor:
    def __init__(self, logger: logging.Logger = None):
        self.logger = logger or logging.getLogger(__name__)
        
    def clean_html(self, raw_data: List[Dict]) -> List[Dict]:
        """清洗HTML标签和特殊字符"""
        for item in raw_data:
            for key in ['jobDescribe', 'jobTags']:
                if key in item:
                    soup = BeautifulSoup(item[key], 'html.parser')
                    item[key] = soup.get_text(separator=' ', strip=True)
                    item[key] = re.sub(r'\s+', ' ', item[key])  # 合并多个空格
            # 清理薪资字段
            if 'jobSalary' in item:
                item['jobSalary'] = re.sub(r'[^\d-~／/]', '', str(item['jobSalary']))
        return raw_data

    def validate_data(self, data: List[Dict]) -> List[Dict]:
        """数据验证和过滤"""
        valid_data = []
        required_fields = ['jobId', 'jobName', 'coId']
        
        for item in data:
            if all(item.get(field) for field in required_fields):
                valid_data.append(item)
            else:
                self.logger.warning(f"Invalid data: {json.dumps(item, ensure_ascii=False)}")
        return valid_data

    def remove_duplicates(self, data: List[Dict], keys: List[str] = ['jobId', 'coId']) -> List[Dict]:
        """基于关键字段去重"""
        df = pd.DataFrame(data)
        return df.drop_duplicates(subset=keys).to_dict('records')

    def transform_salary(self, data: List[Dict]) -> List[Dict]:
        """薪资字段转换处理"""
        for item in data:
            salary = str(item.get('jobSalary', '')).replace(' ', '')
            if '-' in salary:
                min_max = re.findall(r'(\d+)', salary)
                if len(min_max) >= 2:
                    item['salary_min'] = int(min_max[0])
                    item['salary_max'] = int(min_max[1])
            elif '面议' in salary:
                item['salary_min'] = item['salary_max'] = None
            # 保留原始字段
            item['jobSalary'] = salary
        return data

    def process(self, raw_data: List[Dict]) -> List[Dict]:
        """完整处理流水线"""
        processed_data = self.clean_html(raw_data)
        processed_data = self.validate_data(processed_data)
        processed_data = self.remove_duplicates(processed_data)
        processed_data = self.transform_salary(processed_data)
        return processed_data

    def convert_to_feishu_format(self, data: List[Dict]) -> List[Dict]:
        """转换为飞书多维表格式"""
        feishu_data = []
        for item in data:
            feishu_item = {
                'fields': {
                    'jobId': str(item.get('jobId', '')),
                    'jobName': item.get('jobName', ''),
                    'companyName': item.get('companyName', ''),
                    'salaryRange': f"{item.get('salary_min', '')}-{item.get('salary_max', '')}",
                    'location': self._parse_location(item.get('jobArea', '')),
                    'jobTags': ', '.join(json.loads(item.get('jobTags', '[]'))),
                    'rawData': json.dumps(item, ensure_ascii=False)
                }
            }
            feishu_data.append(feishu_item)
        return feishu_data

    def _parse_location(self, location_code: str) -> str:
        """解析城市编码（需要结合城市编码表）"""
        # 示例映射，实际应从配置文件或数据库加载
        city_map = {'020000': '上海', '010000': '北京'}
        return city_map.get(location_code, '未知地区')

# 示例用法
if __name__ == "__main__":
    processor = DataProcessor()
    
    # 原始数据示例
    raw_example = [{
        "jobId": "12345",
        "jobName": "高级数据分析师",
        "jobSalary": "15-30K·15薪",
        "jobArea": "020000",
        "jobDescribe": "<p>负责数据挖掘工作</p>",
        "jobTags": "['大数据', '机器学习']"
    }]
    
    processed = processor.process(raw_example)
    feishu_data = processor.convert_to_feishu_format(processed)
    print(json.dumps(feishu_data, indent=2, ensure_ascii=False))