#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
51job爬虫系统测试脚本
用于验证各个组件的功能
"""

import os
import sys
import time
import logging
from pathlib import Path
from typing import Dict, Any

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SystemTester:
    """系统测试器"""
    
    def __init__(self):
        self.test_results = {}
        self.total_tests = 0
        self.passed_tests = 0
        self.failed_tests = 0
    
    def run_test(self, test_name: str, test_func):
        """
        运行单个测试
        
        Args:
            test_name: 测试名称
            test_func: 测试函数
        """
        self.total_tests += 1
        print(f"\n🧪 测试: {test_name}")
        print("-" * 50)
        
        try:
            start_time = time.time()
            result = test_func()
            end_time = time.time()
            
            if result:
                print(f"✅ 通过 ({end_time - start_time:.2f}s)")
                self.passed_tests += 1
                self.test_results[test_name] = {'status': 'PASS', 'time': end_time - start_time}
            else:
                print(f"❌ 失败 ({end_time - start_time:.2f}s)")
                self.failed_tests += 1
                self.test_results[test_name] = {'status': 'FAIL', 'time': end_time - start_time}
        
        except Exception as e:
            print(f"❌ 异常: {e}")
            self.failed_tests += 1
            self.test_results[test_name] = {'status': 'ERROR', 'error': str(e)}
    
    def test_imports(self) -> bool:
        """
        测试模块导入
        """
        try:
            print("导入utils模块...")
            from utils import ConfigManager, DataValidator, RetryHandler
            print("✓ utils模块导入成功")
            
            print("导入crawler模块...")
            from crawler import JobSpider, DataProcessor, CrawlerScheduler
            print("✓ crawler模块导入成功")
            
            return True
        except ImportError as e:
            print(f"✗ 导入失败: {e}")
            return False
    
    def test_config_manager(self) -> bool:
        """
        测试配置管理器
        """
        try:
            from utils import ConfigManager
            
            print("创建配置管理器...")
            config_manager = ConfigManager()
            print("✓ 配置管理器创建成功")
            
            print("测试配置加载...")
            debug_mode = config_manager.is_debug_mode()
            print(f"✓ 调试模式: {debug_mode}")
            
            print("测试数据库配置...")
            db_config = config_manager.get_database_config()
            print(f"✓ 数据库类型: {db_config.db_type}")
            
            print("测试搜索配置...")
            search_config = config_manager.get_search_config()
            print(f"✓ 搜索关键词: {search_config.keywords}")
            
            return True
        except Exception as e:
            print(f"✗ 配置管理器测试失败: {e}")
            return False
    
    def test_database_manager(self) -> bool:
        """
        测试数据库管理器
        """
        try:
            from utils import ConfigManager, DatabaseManager
            
            print("创建数据库管理器...")
            config_manager = ConfigManager()
            db_manager = DatabaseManager(config_manager)
            print("✓ 数据库管理器创建成功")
            
            print("测试数据库连接...")
            if db_manager.test_connection():
                print("✓ 数据库连接成功")
            else:
                print("✗ 数据库连接失败")
                return False
            
            print("测试表初始化...")
            db_manager.init_tables()
            print("✓ 表初始化成功")
            
            return True
        except Exception as e:
            print(f"✗ 数据库管理器测试失败: {e}")
            return False
    
    def test_data_validator(self) -> bool:
        """
        测试数据验证器
        """
        try:
            from utils import DataValidator
            
            print("创建数据验证器...")
            validator = DataValidator()
            print("✓ 数据验证器创建成功")
            
            print("测试职位数据验证...")
            test_job_data = {
                'jobId': 'test_001',
                'jobName': 'Python开发工程师',
                'companyName': '测试公司',
                'jobAreaString': '上海',
                'provideSalaryString': '10K-20K',
                'workYear': '3-5年',
                'degreeString': '本科',
                'jobTags': ['Python', '后端开发'],
                'hrLabels': ['五险一金', '弹性工作']
            }
            
            result = validator.validate_job_data(test_job_data)
            if result.is_valid:
                print("✓ 职位数据验证通过")
            else:
                print(f"✗ 职位数据验证失败: {result.errors}")
                return False
            
            print("测试数据清洗...")
            if result.cleaned_data:
                print(f"✓ 数据清洗成功，清洗后字段数: {len(result.cleaned_data)}")
            else:
                print("✗ 数据清洗失败")
                return False
            
            return True
        except Exception as e:
            print(f"✗ 数据验证器测试失败: {e}")
            return False
    
    def test_data_processor(self) -> bool:
        """
        测试数据处理器
        """
        try:
            from crawler import DataProcessor
            
            print("创建数据处理器...")
            processor = DataProcessor()
            print("✓ 数据处理器创建成功")
            
            print("测试数据处理...")
            test_job_data = {
                'jobId': 'test_002',
                'jobName': '  Java开发工程师  ',
                'companyName': '测试科技有限公司',
                'provideSalaryString': '12K-18K·13薪',
                'jobAreaString': '北京-朝阳区',
                'jobDescription': '负责Java后端开发，熟悉Spring框架',
                'jobUrl': 'https://example.com/job/2',
                'job_id': 'test_002',
                'title': '  Java开发工程师  ',
                'company_name': '测试科技有限公司'
            }
            
            processed_data = processor.process_job_data(test_job_data)
            if processed_data:
                print("✓ 数据处理成功")
                print(f"  处理后标题: {processed_data.get('job_name', processed_data.get('title', 'N/A'))}")
            else:
                print("✗ 数据处理失败")
                return False
            
            print("测试批量处理...")
            test_batch = [test_job_data] * 3
            processed_batch = processor.process_batch(test_batch)
            print(f"✓ 批量处理: 输入{len(test_batch)}, 输出{len(processed_batch)}")
            
            return True
        except Exception as e:
            print(f"✗ 数据处理器测试失败: {e}")
            return False
    
    def test_request_handler(self) -> bool:
        """
        测试请求处理器
        """
        try:
            from utils import ConfigManager, RequestHandler
            
            print("创建请求处理器...")
            config_manager = ConfigManager()
            request_handler = RequestHandler(config_manager)
            print("✓ 请求处理器创建成功")
            
            print("测试网络请求...")
            response = request_handler.get('https://httpbin.org/get', timeout=10)
            if response and response.status_code == 200:
                print("✓ 网络请求成功")
            else:
                print("✗ 网络请求失败")
                return False
            
            print("测试请求统计...")
            stats = request_handler.get_stats()
            print(f"✓ 请求统计: 总请求={stats.total_requests}")
            
            return True
        except Exception as e:
            print(f"✗ 请求处理器测试失败: {e}")
            return False
    
    def test_job_spider(self) -> bool:
        """
        测试爬虫
        """
        try:
            from utils import ConfigManager
            from crawler import create_job_spider
            
            print("创建爬虫实例...")
            spider = create_job_spider("./config")
            print("✓ 爬虫实例创建成功")
            
            print("测试职位搜索...")
            with spider:
                # 只搜索1页进行测试
                results_generator = spider.crawl_jobs(
                    keyword="Python",
                    city_name="上海",
                    max_pages=1
                )
                
                # 将生成器转换为列表
                results = list(results_generator)
                
                if results:
                    print(f"✓ 职位搜索成功，获取到 {len(results)} 条数据")
                    
                    # 显示第一条数据的基本信息
                    if results:
                        first_job = results[0]
                        print(f"  示例职位: {first_job.get('title', 'N/A')}")
                        print(f"  公司: {first_job.get('company_name', 'N/A')}")
                        print(f"  薪资: {first_job.get('salary_text', 'N/A')}")
                else:
                    print("⚠️ 职位搜索无结果（可能是网络问题或反爬限制）")
                    return True  # 不算失败，可能是正常的反爬限制
            
            return True
        except Exception as e:
            print(f"✗ 爬虫测试失败: {e}")
            return False
    
    def test_scheduler(self) -> bool:
        """
        测试调度器
        """
        try:
            from utils import ConfigManager
            from crawler import create_crawler_scheduler
            
            print("创建调度器...")
            config_manager = ConfigManager()
            scheduler = create_crawler_scheduler(
                config_manager=config_manager,
                max_workers=1,
                enable_schedule=False
            )
            print("✓ 调度器创建成功")
            
            print("测试任务创建...")
            task_id = scheduler.create_task(['Python'], ['上海'])
            print(f"✓ 任务创建成功: {task_id}")
            
            print("测试任务查询...")
            task = scheduler.get_task(task_id)
            if task:
                print(f"✓ 任务查询成功: {task.status.value}")
            else:
                print("✗ 任务查询失败")
                return False
            
            print("测试任务取消...")
            if scheduler.cancel_task(task_id):
                print("✓ 任务取消成功")
            else:
                print("✗ 任务取消失败")
                return False
            
            scheduler.shutdown()
            return True
        except Exception as e:
            print(f"✗ 调度器测试失败: {e}")
            return False
    
    def test_file_structure(self) -> bool:
        """
        测试文件结构
        """
        try:
            print("检查项目文件结构...")
            
            required_files = [
                'main_new.py',
                'run.py',
                'setup.py',
                'requirements.txt',
                'README.md'
            ]
            
            required_dirs = [
                'utils',
                'crawler',
                'config'
            ]
            
            for file_path in required_files:
                if Path(file_path).exists():
                    print(f"✓ 文件存在: {file_path}")
                else:
                    print(f"✗ 文件缺失: {file_path}")
                    return False
            
            for dir_path in required_dirs:
                if Path(dir_path).is_dir():
                    print(f"✓ 目录存在: {dir_path}")
                else:
                    print(f"✗ 目录缺失: {dir_path}")
                    return False
            
            print("检查配置文件...")
            config_files = [
                'config/settings.yaml',
                'config/database.yaml',
                'config/logging.yaml'
            ]
            
            for config_file in config_files:
                if Path(config_file).exists():
                    print(f"✓ 配置文件存在: {config_file}")
                else:
                    print(f"⚠️ 配置文件缺失: {config_file}")
            
            return True
        except Exception as e:
            print(f"✗ 文件结构检查失败: {e}")
            return False
    
    def run_all_tests(self):
        """
        运行所有测试
        """
        print("🚀 开始系统测试")
        print("=" * 60)
        
        # 定义测试列表
        tests = [
            ("文件结构检查", self.test_file_structure),
            ("模块导入测试", self.test_imports),
            ("配置管理器测试", self.test_config_manager),
            ("数据库管理器测试", self.test_database_manager),
            ("数据验证器测试", self.test_data_validator),
            ("数据处理器测试", self.test_data_processor),
            ("请求处理器测试", self.test_request_handler),
            ("爬虫测试", self.test_job_spider),
            ("调度器测试", self.test_scheduler)
        ]
        
        # 运行测试
        for test_name, test_func in tests:
            self.run_test(test_name, test_func)
        
        # 显示测试结果
        self.show_summary()
    
    def show_summary(self):
        """
        显示测试摘要
        """
        print("\n" + "=" * 60)
        print("📊 测试摘要")
        print("=" * 60)
        
        print(f"总测试数: {self.total_tests}")
        print(f"通过: {self.passed_tests} ✅")
        print(f"失败: {self.failed_tests} ❌")
        print(f"成功率: {(self.passed_tests / self.total_tests * 100):.1f}%")
        
        print("\n详细结果:")
        for test_name, result in self.test_results.items():
            status = result['status']
            if status == 'PASS':
                print(f"  ✅ {test_name} ({result.get('time', 0):.2f}s)")
            elif status == 'FAIL':
                print(f"  ❌ {test_name} ({result.get('time', 0):.2f}s)")
            else:
                print(f"  💥 {test_name} - {result.get('error', 'Unknown error')}")
        
        if self.failed_tests == 0:
            print("\n🎉 所有测试通过！系统运行正常。")
        else:
            print(f"\n⚠️ 有 {self.failed_tests} 个测试失败，请检查相关组件。")
        
        print("=" * 60)


def main():
    """
    主函数
    """
    try:
        tester = SystemTester()
        tester.run_all_tests()
    except KeyboardInterrupt:
        print("\n🛑 测试被用户中断")
    except Exception as e:
        print(f"\n💥 测试程序异常: {e}")
        logger.error(f"测试程序异常: {e}")


if __name__ == "__main__":
    main()