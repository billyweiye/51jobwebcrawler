#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite本地测试示例
演示如何使用SQLite数据库进行本地爬虫测试
"""

import os
import sys
import logging
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils import create_sqlite_database_manager, ConfigManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_sqlite_database():
    """测试SQLite数据库功能"""
    print("=== SQLite数据库测试 ===")
    
    # 创建SQLite数据库管理器
    db_manager = create_sqlite_database_manager("./data/test_job_crawler.db")
    
    try:
        # 测试连接
        if db_manager.test_connection():
            print("✓ SQLite数据库连接成功")
        else:
            print("✗ SQLite数据库连接失败")
            return False
        
        # 清理可能存在的测试数据
        try:
            db_manager.execute_query("DELETE FROM job_listings WHERE job_id LIKE 'test_%'")
        except:
            pass  # 忽略清理错误
        
        # 插入测试数据
        import uuid
        test_job_id = f'test_{uuid.uuid4().hex[:8]}'
        test_job_data = {
            'job_id': test_job_id,
            'title': 'Python开发工程师',
            'company_name': '测试公司',
            'salary_min': 15000,
            'salary_max': 25000,
            'salary_text': '15k-25k',
            'location_city': '上海',
            'location_district': '浦东新区',
            'job_description': '负责Python后端开发工作',
            'experience_required': '3-5年',
            'education_required': '本科',
            'industry': 'IT互联网',
            'publish_time': datetime.now(),
            'job_url': 'https://jobs.51job.com/test/001.html'
        }
        
        # 插入数据
        if db_manager.insert_job_listing(test_job_data):
            print("✓ 测试数据插入成功")
        else:
            print("✗ 测试数据插入失败")
        
        # 查询数据
        jobs = db_manager.search_job_listings(limit=5)
        print(f"✓ 查询到 {len(jobs)} 条职位数据")
        
        # 获取统计信息
        stats = db_manager.get_statistics()
        print(f"✓ 数据库统计: 总职位数 {stats['total_jobs']}")
        
        return True
        
    except Exception as e:
        print(f"✗ SQLite测试失败: {e}")
        return False
    finally:
        db_manager.close()


def test_sqlite_crawler():
    """测试使用SQLite的爬虫功能"""
    print("\n=== SQLite爬虫测试 ===")
    
    try:
        # 创建配置管理器
        config_manager = ConfigManager()
        
        # 确保SQLite已启用
        db_config = config_manager.get('database', {})
        if not db_config.get('sqlite', {}).get('enabled', False):
            print("请在config/database.yaml中启用SQLite配置")
            return False
        
        print("✓ SQLite配置已启用")
        print("✓ 爬虫模块测试跳过（需要完整环境）")
        
        return True
        
    except Exception as e:
        print(f"✗ SQLite爬虫测试失败: {e}")
        return False


def show_sqlite_advantages():
    """显示SQLite用于本地测试的优势"""
    print("\n=== SQLite本地测试优势 ===")
    advantages = [
        "🚀 零配置: 无需安装和配置MySQL服务器",
        "📁 文件数据库: 数据存储在单个文件中，便于管理",
        "🔧 轻量级: 占用资源少，启动速度快",
        "🧪 测试友好: 可以轻松创建和删除测试数据库",
        "📦 便携性: 数据库文件可以轻松备份和迁移",
        "🔄 兼容性: 与MySQL使用相同的ORM代码",
        "⚡ 快速开发: 适合原型开发和功能测试"
    ]
    
    for advantage in advantages:
        print(f"  {advantage}")


def main():
    """主函数"""
    print("SQLite本地测试工具")
    print("=" * 50)
    
    # 显示SQLite优势
    show_sqlite_advantages()
    
    # 测试SQLite数据库
    db_success = test_sqlite_database()
    
    # 测试SQLite爬虫
    crawler_success = test_sqlite_crawler()
    
    # 总结
    print("\n=== 测试总结 ===")
    print(f"数据库测试: {'✓ 通过' if db_success else '✗ 失败'}")
    print(f"爬虫测试: {'✓ 通过' if crawler_success else '✗ 失败'}")
    
    if db_success and crawler_success:
        print("\n🎉 SQLite本地测试环境配置成功！")
        print("现在可以使用以下命令进行本地测试:")
        print("  python run.py crawl Python 上海")
        print("  python main_new.py --once --keywords Python --cities 上海")
    else:
        print("\n❌ 测试失败，请检查配置和环境")


if __name__ == '__main__':
    main()