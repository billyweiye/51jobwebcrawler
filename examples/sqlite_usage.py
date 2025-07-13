#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite使用示例
演示如何在代码中使用SQLite数据库进行本地开发和测试
"""

import os
import sys
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import (
    create_sqlite_database_manager,
    create_database_manager_from_config,
    ConfigManager
)
from crawler import JobSpider, create_job_spider_from_config


def example_1_basic_sqlite_usage():
    """示例1: 基础SQLite使用"""
    print("=== 示例1: 基础SQLite使用 ===")
    
    # 创建SQLite数据库管理器
    db_manager = create_sqlite_database_manager('./data/example1.db')
    
    try:
        # 插入测试数据
        job_data = {
            'job_id': 'example_001',
            'title': 'Python后端开发工程师',
            'company_name': '示例科技有限公司',
            'salary_min': 15000,
            'salary_max': 25000,
            'salary_text': '15k-25k',
            'location_city': '上海',
            'location_district': '浦东新区',
            'job_description': '负责后端系统开发，熟悉Python、Django等技术栈',
            'experience_required': '3-5年',
            'education_required': '本科',
            'industry': 'IT互联网',
            'publish_time': datetime.now(),
            'job_url': 'https://jobs.51job.com/example/001.html'
        }
        
        # 插入数据
        success = db_manager.insert_job_listing(job_data)
        print(f"数据插入结果: {success}")
        
        # 查询数据
        jobs = db_manager.search_job_listings(
            keywords=['Python'],
            cities=['上海'],
            limit=10
        )
        print(f"查询到 {len(jobs)} 条职位")
        
        # 获取统计信息
        stats = db_manager.get_statistics()
        print(f"数据库统计: 总职位数 {stats['total_jobs']}")
        
    finally:
        db_manager.close()


def example_2_config_based_sqlite():
    """示例2: 基于配置的SQLite使用"""
    print("\n=== 示例2: 基于配置的SQLite使用 ===")
    
    # 创建配置管理器
    config_manager = ConfigManager()
    
    # 检查SQLite是否已启用
    db_config = config_manager.get_database_config()
    sqlite_enabled = db_config.get('sqlite', {}).get('enabled', False)
    
    print(f"SQLite配置状态: {'已启用' if sqlite_enabled else '未启用'}")
    
    if sqlite_enabled:
        # 使用配置创建数据库管理器
        db_manager = create_database_manager_from_config(config_manager)
        
        try:
            # 测试连接
            if db_manager.test_connection():
                print("✓ 数据库连接成功")
                
                # 批量插入示例数据
                batch_data = [
                    {
                        'job_id': f'batch_{i:03d}',
                        'title': f'职位标题 {i}',
                        'company_name': f'公司 {i}',
                        'salary_text': f'{10+i}k-{15+i}k',
                        'location_city': '北京' if i % 2 == 0 else '上海'
                    }
                    for i in range(1, 6)
                ]
                
                inserted, skipped = db_manager.batch_insert_job_listings(batch_data)
                print(f"批量插入结果: 成功 {inserted}, 跳过 {skipped}")
                
            else:
                print("✗ 数据库连接失败")
                
        finally:
            db_manager.close()
    else:
        print("请先运行 'python setup_sqlite.py' 启用SQLite配置")


def example_3_crawler_with_sqlite():
    """示例3: 爬虫与SQLite集成"""
    print("\n=== 示例3: 爬虫与SQLite集成 ===")
    
    try:
        # 创建配置管理器
        config_manager = ConfigManager()
        
        # 创建爬虫实例
        spider = create_job_spider_from_config(config_manager)
        
        print("✓ 爬虫创建成功")
        
        # 执行小规模测试爬取
        with spider:
            print("开始测试爬取...")
            
            # 爬取少量数据进行测试
            results = spider.crawl_jobs(
                keywords=['Python'],
                cities=['上海'],
                max_pages=1,  # 只爬取1页
                delay_range=(1, 2)
            )
            
            print(f"✓ 爬取完成: {len(results)} 条数据")
            
            # 显示爬虫统计
            stats = spider.get_stats()
            print(f"爬虫统计: 成功 {stats.successful_requests}, 失败 {stats.failed_requests}")
            
            # 显示数据库统计
            db_stats = spider.db_manager.get_statistics()
            print(f"数据库统计: 总职位 {db_stats['total_jobs']}")
            
    except Exception as e:
        print(f"✗ 爬虫测试失败: {e}")


def example_4_data_export_import():
    """示例4: 数据导出导入"""
    print("\n=== 示例4: 数据导出导入 ===")
    
    db_manager = create_sqlite_database_manager('./data/example4.db')
    
    try:
        # 导出数据到DataFrame
        df = db_manager.export_to_dataframe(
            query="SELECT * FROM job_listings LIMIT 10"
        )
        print(f"导出数据: {len(df)} 行")
        
        if not df.empty:
            # 显示数据概览
            print("\n数据概览:")
            print(df[['job_id', 'title', 'company_name', 'salary_text']].head())
            
            # 保存到CSV文件
            csv_file = './data/exported_jobs.csv'
            df.to_csv(csv_file, index=False, encoding='utf-8')
            print(f"✓ 数据已导出到: {csv_file}")
        
    except Exception as e:
        print(f"✗ 数据导出失败: {e}")
    finally:
        db_manager.close()


def example_5_database_maintenance():
    """示例5: 数据库维护"""
    print("\n=== 示例5: 数据库维护 ===")
    
    db_manager = create_sqlite_database_manager('./data/example5.db')
    
    try:
        # 获取数据库统计
        stats = db_manager.get_statistics()
        print(f"当前数据库状态:")
        print(f"  总职位数: {stats['total_jobs']}")
        print(f"  查询成功率: {stats['database_stats'].success_rate:.2%}")
        
        # 清理旧数据（保留最近30天）
        deleted_count = db_manager.cleanup_old_data(days=30)
        print(f"✓ 清理旧数据: 删除 {deleted_count} 条记录")
        
        # 重置统计信息
        db_manager.reset_stats()
        print("✓ 统计信息已重置")
        
    except Exception as e:
        print(f"✗ 数据库维护失败: {e}")
    finally:
        db_manager.close()


def main():
    """主函数"""
    print("SQLite使用示例")
    print("=" * 50)
    
    # 确保数据目录存在
    os.makedirs('./data', exist_ok=True)
    
    # 运行所有示例
    example_1_basic_sqlite_usage()
    example_2_config_based_sqlite()
    example_3_crawler_with_sqlite()
    example_4_data_export_import()
    example_5_database_maintenance()
    
    print("\n=== 所有示例执行完成 ===")
    print("\n💡 提示:")
    print("  - SQLite数据库文件保存在 ./data/ 目录下")
    print("  - 可以使用SQLite浏览器工具查看数据库内容")
    print("  - 生产环境建议使用MySQL数据库")


if __name__ == '__main__':
    main()