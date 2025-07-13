#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite快速设置脚本
帮助用户快速配置SQLite本地测试环境
"""

import os
import sys
import yaml
import shutil
from pathlib import Path

def create_directories():
    """创建必要的目录"""
    directories = [
        './data',
        './logs',
        './cache',
        './exports'
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"✓ 创建目录: {directory}")

def setup_sqlite_config():
    """设置SQLite配置"""
    config_file = './config/database.yaml'
    
    if not os.path.exists(config_file):
        print(f"✗ 配置文件不存在: {config_file}")
        return False
    
    # 读取配置文件
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # 启用SQLite配置
    if 'sqlite' not in config:
        config['sqlite'] = {}
    
    config['sqlite']['enabled'] = True
    config['sqlite']['database_path'] = './data/job_crawler.db'
    
    # 禁用其他数据库
    if 'mysql' in config:
        config['mysql']['enabled'] = False
    if 'postgresql' in config:
        config['postgresql']['enabled'] = False
    
    # 备份原配置文件
    backup_file = config_file + '.backup'
    if not os.path.exists(backup_file):
        shutil.copy2(config_file, backup_file)
        print(f"✓ 备份原配置文件: {backup_file}")
    
    # 写入新配置
    with open(config_file, 'w', encoding='utf-8') as f:
        yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
    
    print(f"✓ 更新SQLite配置: {config_file}")
    return True

def create_sqlite_test_config():
    """创建SQLite测试专用配置"""
    test_config = {
        'sqlite': {
            'enabled': True,
            'database_path': './data/job_crawler.db'
        },
        'mysql': {
            'enabled': False
        },
        'postgresql': {
            'enabled': False
        },
        'redis': {
            'enabled': False
        },
        'tables': {
            'job_listings': 'job_listings',
            'crawl_stats': 'crawl_statistics',
            'error_logs': 'error_logs',
            'dedup_cache': 'deduplication_cache'
        },
        'initialization': {
            'auto_create_tables': True,
            'auto_migrate': True
        },
        'backup': {
            'enabled': False
        }
    }
    
    test_config_file = './config/database_sqlite.yaml'
    with open(test_config_file, 'w', encoding='utf-8') as f:
        yaml.dump(test_config, f, default_flow_style=False, allow_unicode=True)
    
    print(f"✓ 创建SQLite测试配置: {test_config_file}")
    return test_config_file

def test_sqlite_setup():
    """测试SQLite设置"""
    try:
        # 添加项目路径
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        
        from utils import create_sqlite_database_manager
        
        # 创建SQLite数据库管理器
        db_manager = create_sqlite_database_manager('./data/test_setup.db')
        
        # 测试连接
        if db_manager.test_connection():
            print("✓ SQLite数据库连接测试成功")
            
            # 插入测试数据
            test_data = {
                'job_id': 'setup_test_001',
                'title': '测试职位',
                'company_name': '测试公司',
                'salary_text': '面议',
                'location_city': '测试城市'
            }
            
            if db_manager.insert_job_listing(test_data):
                print("✓ 测试数据插入成功")
            
            # 查询测试
            jobs = db_manager.search_job_listings(limit=1)
            if jobs:
                print(f"✓ 数据查询成功: 找到 {len(jobs)} 条记录")
            
            db_manager.close()
            
            # 清理测试数据库
            test_db_path = './data/test_setup.db'
            if os.path.exists(test_db_path):
                os.remove(test_db_path)
                print("✓ 清理测试数据库")
            
            return True
        else:
            print("✗ SQLite数据库连接测试失败")
            return False
            
    except Exception as e:
        print(f"✗ SQLite设置测试失败: {e}")
        return False

def show_usage_examples():
    """显示使用示例"""
    print("\n=== SQLite使用示例 ===")
    examples = [
        "# 快速测试爬虫",
        "python test_sqlite.py",
        "",
        "# 使用SQLite运行爬虫",
        "python run.py crawl Python 上海",
        "",
        "# 完整功能测试",
        "python main_new.py --once --keywords Python --cities 上海 --max-workers 2",
        "",
        "# 系统测试",
        "python test_system.py",
        "",
        "# 查看SQLite数据库文件",
        "ls -la ./data/job_crawler.db"
    ]
    
    for example in examples:
        print(f"  {example}")

def main():
    """主函数"""
    print("SQLite快速设置工具")
    print("=" * 50)
    
    print("\n🔧 开始设置SQLite本地测试环境...")
    
    # 创建目录
    print("\n1. 创建必要目录")
    create_directories()
    
    # 设置配置
    print("\n2. 配置SQLite数据库")
    if setup_sqlite_config():
        print("✓ SQLite配置完成")
    else:
        print("✗ SQLite配置失败")
        return
    
    # 创建测试配置
    print("\n3. 创建测试配置")
    test_config_file = create_sqlite_test_config()
    
    # 测试设置
    print("\n4. 测试SQLite设置")
    if test_sqlite_setup():
        print("✓ SQLite设置测试通过")
    else:
        print("✗ SQLite设置测试失败")
        return
    
    # 显示使用示例
    show_usage_examples()
    
    print("\n🎉 SQLite本地测试环境设置完成！")
    print("\n📝 重要提示:")
    print("  - SQLite数据库文件位置: ./data/job_crawler.db")
    print("  - 配置文件已更新: ./config/database.yaml")
    print("  - 测试配置文件: ./config/database_sqlite.yaml")
    print("  - 原配置已备份: ./config/database.yaml.backup")
    
    print("\n🚀 现在可以开始使用SQLite进行本地测试了！")

if __name__ == '__main__':
    main()