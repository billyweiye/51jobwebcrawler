#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
51job爬虫系统安装脚本
"""

import os
import sys
import subprocess
from pathlib import Path


def check_python_version():
    """检查Python版本"""
    if sys.version_info < (3, 8):
        print("错误: 需要Python 3.8或更高版本")
        print(f"当前版本: {sys.version}")
        sys.exit(1)
    else:
        print(f"✓ Python版本检查通过: {sys.version.split()[0]}")


def create_directories():
    """创建必要的目录"""
    directories = [
        'logs',
        'data',
        'cache',
        'exports'
    ]
    
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"✓ 创建目录: {directory}")


def install_dependencies():
    """安装依赖包"""
    print("开始安装依赖包...")
    
    try:
        # 升级pip
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'pip'])
        print("✓ pip已升级")
        
        # 安装核心依赖
        core_packages = [
            'requests>=2.31.0',
            'beautifulsoup4>=4.12.0',
            'lxml>=4.9.0',
            'pandas>=2.0.0',
            'numpy>=1.24.0',
            'SQLAlchemy>=2.0.0',
            'PyMySQL>=1.1.0',
            'PyYAML>=6.0',
            'schedule>=1.2.0',
            'fake-useragent>=1.4.0'
        ]
        
        for package in core_packages:
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])
            print(f"✓ 安装: {package}")
        
        print("✓ 核心依赖安装完成")
        
    except subprocess.CalledProcessError as e:
        print(f"错误: 安装依赖失败 - {e}")
        sys.exit(1)


def create_config_template():
    """创建配置文件模板"""
    config_dir = Path('config')
    config_dir.mkdir(exist_ok=True)
    
    # 检查配置文件是否已存在
    config_files = {
        'settings.yaml': 'settings.yaml',
        'database.yaml': 'database.yaml', 
        'logging.yaml': 'logging.yaml'
    }
    
    for config_file, template_file in config_files.items():
        config_path = config_dir / config_file
        if config_path.exists():
            print(f"✓ 配置文件已存在: {config_file}")
        else:
            print(f"⚠ 配置文件不存在: {config_file}")
            print(f"  请参考 config/{template_file} 创建配置文件")


def create_sample_config():
    """创建示例配置文件"""
    config_dir = Path('config')
    
    # 创建示例数据库配置
    sample_db_config = config_dir / 'database.example.yaml'
    if not sample_db_config.exists():
        with open(sample_db_config, 'w', encoding='utf-8') as f:
            f.write("""
# 数据库配置示例
# 复制此文件为 database.yaml 并修改相应配置

mysql:
  primary:
    host: localhost
    port: 3306
    username: your_username
    password: your_password
    database: job_crawler
    charset: utf8mb4
    
sqlite:
  database: data/jobs.db
  
redis:
  host: localhost
  port: 6379
  db: 0
""")
        print("✓ 创建示例数据库配置: config/database.example.yaml")
    
    # 创建示例环境变量文件
    env_example = Path('.env.example')
    if not env_example.exists():
        with open(env_example, 'w', encoding='utf-8') as f:
            f.write("""
# 环境变量示例
# 复制此文件为 .env 并修改相应配置

# 数据库配置
DB_HOST=localhost
DB_PORT=3306
DB_USERNAME=your_username
DB_PASSWORD=your_password
DB_DATABASE=job_crawler

# 调试模式
DEBUG=False

# 日志级别
LOG_LEVEL=INFO
""")
        print("✓ 创建示例环境变量文件: .env.example")


def check_database_connection():
    """检查数据库连接"""
    try:
        from utils import ConfigManager
        
        config_manager = ConfigManager()
        db_config = config_manager.get_database_config()
        
        if db_config.db_type == 'mysql':
            import pymysql
            connection = pymysql.connect(
                host=db_config.host,
                port=db_config.port,
                user=db_config.username,
                password=db_config.password,
                database=db_config.database,
                charset=db_config.charset
            )
            connection.close()
            print("✓ MySQL数据库连接测试成功")
        
        elif db_config.db_type == 'sqlite':
            import sqlite3
            conn = sqlite3.connect(db_config.database)
            conn.close()
            print("✓ SQLite数据库连接测试成功")
        
    except ImportError:
        print("⚠ 无法导入配置管理器，跳过数据库连接测试")
    except Exception as e:
        print(f"⚠ 数据库连接测试失败: {e}")
        print("  请检查数据库配置是否正确")


def run_tests():
    """运行基础测试"""
    try:
        print("运行基础测试...")
        
        # 测试导入
        from utils import ConfigManager
        from crawler import JobSpider
        
        print("✓ 模块导入测试通过")
        
        # 测试配置加载
        config_manager = ConfigManager()
        print("✓ 配置加载测试通过")
        
    except Exception as e:
        print(f"⚠ 测试失败: {e}")
        print("  请检查代码和配置是否正确")


def main():
    """主函数"""
    print("=== 51job爬虫系统安装程序 ===")
    print()
    
    # 检查Python版本
    check_python_version()
    
    # 创建目录
    print("\n创建必要目录...")
    create_directories()
    
    # 安装依赖
    print("\n安装依赖包...")
    install_dependencies()
    
    # 创建配置文件
    print("\n检查配置文件...")
    create_config_template()
    create_sample_config()
    
    # 检查数据库连接
    print("\n检查数据库连接...")
    check_database_connection()
    
    # 运行测试
    print("\n运行基础测试...")
    run_tests()
    
    print("\n=== 安装完成 ===")
    print()
    print("下一步:")
    print("1. 复制 config/database.example.yaml 为 config/database.yaml 并配置数据库")
    print("2. 复制 .env.example 为 .env 并配置环境变量")
    print("3. 运行测试: python main_new.py --test")
    print("4. 执行爬虫: python main_new.py --once --keywords Python --cities 上海")
    print()


if __name__ == "__main__":
    main()