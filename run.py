#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
51job爬虫快速启动脚本
提供简化的命令行接口
"""

import os
import sys
import time
import logging
from pathlib import Path
from typing import List, Optional

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    from utils import ConfigManager
    from crawler import create_crawler_scheduler, create_job_spider
except ImportError as e:
    print(f"导入错误: {e}")
    print("请确保已安装所有依赖包: pip install -r requirements.txt")
    sys.exit(1)

# 配置基础日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def quick_crawl(keywords: List[str], cities: List[str], pages: int = 5):
    """
    快速爬取
    
    Args:
        keywords: 关键词列表
        cities: 城市列表
        pages: 爬取页数
    """
    print(f"🚀 开始快速爬取: 关键词={keywords}, 城市={cities}, 页数={pages}")
    
    try:
        # 创建配置管理器
        config_manager = ConfigManager()
        
        # 创建调度器
        scheduler = create_crawler_scheduler(
            config_manager=config_manager,
            max_workers=1,
            enable_schedule=False
        )
        
        # 执行任务
        task_id = scheduler.run_once(keywords, cities)
        print(f"📋 任务ID: {task_id}")
        
        # 等待完成
        print("⏳ 等待任务完成...")
        success = scheduler.wait_for_completion(timeout=1800)  # 30分钟超时
        
        # 获取结果
        task = scheduler.get_task(task_id)
        if task and task.results:
            jobs_crawled = task.results.get('jobs_crawled', 0)
            jobs_saved = task.results.get('jobs_saved', 0)
            
            print(f"✅ 任务完成!")
            print(f"   爬取职位: {jobs_crawled}")
            print(f"   保存职位: {jobs_saved}")
            print(f"   执行时间: {task.duration}")
        else:
            print("❌ 任务失败或无结果")
        
        scheduler.shutdown()
        
    except Exception as e:
        print(f"❌ 爬取失败: {e}")
        logger.error(f"快速爬取异常: {e}")


def start_scheduler():
    """
    启动定时调度器
    """
    print("🕐 启动定时调度器...")
    
    try:
        # 创建配置管理器
        config_manager = ConfigManager()
        
        # 创建调度器
        scheduler = create_crawler_scheduler(
            config_manager=config_manager,
            max_workers=3,
            enable_schedule=True
        )
        
        # 启动调度器
        with scheduler:
            print("✅ 调度器已启动")
            print("📅 定时任务已设置")
            print("🔄 按Ctrl+C停止")
            print()
            
            try:
                while True:
                    # 显示状态
                    status = scheduler.get_status()
                    print(f"📊 状态: 运行中, 活跃任务={status['active_tasks']}, 总任务={status['total_tasks']}")
                    time.sleep(300)  # 每5分钟显示一次状态
                    
            except KeyboardInterrupt:
                print("\n🛑 收到停止信号")
        
        print("✅ 调度器已停止")
        
    except Exception as e:
        print(f"❌ 调度器启动失败: {e}")
        logger.error(f"调度器异常: {e}")


def test_system():
    """
    测试系统功能
    """
    print("🧪 开始系统测试...")
    
    try:
        # 测试配置加载
        print("📋 测试配置加载...")
        config_manager = ConfigManager()
        print("✅ 配置加载成功")
        
        # 测试数据库连接
        print("🗄️ 测试数据库连接...")
        from utils import DatabaseManager
        db_manager = DatabaseManager(config_manager)
        
        if db_manager.test_connection():
            print("✅ 数据库连接成功")
        else:
            print("❌ 数据库连接失败")
            return False
        
        # 测试爬虫
        print("🕷️ 测试爬虫功能...")
        spider = create_job_spider("./config")
        
        with spider:
            # 简单测试
            results_generator = spider.crawl_jobs(
                keyword="Python",
                city_name="上海",
                max_pages=1
            )
            
            # 将生成器转换为列表
            results = list(results_generator)
            
            if results:
                print(f"✅ 爬虫测试成功，获取到 {len(results)} 条数据")
            else:
                print("⚠️ 爬虫测试无数据")
        
        print("✅ 系统测试完成")
        return True
        
    except Exception as e:
        print(f"❌ 系统测试失败: {e}")
        logger.error(f"系统测试异常: {e}")
        return False


def show_status():
    """
    显示系统状态
    """
    print("📊 系统状态")
    print("=" * 50)
    
    try:
        # 配置信息
        config_manager = ConfigManager()
        
        print(f"📁 配置目录: {config_manager.config_dir}")
        print(f"🐛 调试模式: {config_manager.is_debug_mode()}")
        
        # 数据库信息
        db_config = config_manager.get_database_config()
        print(f"🗄️ 数据库类型: {db_config.db_type}")
        print(f"🌐 数据库地址: {db_config.host}:{db_config.port}")
        
        # 搜索配置
        search_config = config_manager.get_search_config()
        print(f"🔍 搜索关键词: {search_config.keywords}")
        print(f"🏙️ 搜索城市: {search_config.cities}")
        print(f"📄 最大页数: {search_config.max_pages}")
        
        # 检查日志目录
        log_dir = Path('logs')
        if log_dir.exists():
            log_files = list(log_dir.glob('*.log'))
            print(f"📝 日志文件: {len(log_files)} 个")
        
        # 检查数据目录
        data_dir = Path('data')
        if data_dir.exists():
            data_files = list(data_dir.glob('*'))
            print(f"💾 数据文件: {len(data_files)} 个")
        
        print("=" * 50)
        
    except Exception as e:
        print(f"❌ 获取状态失败: {e}")


def show_help():
    """
    显示帮助信息
    """
    help_text = """
🕷️ 51job爬虫系统 - 快速启动脚本

📋 使用方法:
  python run.py [命令] [参数]

🚀 可用命令:
  crawl     快速爬取职位数据
  schedule  启动定时调度器
  test      测试系统功能
  status    显示系统状态
  help      显示此帮助信息

💡 示例:
  # 快速爬取Python相关职位
  python run.py crawl Python 上海
  
  # 爬取多个关键词和城市
  python run.py crawl "Python,Java" "上海,北京"
  
  # 启动定时调度器
  python run.py schedule
  
  # 测试系统
  python run.py test
  
  # 查看状态
  python run.py status

📚 更多功能请使用:
  python main_new.py --help

🔧 配置文件:
  config/settings.yaml    - 基础配置
  config/database.yaml    - 数据库配置
  config/logging.yaml     - 日志配置

📖 文档:
  README.md              - 项目说明
  requirements.txt       - 依赖包列表
    """
    print(help_text)


def main():
    """
    主函数
    """
    if len(sys.argv) < 2:
        show_help()
        return
    
    command = sys.argv[1].lower()
    
    if command == 'crawl':
        if len(sys.argv) < 4:
            print("❌ 用法: python run.py crawl <关键词> <城市>")
            print("💡 示例: python run.py crawl Python 上海")
            return
        
        keywords_str = sys.argv[2]
        cities_str = sys.argv[3]
        
        # 解析关键词和城市
        keywords = [k.strip() for k in keywords_str.split(',')]
        cities = [c.strip() for c in cities_str.split(',')]
        
        # 获取页数参数
        pages = 5
        if len(sys.argv) > 4:
            try:
                pages = int(sys.argv[4])
            except ValueError:
                print("⚠️ 页数参数无效，使用默认值5")
        
        quick_crawl(keywords, cities, pages)
    
    elif command == 'schedule':
        start_scheduler()
    
    elif command == 'test':
        test_system()
    
    elif command == 'status':
        show_status()
    
    elif command in ['help', '-h', '--help']:
        show_help()
    
    else:
        print(f"❌ 未知命令: {command}")
        print("💡 使用 'python run.py help' 查看帮助")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 程序被用户中断")
    except Exception as e:
        print(f"❌ 程序异常: {e}")
        logger.error(f"程序异常: {e}")
        sys.exit(1)