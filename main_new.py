#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
51job爬虫主程序
新版本主程序，整合了所有组件
"""

import os
import sys
import time
import logging
import argparse
from typing import List, Optional
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from utils import ConfigManager
from crawler import (
    JobSpider, create_job_spider,
    DataProcessor, 
    CrawlerScheduler, create_crawler_scheduler
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/main.log', encoding='utf-8')
    ]
)

logger = logging.getLogger(__name__)


def setup_logging(config_manager: ConfigManager):
    """
    设置日志配置
    
    Args:
        config_manager: 配置管理器
    """
    try:
        import logging.config
        import yaml
        
        # 加载日志配置
        logging_config_path = config_manager.config_dir / 'logging.yaml'
        if logging_config_path.exists():
            with open(logging_config_path, 'r', encoding='utf-8') as f:
                logging_config = yaml.safe_load(f)
            
            # 确保日志目录存在
            log_dir = Path('logs')
            log_dir.mkdir(exist_ok=True)
            
            logging.config.dictConfig(logging_config)
            logger.info("日志配置加载成功")
        else:
            logger.warning(f"日志配置文件不存在: {logging_config_path}")
    
    except Exception as e:
        logger.error(f"设置日志配置失败: {e}")


def run_once(keywords: List[str], cities: List[str], 
            config_manager: ConfigManager) -> bool:
    """
    执行一次爬虫任务
    
    Args:
        keywords: 搜索关键词列表
        cities: 城市列表
        config_manager: 配置管理器
        
    Returns:
        是否执行成功
    """
    try:
        logger.info(f"开始执行一次性爬虫任务: 关键词={keywords}, 城市={cities}")
        
        # 创建调度器
        scheduler = create_crawler_scheduler(
            config_manager=config_manager,
            max_workers=1,
            enable_schedule=False
        )
        
        # 执行任务
        task_id = scheduler.run_once(keywords, cities)
        
        # 等待完成
        success = scheduler.wait_for_completion(timeout=3600)  # 1小时超时
        
        # 获取结果
        task = scheduler.get_task(task_id)
        if task and task.results:
            logger.info(f"任务完成: 状态={task.status.value}, "
                       f"爬取={task.results.get('jobs_crawled', 0)}, "
                       f"保存={task.results.get('jobs_saved', 0)}")
        
        scheduler.shutdown()
        return success
        
    except Exception as e:
        logger.error(f"执行一次性任务失败: {e}")
        return False


def run_scheduler(config_manager: ConfigManager, max_workers: int = 3):
    """
    运行定时调度器
    
    Args:
        config_manager: 配置管理器
        max_workers: 最大工作线程数
    """
    try:
        logger.info("启动定时调度器")
        
        # 创建调度器
        scheduler = create_crawler_scheduler(
            config_manager=config_manager,
            max_workers=max_workers,
            enable_schedule=True
        )
        
        # 启动调度器
        with scheduler:
            logger.info("调度器已启动，按Ctrl+C停止")
            
            try:
                while True:
                    # 显示状态
                    status = scheduler.get_status()
                    logger.info(f"调度器状态: 运行={status['running']}, "
                               f"活跃任务={status['active_tasks']}, "
                               f"总任务={status['total_tasks']}")
                    
                    time.sleep(60)  # 每分钟显示一次状态
                    
            except KeyboardInterrupt:
                logger.info("收到停止信号")
        
        logger.info("调度器已停止")
        
    except Exception as e:
        logger.error(f"运行调度器失败: {e}")


def test_spider(config_manager: ConfigManager):
    """
    测试爬虫功能
    
    Args:
        config_manager: 配置管理器
    """
    try:
        logger.info("开始测试爬虫功能")
        
        # 创建爬虫实例
        spider = create_job_spider(config_manager)
        
        # 测试搜索
        with spider:
            test_keyword = "Python"
            test_city = "上海"
            
            logger.info(f"测试搜索: 关键词={test_keyword}, 城市={test_city}")
            
            results = spider.crawl_jobs(
                keyword=test_keyword,
                city=test_city,
                max_pages=1  # 只爬取1页用于测试
            )
            
            if results:
                logger.info(f"测试成功: 获取到 {len(results)} 条职位信息")
                
                # 显示第一条数据
                if results:
                    first_job = results[0]
                    logger.info(f"示例职位: {first_job.get('title', 'N/A')} - "
                               f"{first_job.get('company_name', 'N/A')}")
                
                # 测试数据处理
                processor = DataProcessor()
                processed_jobs = processor.process_batch(results)
                
                logger.info(f"数据处理完成: 输入={len(results)}, 输出={len(processed_jobs)}")
                
                # 测试数据库保存
                if processed_jobs:
                    saved_count, skipped_count = spider.database_manager.batch_insert_job_listings(
                        processed_jobs[:5]  # 只保存前5条用于测试
                    )
                    logger.info(f"数据库保存测试: 保存了 {saved_count} 条记录")
                
            else:
                logger.warning("测试失败: 未获取到任何数据")
        
        logger.info("爬虫功能测试完成")
        
    except Exception as e:
        logger.error(f"测试爬虫功能失败: {e}")


def show_config(config_manager: ConfigManager):
    """
    显示配置信息
    
    Args:
        config_manager: 配置管理器
    """
    try:
        print("\n=== 配置信息 ===")
        
        # 基础配置
        print(f"配置目录: {config_manager.config_dir}")
        print(f"调试模式: {config_manager.is_debug_mode()}")
        
        # 数据库配置
        db_config = config_manager.get_database_config()
        print(f"数据库类型: {db_config.db_type}")
        print(f"数据库主机: {db_config.host}:{db_config.port}")
        print(f"数据库名称: {db_config.database}")
        
        # 搜索配置
        search_config = config_manager.get_search_config()
        print(f"搜索关键词: {search_config.keywords}")
        print(f"搜索城市: {search_config.cities}")
        print(f"最大页数: {search_config.max_pages}")
        
        # 请求配置
        request_config = config_manager.get_request_config()
        print(f"下载延迟: {request_config.download_delay}")
        print(f"请求超时: {request_config.timeout}")
        print(f"最大重试: {request_config.max_retries}")
        
        print("\n")
        
    except Exception as e:
        logger.error(f"显示配置信息失败: {e}")


def main():
    """
    主函数
    """
    parser = argparse.ArgumentParser(
        description='51job爬虫系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  %(prog)s --once --keywords Python Java --cities 上海 北京
  %(prog)s --schedule --max-workers 5
  %(prog)s --test
  %(prog)s --config
        """
    )
    
    # 运行模式
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('--once', action='store_true', help='执行一次爬虫任务')
    mode_group.add_argument('--schedule', action='store_true', help='启动定时调度器')
    mode_group.add_argument('--test', action='store_true', help='测试爬虫功能')
    mode_group.add_argument('--config', action='store_true', help='显示配置信息')
    
    # 参数选项
    parser.add_argument('--keywords', nargs='+', help='搜索关键词列表')
    parser.add_argument('--cities', nargs='+', help='城市列表')
    parser.add_argument('--max-workers', type=int, default=3, help='最大工作线程数')
    parser.add_argument('--config-dir', help='配置文件目录')
    parser.add_argument('--debug', action='store_true', help='启用调试模式')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                       default='INFO', help='日志级别')
    
    args = parser.parse_args()
    
    try:
        # 设置日志级别
        logging.getLogger().setLevel(getattr(logging, args.log_level))
        
        # 创建配置管理器
        config_dir = args.config_dir or "./config"
        config_manager = ConfigManager(config_dir=config_dir)
        
        # 设置日志配置
        setup_logging(config_manager)
        
        logger.info(f"51job爬虫系统启动: 模式={sys.argv[1:]}")
        
        # 根据模式执行相应操作
        if args.config:
            show_config(config_manager)
        
        elif args.test:
            test_spider(config_manager)
        
        elif args.once:
            # 获取关键词和城市
            keywords = args.keywords
            cities = args.cities
            
            if not keywords or not cities:
                # 使用配置文件中的默认值
                search_config = config_manager.get_search_config()
                keywords = keywords or search_config.keywords
                cities = cities or search_config.cities
            
            if not keywords or not cities:
                logger.error("请提供搜索关键词和城市，或在配置文件中设置默认值")
                sys.exit(1)
            
            success = run_once(keywords, cities, config_manager)
            sys.exit(0 if success else 1)
        
        elif args.schedule:
            run_scheduler(config_manager, args.max_workers)
        
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"程序异常: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()