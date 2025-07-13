#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
爬虫调度器
用于管理定时任务和爬虫执行
"""

import time
import logging
import threading
import signal
import sys
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, Future
from enum import Enum
import schedule
import random

from utils import ConfigManager
from crawler.job_spider import JobSpider, create_job_spider
from crawler.data_processor import DataProcessor

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """任务状态枚举"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class CrawlTask:
    """爬虫任务"""
    task_id: str
    keywords: List[str]
    cities: List[str]
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    results: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def duration(self) -> Optional[timedelta]:
        """任务执行时长"""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None
    
    @property
    def is_running(self) -> bool:
        """是否正在运行"""
        return self.status == TaskStatus.RUNNING
    
    @property
    def is_completed(self) -> bool:
        """是否已完成"""
        return self.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]


@dataclass
class SchedulerStats:
    """调度器统计信息"""
    total_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    cancelled_tasks: int = 0
    total_jobs_crawled: int = 0
    total_jobs_saved: int = 0
    uptime: timedelta = field(default_factory=lambda: timedelta())
    
    @property
    def success_rate(self) -> float:
        """成功率"""
        if self.total_tasks == 0:
            return 0.0
        return self.completed_tasks / self.total_tasks
    
    @property
    def running_tasks(self) -> int:
        """正在运行的任务数"""
        return self.total_tasks - self.completed_tasks - self.failed_tasks - self.cancelled_tasks


class CrawlerScheduler:
    """爬虫调度器"""
    
    def __init__(self, config_manager: Optional[ConfigManager] = None,
                 max_workers: int = 3, enable_schedule: bool = True):
        """
        初始化调度器
        
        Args:
            config_manager: 配置管理器
            max_workers: 最大工作线程数
            enable_schedule: 是否启用定时调度
        """
        self.config_manager = config_manager or ConfigManager()
        self.max_workers = max_workers
        self.enable_schedule = enable_schedule
        
        # 线程池
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # 任务管理
        self.tasks: Dict[str, CrawlTask] = {}
        self.running_futures: Dict[str, Future] = {}
        
        # 统计信息
        self.stats = SchedulerStats()
        self.start_time = datetime.now()
        
        # 控制标志
        self._running = False
        self._shutdown_event = threading.Event()
        
        # 爬虫实例
        self.spider = None
        self.data_processor = None
        
        # 调度配置
        self._load_schedule_config()
        
        # 信号处理
        self._setup_signal_handlers()
        
        logger.info(f"爬虫调度器初始化完成: max_workers={max_workers}, enable_schedule={enable_schedule}")
    
    def _load_schedule_config(self):
        """加载调度配置"""
        try:
            schedule_config = self.config_manager.get_config('schedule', {})
            
            # 默认调度配置
            self.schedule_config = {
                'enabled': schedule_config.get('enabled', True),
                'cron_expressions': schedule_config.get('cron_expressions', [
                    '0 18-21 * * *',  # 每天18-21点
                    '0 21-22 * * *'   # 每天21-22点
                ]),
                'random_delay': schedule_config.get('random_delay', True),
                'max_delay_minutes': schedule_config.get('max_delay_minutes', 30),
                'timezone': schedule_config.get('timezone', 'Asia/Shanghai')
            }
            
            logger.debug(f"调度配置加载完成: {self.schedule_config}")
            
        except Exception as e:
            logger.warning(f"加载调度配置失败，使用默认配置: {e}")
            self.schedule_config = {
                'enabled': True,
                'cron_expressions': ['0 18-21 * * *', '0 21-22 * * *'],
                'random_delay': True,
                'max_delay_minutes': 30,
                'timezone': 'Asia/Shanghai'
            }
    
    def _setup_signal_handlers(self):
        """设置信号处理器"""
        def signal_handler(signum, frame):
            logger.info(f"接收到信号 {signum}，开始优雅关闭...")
            self.shutdown()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def _initialize_components(self):
        """初始化组件"""
        if not self.spider:
            self.spider = JobSpider(self.config_manager)
            logger.info("爬虫实例初始化完成")
        
        if not self.data_processor:
            self.data_processor = DataProcessor(
                enable_enrichment=True,
                enable_deduplication=True
            )
            logger.info("数据处理器初始化完成")
    
    def create_task(self, keywords: List[str], cities: List[str], 
                   task_id: Optional[str] = None) -> str:
        """
        创建爬虫任务
        
        Args:
            keywords: 搜索关键词列表
            cities: 城市列表
            task_id: 任务ID，如果不提供则自动生成
            
        Returns:
            任务ID
        """
        if not task_id:
            task_id = f"task_{int(time.time())}_{random.randint(1000, 9999)}"
        
        task = CrawlTask(
            task_id=task_id,
            keywords=keywords,
            cities=cities
        )
        
        self.tasks[task_id] = task
        self.stats.total_tasks += 1
        
        logger.info(f"创建爬虫任务: {task_id}, 关键词: {keywords}, 城市: {cities}")
        return task_id
    
    def submit_task(self, task_id: str) -> bool:
        """
        提交任务执行
        
        Args:
            task_id: 任务ID
            
        Returns:
            是否提交成功
        """
        if task_id not in self.tasks:
            logger.error(f"任务不存在: {task_id}")
            return False
        
        task = self.tasks[task_id]
        if task.is_running:
            logger.warning(f"任务已在运行: {task_id}")
            return False
        
        if task.is_completed:
            logger.warning(f"任务已完成: {task_id}")
            return False
        
        # 初始化组件
        self._initialize_components()
        
        # 提交到线程池
        future = self.executor.submit(self._execute_task, task_id)
        self.running_futures[task_id] = future
        
        # 更新任务状态
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now()
        
        logger.info(f"任务已提交执行: {task_id}")
        return True
    
    def _execute_task(self, task_id: str):
        """
        执行爬虫任务
        
        Args:
            task_id: 任务ID
        """
        task = self.tasks[task_id]
        
        try:
            logger.info(f"开始执行任务: {task_id}")
            
            total_jobs_crawled = 0
            total_jobs_saved = 0
            
            # 遍历关键词和城市
            for keyword in task.keywords:
                for city in task.cities:
                    if self._shutdown_event.is_set():
                        logger.info(f"任务被中断: {task_id}")
                        task.status = TaskStatus.CANCELLED
                        return
                    
                    logger.info(f"爬取: 关键词={keyword}, 城市={city}")
                    
                    # 执行爬虫
                    with self.spider:
                        crawl_results = list(self.spider.crawl_jobs(
                            keyword=keyword,
                            city_name=city,
                            max_pages=self.config_manager.get_config('search.max_pages', 10)
                        ))
                    
                    if crawl_results:
                        # 数据处理
                        processed_jobs = self.data_processor.process_batch(crawl_results)
                        
                        # 保存到数据库
                        if processed_jobs:
                            saved_count, skipped_count = self.spider.database_manager.batch_insert_job_listings(
                                processed_jobs
                            )
                            total_jobs_saved += saved_count
                        
                        total_jobs_crawled += len(crawl_results)
                        
                        logger.info(f"完成爬取: 关键词={keyword}, 城市={city}, "
                                  f"爬取={len(crawl_results)}, 保存={len(processed_jobs)}")
                    
                    # 随机延迟
                    if not self._shutdown_event.is_set():
                        delay = random.uniform(1, 3)
                        time.sleep(delay)
            
            # 任务完成
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now()
            task.results = {
                'jobs_crawled': total_jobs_crawled,
                'jobs_saved': total_jobs_saved,
                'spider_stats': self.spider.get_stats(),
                'processor_stats': self.data_processor.get_stats()
            }
            
            # 更新统计
            self.stats.completed_tasks += 1
            self.stats.total_jobs_crawled += total_jobs_crawled
            self.stats.total_jobs_saved += total_jobs_saved
            
            logger.info(f"任务执行完成: {task_id}, 爬取={total_jobs_crawled}, 保存={total_jobs_saved}")
            
        except Exception as e:
            logger.error(f"任务执行失败: {task_id}, 错误: {e}")
            task.status = TaskStatus.FAILED
            task.completed_at = datetime.now()
            task.error_message = str(e)
            self.stats.failed_tasks += 1
        
        finally:
            # 清理
            if task_id in self.running_futures:
                del self.running_futures[task_id]
    
    def cancel_task(self, task_id: str) -> bool:
        """
        取消任务
        
        Args:
            task_id: 任务ID
            
        Returns:
            是否取消成功
        """
        if task_id not in self.tasks:
            logger.error(f"任务不存在: {task_id}")
            return False
        
        task = self.tasks[task_id]
        
        if task.is_completed:
            logger.warning(f"任务已完成，无法取消: {task_id}")
            return False
        
        if task_id in self.running_futures:
            future = self.running_futures[task_id]
            if future.cancel():
                task.status = TaskStatus.CANCELLED
                task.completed_at = datetime.now()
                self.stats.cancelled_tasks += 1
                logger.info(f"任务已取消: {task_id}")
                return True
            else:
                logger.warning(f"任务正在运行，无法取消: {task_id}")
                return False
        else:
            task.status = TaskStatus.CANCELLED
            task.completed_at = datetime.now()
            self.stats.cancelled_tasks += 1
            logger.info(f"任务已取消: {task_id}")
            return True
    
    def get_task(self, task_id: str) -> Optional[CrawlTask]:
        """
        获取任务信息
        
        Args:
            task_id: 任务ID
            
        Returns:
            任务对象
        """
        return self.tasks.get(task_id)
    
    def list_tasks(self, status: Optional[TaskStatus] = None) -> List[CrawlTask]:
        """
        列出任务
        
        Args:
            status: 过滤状态
            
        Returns:
            任务列表
        """
        tasks = list(self.tasks.values())
        
        if status:
            tasks = [task for task in tasks if task.status == status]
        
        # 按创建时间排序
        tasks.sort(key=lambda x: x.created_at, reverse=True)
        
        return tasks
    
    def setup_schedule(self):
        """
        设置定时调度
        """
        if not self.enable_schedule or not self.schedule_config.get('enabled', True):
            logger.info("定时调度已禁用")
            return
        
        # 清除现有调度
        schedule.clear()
        
        # 获取搜索配置
        search_config = self.config_manager.get_search_config()
        keywords = search_config.keywords
        cities = search_config.cities
        
        if not keywords or not cities:
            logger.warning("搜索关键词或城市为空，跳过定时调度设置")
            return
        
        # 设置定时任务
        def scheduled_crawl():
            """定时爬虫任务"""
            if self._shutdown_event.is_set():
                return
            
            # 添加随机延迟
            if self.schedule_config.get('random_delay', True):
                max_delay = self.schedule_config.get('max_delay_minutes', 30)
                delay_minutes = random.randint(0, max_delay)
                logger.info(f"定时任务随机延迟 {delay_minutes} 分钟")
                time.sleep(delay_minutes * 60)
            
            # 创建并提交任务
            task_id = self.create_task(keywords, cities)
            self.submit_task(task_id)
        
        # 解析cron表达式并设置调度
        cron_expressions = self.schedule_config.get('cron_expressions', [])
        for cron_expr in cron_expressions:
            try:
                # 简化的cron解析（仅支持小时范围）
                if '18-21' in cron_expr:
                    for hour in range(18, 22):
                        schedule.every().day.at(f"{hour:02d}:00").do(scheduled_crawl)
                elif '21-22' in cron_expr:
                    for hour in range(21, 23):
                        schedule.every().day.at(f"{hour:02d}:00").do(scheduled_crawl)
                else:
                    logger.warning(f"不支持的cron表达式: {cron_expr}")
            except Exception as e:
                logger.error(f"解析cron表达式失败: {cron_expr}, 错误: {e}")
        
        logger.info(f"定时调度设置完成，共 {len(schedule.jobs)} 个任务")
    
    def start(self):
        """
        启动调度器
        """
        if self._running:
            logger.warning("调度器已在运行")
            return
        
        self._running = True
        self._shutdown_event.clear()
        
        logger.info("启动爬虫调度器")
        
        # 设置定时调度
        self.setup_schedule()
        
        # 启动调度循环
        def schedule_loop():
            """调度循环"""
            while self._running and not self._shutdown_event.is_set():
                try:
                    schedule.run_pending()
                    time.sleep(1)
                except Exception as e:
                    logger.error(f"调度循环异常: {e}")
                    time.sleep(5)
        
        if self.enable_schedule:
            schedule_thread = threading.Thread(target=schedule_loop, daemon=True)
            schedule_thread.start()
            logger.info("定时调度线程已启动")
        
        logger.info("爬虫调度器启动完成")
    
    def shutdown(self, timeout: int = 30):
        """
        关闭调度器
        
        Args:
            timeout: 超时时间（秒）
        """
        if not self._running:
            logger.warning("调度器未运行")
            return
        
        logger.info("开始关闭爬虫调度器...")
        
        # 设置关闭标志
        self._running = False
        self._shutdown_event.set()
        
        # 取消所有待执行的任务
        for task_id in list(self.running_futures.keys()):
            self.cancel_task(task_id)
        
        # 关闭线程池
        self.executor.shutdown(wait=True, timeout=timeout)
        
        # 清除调度
        schedule.clear()
        
        # 更新统计
        self.stats.uptime = datetime.now() - self.start_time
        
        logger.info("爬虫调度器已关闭")
    
    def get_stats(self) -> SchedulerStats:
        """
        获取调度器统计信息
        
        Returns:
            统计信息
        """
        # 更新运行时间
        self.stats.uptime = datetime.now() - self.start_time
        return self.stats
    
    def get_status(self) -> Dict[str, Any]:
        """
        获取调度器状态
        
        Returns:
            状态信息
        """
        return {
            'running': self._running,
            'stats': self.stats,
            'active_tasks': len(self.running_futures),
            'total_tasks': len(self.tasks),
            'schedule_jobs': len(schedule.jobs) if self.enable_schedule else 0,
            'uptime': datetime.now() - self.start_time
        }
    
    def run_once(self, keywords: Optional[List[str]] = None, 
                cities: Optional[List[str]] = None) -> str:
        """
        执行一次爬虫任务
        
        Args:
            keywords: 搜索关键词，如果不提供则使用配置文件
            cities: 城市列表，如果不提供则使用配置文件
            
        Returns:
            任务ID
        """
        # 使用配置文件中的默认值
        if not keywords or not cities:
            search_config = self.config_manager.get_search_config()
            keywords = keywords or search_config.keywords
            cities = cities or search_config.cities
        
        if not keywords or not cities:
            raise ValueError("关键词和城市不能为空")
        
        # 创建并提交任务
        task_id = self.create_task(keywords, cities)
        self.submit_task(task_id)
        
        logger.info(f"一次性任务已提交: {task_id}")
        return task_id
    
    def wait_for_completion(self, timeout: Optional[int] = None) -> bool:
        """
        等待所有任务完成
        
        Args:
            timeout: 超时时间（秒）
            
        Returns:
            是否所有任务都完成
        """
        start_time = time.time()
        
        while self.running_futures:
            if timeout and (time.time() - start_time) > timeout:
                logger.warning(f"等待任务完成超时: {timeout}秒")
                return False
            
            if self._shutdown_event.is_set():
                logger.info("收到关闭信号，停止等待")
                return False
            
            time.sleep(1)
        
        logger.info("所有任务已完成")
        return True
    
    def __enter__(self):
        """上下文管理器入口"""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.shutdown()
    
    def __str__(self) -> str:
        """字符串表示"""
        return f"CrawlerScheduler(running={self._running}, tasks={len(self.tasks)})"
    
    def __repr__(self) -> str:
        """对象表示"""
        return self.__str__()


def create_crawler_scheduler(config_manager: Optional[ConfigManager] = None,
                           max_workers: int = 3,
                           enable_schedule: bool = True) -> CrawlerScheduler:
    """
    创建爬虫调度器实例
    
    Args:
        config_manager: 配置管理器
        max_workers: 最大工作线程数
        enable_schedule: 是否启用定时调度
        
    Returns:
        爬虫调度器实例
    """
    if not config_manager:
        config_manager = ConfigManager()
    
    return CrawlerScheduler(
        config_manager=config_manager,
        max_workers=max_workers,
        enable_schedule=enable_schedule
    )


if __name__ == "__main__":
    # 示例用法
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description='51job爬虫调度器')
    parser.add_argument('--keywords', nargs='+', help='搜索关键词')
    parser.add_argument('--cities', nargs='+', help='城市列表')
    parser.add_argument('--once', action='store_true', help='执行一次')
    parser.add_argument('--schedule', action='store_true', help='启动定时调度')
    parser.add_argument('--max-workers', type=int, default=3, help='最大工作线程数')
    
    args = parser.parse_args()
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        scheduler = create_crawler_scheduler(
            max_workers=args.max_workers,
            enable_schedule=args.schedule
        )
        
        if args.once:
            # 执行一次
            task_id = scheduler.run_once(args.keywords, args.cities)
            print(f"任务已提交: {task_id}")
            
            # 等待完成
            scheduler.wait_for_completion()
            
            # 显示结果
            task = scheduler.get_task(task_id)
            if task:
                print(f"任务状态: {task.status.value}")
                if task.results:
                    print(f"爬取结果: {task.results}")
        
        elif args.schedule:
            # 启动定时调度
            with scheduler:
                print("调度器已启动，按Ctrl+C停止")
                try:
                    while True:
                        time.sleep(1)
                except KeyboardInterrupt:
                    print("\n收到停止信号")
        
        else:
            print("请指定 --once 或 --schedule 参数")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"程序异常: {e}")
        sys.exit(1)