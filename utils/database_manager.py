#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库管理器
用于处理数据存储、连接池管理和数据操作
"""

import time
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any, Union, Tuple
from dataclasses import dataclass
from contextlib import contextmanager
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Text, DateTime, Float, JSON
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.pool import QueuePool, StaticPool
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from .retry_handler import retry_on_database_error
from .config_manager import ConfigManager

logger = logging.getLogger(__name__)

Base = declarative_base()


@dataclass
class DatabaseStats:
    """数据库统计信息"""
    total_connections: int = 0
    active_connections: int = 0
    total_queries: int = 0
    successful_queries: int = 0
    failed_queries: int = 0
    total_inserts: int = 0
    total_updates: int = 0
    total_deletes: int = 0
    total_query_time: float = 0.0
    
    @property
    def success_rate(self) -> float:
        """查询成功率"""
        if self.total_queries == 0:
            return 0.0
        return self.successful_queries / self.total_queries
    
    @property
    def average_query_time(self) -> float:
        """平均查询时间"""
        if self.successful_queries == 0:
            return 0.0
        return self.total_query_time / self.successful_queries


class JobListing(Base):
    """职位信息表模型"""
    __tablename__ = 'job_listings'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(50), unique=True, nullable=False, comment='职位ID')
    title = Column(String(200), nullable=False, comment='职位标题')
    company_name = Column(String(200), nullable=False, comment='公司名称')
    company_size = Column(String(50), comment='公司规模')
    company_type = Column(String(100), comment='公司类型')
    salary_min = Column(Integer, comment='最低薪资')
    salary_max = Column(Integer, comment='最高薪资')
    salary_text = Column(String(100), comment='薪资文本')
    location_city = Column(String(50), comment='城市')
    location_district = Column(String(50), comment='区域')
    location_address = Column(Text, comment='详细地址')
    job_description = Column(Text, comment='职位描述')
    job_requirements = Column(Text, comment='职位要求')
    experience_required = Column(String(50), comment='经验要求')
    education_required = Column(String(50), comment='学历要求')
    industry = Column(String(100), comment='行业')
    job_tags = Column(Text, comment='职位标签')
    publish_time = Column(DateTime, comment='发布时间')
    update_time = Column(DateTime, comment='更新时间')
    confirm_date = Column(DateTime, comment='确认日期')
    job_url = Column(String(500), comment='职位链接')
    company_url = Column(String(500), comment='公司链接')
    welfare = Column(Text, comment='福利待遇')
    work_type = Column(String(50), comment='工作类型')
    coordinates_lat = Column(Float, comment='纬度')
    coordinates_lng = Column(Float, comment='经度')
    raw_data = Column(JSON, comment='原始数据')
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'id': self.id,
            'job_id': self.job_id,
            'title': self.title,
            'company_name': self.company_name,
            'company_size': self.company_size,
            'company_type': self.company_type,
            'salary_min': self.salary_min,
            'salary_max': self.salary_max,
            'salary_text': self.salary_text,
            'location_city': self.location_city,
            'location_district': self.location_district,
            'location_address': self.location_address,
            'job_description': self.job_description,
            'job_requirements': self.job_requirements,
            'experience_required': self.experience_required,
            'education_required': self.education_required,
            'industry': self.industry,
            'job_tags': self.job_tags,
            'publish_time': self.publish_time,
            'update_time': self.update_time,
            'confirm_date': self.confirm_date,
            'job_url': self.job_url,
            'company_url': self.company_url,
            'welfare': self.welfare,
            'work_type': self.work_type,
            'coordinates_lat': self.coordinates_lat,
            'coordinates_lng': self.coordinates_lng,
            'raw_data': self.raw_data,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }


class DatabaseManager:
    """数据库管理器"""
    
    def __init__(self, config_manager: Optional[ConfigManager] = None, 
                 database_url: Optional[str] = None):
        """
        初始化数据库管理器
        
        Args:
            config_manager: 配置管理器
            database_url: 数据库连接URL
        """
        self.config_manager = config_manager
        self.stats = DatabaseStats()
        
        # 获取数据库配置
        if config_manager:
            # 直接从配置管理器获取原始配置字典
            db_config = config_manager.get('database', {})
            self.database_url = self._build_database_url(db_config)
        elif database_url:
            self.database_url = database_url
        else:
            raise ValueError("必须提供config_manager或database_url")
        
        # 创建引擎和会话
        self.engine = self._create_engine()
        self.SessionLocal = sessionmaker(bind=self.engine)
        
        # 创建表
        self._create_tables()
        
        logger.info(f"数据库管理器初始化完成: {self.database_url}")
    
    def _build_database_url(self, db_config: Dict[str, Any]) -> str:
        """构建数据库连接URL"""
        # 检查SQLite配置
        sqlite_config = db_config.get('sqlite', {})
        if sqlite_config.get('enabled', False):
            database_path = sqlite_config.get('database_path', './data/job_crawler.db')
            # 确保数据目录存在
            import os
            os.makedirs(os.path.dirname(database_path), exist_ok=True)
            return f"sqlite:///{database_path}"
        
        # 检查PostgreSQL配置
        postgresql_config = db_config.get('postgresql', {})
        if postgresql_config.get('enabled', False):
            host = postgresql_config.get('host', 'localhost')
            port = postgresql_config.get('port', 5432)
            username = postgresql_config.get('username', 'postgres')
            password = postgresql_config.get('password', '')
            database = postgresql_config.get('database', 'job_crawler')
            return f"postgresql://{username}:{password}@{host}:{port}/{database}"
        
        # 默认使用MySQL配置
        mysql_config = db_config.get('mysql', {}).get('primary', {})
        
        host = mysql_config.get('host', 'localhost')
        port = mysql_config.get('port', 3306)
        username = mysql_config.get('username', 'root')
        password = mysql_config.get('password', '')
        database = mysql_config.get('database', 'job_crawler')
        charset = mysql_config.get('charset', 'utf8mb4')
        
        return f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}?charset={charset}"
    
    def _create_engine(self):
        """创建数据库引擎"""
        # 基础引擎配置
        engine_config = {
            'pool_pre_ping': True,
            'echo': False
        }
        
        # 根据数据库类型调整配置
        if 'sqlite' in self.database_url:
            # SQLite特定配置
            engine_config.update({
                'pool_size': 1,  # SQLite不支持多连接
                'max_overflow': 0,
                'poolclass': None,  # 使用默认的StaticPool
                'connect_args': {'check_same_thread': False}  # 允许多线程访问
            })
        else:
            # MySQL/PostgreSQL配置
            engine_config.update({
                'poolclass': QueuePool,
                'pool_size': 10,
                'max_overflow': 20,
                'pool_recycle': 3600
            })
            
            if self.config_manager:
                db_config = self.config_manager.get('database', {})
                
                # 根据数据库类型获取池配置
                if 'postgresql' in self.database_url:
                    pool_config = db_config.get('postgresql', {})
                else:
                    pool_config = db_config.get('mysql', {}).get('primary', {})
                
                engine_config.update({
                    'pool_size': pool_config.get('pool_size', 10),
                    'max_overflow': pool_config.get('max_overflow', 20),
                    'pool_recycle': pool_config.get('pool_recycle', 3600)
                })
        
        return create_engine(self.database_url, **engine_config)
    
    def _create_tables(self):
        """创建数据库表"""
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("数据库表创建完成")
        except Exception as e:
            logger.error(f"创建数据库表失败: {e}")
            raise
    
    def init_tables(self):
        """初始化数据库表（公共接口）"""
        self._create_tables()
    
    @contextmanager
    def get_session(self):
        """获取数据库会话 (上下文管理器)"""
        session = self.SessionLocal()
        self.stats.total_connections += 1
        self.stats.active_connections += 1
        
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"数据库会话异常: {e}")
            raise
        finally:
            session.close()
            self.stats.active_connections -= 1
    
    @retry_on_database_error
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        执行查询语句
        
        Args:
            query: SQL查询语句
            params: 查询参数
            
        Returns:
            查询结果列表
        """
        start_time = time.time()
        self.stats.total_queries += 1
        
        try:
            with self.get_session() as session:
                result = session.execute(text(query), params or {})
                
                # 检查是否是SELECT查询
                query_upper = query.strip().upper()
                if query_upper.startswith('SELECT'):
                    rows = result.fetchall()
                    # 转换为字典列表
                    columns = result.keys()
                    data = [dict(zip(columns, row)) for row in rows]
                else:
                    # 对于非SELECT语句（INSERT, UPDATE, DELETE等），返回空列表
                    data = []
                
                query_time = time.time() - start_time
                self.stats.successful_queries += 1
                self.stats.total_query_time += query_time
                
                logger.debug(f"查询执行成功，耗时 {query_time:.3f}s，返回 {len(data)} 行")
                return data
                
        except Exception as e:
            self.stats.failed_queries += 1
            logger.error(f"查询执行失败: {e}")
            raise
    
    @retry_on_database_error
    def insert_job_listing(self, job_data: Dict[str, Any]) -> bool:
        """
        插入职位信息
        
        Args:
            job_data: 职位数据字典
            
        Returns:
            是否插入成功
        """
        try:
            with self.get_session() as session:
                # 过滤掉None值和不存在的字段，只保留JobListing模型中定义的字段
                valid_fields = {
                    'job_id', 'title', 'company_name', 'company_size', 'company_type',
                    'salary_min', 'salary_max', 'salary_text', 'location_city', 
                    'location_district', 'location_address', 'job_description', 
                    'job_requirements', 'experience_required', 'education_required',
                    'industry', 'job_tags', 'publish_time', 'update_time', 'job_url',
                    'company_url', 'welfare', 'work_type', 'coordinates_lat', 
                    'coordinates_lng', 'raw_data'
                }
                
                # 只保留有效字段且值不为None的数据
                filtered_data = {
                    k: v for k, v in job_data.items() 
                    if k in valid_fields and v is not None
                }
                
                job_listing = JobListing(**filtered_data)
                session.add(job_listing)
                session.flush()  # 获取ID但不提交
                
                self.stats.total_inserts += 1
                logger.debug(f"插入职位信息成功: {job_data.get('job_id', 'unknown')}")
                return True
                
        except IntegrityError as e:
            if "Duplicate entry" in str(e) or "UNIQUE constraint failed" in str(e):
                logger.debug(f"职位已存在，跳过插入: {job_data.get('job_id', 'unknown')}")
                return False
            else:
                logger.error(f"插入职位信息失败 (完整性错误): {e}")
                raise
        except Exception as e:
            logger.error(f"插入职位信息失败: {e}")
            raise
    
    @retry_on_database_error
    def batch_insert_job_listings(self, job_data_list: List[Dict[str, Any]]) -> Tuple[int, int]:
        """
        批量插入职位信息
        
        Args:
            job_data_list: 职位数据列表
            
        Returns:
            (成功插入数量, 跳过数量)
        """
        if not job_data_list:
            return 0, 0
        
        inserted_count = 0
        skipped_count = 0
        
        try:
            with self.get_session() as session:
                for job_data in job_data_list:
                    try:
                        job_listing = JobListing(**job_data)
                        session.add(job_listing)
                        session.flush()
                        inserted_count += 1
                        
                    except IntegrityError as e:
                        session.rollback()
                        if "Duplicate entry" in str(e):
                            skipped_count += 1
                            logger.debug(f"职位已存在，跳过: {job_data.get('job_id', 'unknown')}")
                        else:
                            logger.error(f"插入职位失败: {e}")
                            raise
                    except Exception as e:
                        session.rollback()
                        logger.error(f"插入职位异常: {e}")
                        raise
                
                self.stats.total_inserts += inserted_count
                logger.info(f"批量插入完成: 成功 {inserted_count}, 跳过 {skipped_count}")
                
        except Exception as e:
            logger.error(f"批量插入失败: {e}")
            raise
        
        return inserted_count, skipped_count
    
    @retry_on_database_error
    def update_job_listing(self, job_id: str, update_data: Dict[str, Any]) -> bool:
        """
        更新职位信息
        
        Args:
            job_id: 职位ID
            update_data: 更新数据
            
        Returns:
            是否更新成功
        """
        try:
            with self.get_session() as session:
                job_listing = session.query(JobListing).filter(JobListing.job_id == job_id).first()
                
                if job_listing:
                    for key, value in update_data.items():
                        if hasattr(job_listing, key):
                            setattr(job_listing, key, value)
                    
                    job_listing.updated_at = time.time()
                    self.stats.total_updates += 1
                    
                    logger.debug(f"更新职位信息成功: {job_id}")
                    return True
                else:
                    logger.warning(f"未找到职位: {job_id}")
                    return False
                    
        except Exception as e:
            logger.error(f"更新职位信息失败: {e}")
            raise
    
    @retry_on_database_error
    def delete_job_listing(self, job_id: str) -> bool:
        """
        删除职位信息
        
        Args:
            job_id: 职位ID
            
        Returns:
            是否删除成功
        """
        try:
            with self.get_session() as session:
                job_listing = session.query(JobListing).filter(JobListing.job_id == job_id).first()
                
                if job_listing:
                    session.delete(job_listing)
                    self.stats.total_deletes += 1
                    
                    logger.debug(f"删除职位信息成功: {job_id}")
                    return True
                else:
                    logger.warning(f"未找到职位: {job_id}")
                    return False
                    
        except Exception as e:
            logger.error(f"删除职位信息失败: {e}")
            raise
    
    def get_job_listing(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        获取职位信息
        
        Args:
            job_id: 职位ID
            
        Returns:
            职位信息字典，如果不存在则返回None
        """
        try:
            with self.get_session() as session:
                job_listing = session.query(JobListing).filter(JobListing.job_id == job_id).first()
                
                if job_listing:
                    return job_listing.to_dict()
                else:
                    return None
                    
        except Exception as e:
            logger.error(f"获取职位信息失败: {e}")
            raise
    
    def search_job_listings(self, 
                           keyword: Optional[str] = None,
                           city: Optional[str] = None,
                           company: Optional[str] = None,
                           salary_min: Optional[int] = None,
                           salary_max: Optional[int] = None,
                           limit: int = 100,
                           offset: int = 0) -> List[Dict[str, Any]]:
        """
        搜索职位信息
        
        Args:
            keyword: 关键词
            city: 城市
            company: 公司
            salary_min: 最低薪资
            salary_max: 最高薪资
            limit: 限制数量
            offset: 偏移量
            
        Returns:
            职位信息列表
        """
        try:
            with self.get_session() as session:
                query = session.query(JobListing)
                
                # 添加搜索条件
                if keyword:
                    query = query.filter(
                        (JobListing.title.contains(keyword)) |
                        (JobListing.job_description.contains(keyword)) |
                        (JobListing.job_requirements.contains(keyword))
                    )
                
                if city:
                    query = query.filter(JobListing.location_city == city)
                
                if company:
                    query = query.filter(JobListing.company_name.contains(company))
                
                if salary_min is not None:
                    query = query.filter(JobListing.salary_min >= salary_min)
                
                if salary_max is not None:
                    query = query.filter(JobListing.salary_max <= salary_max)
                
                # 排序和分页
                query = query.order_by(JobListing.created_at.desc())
                query = query.offset(offset).limit(limit)
                
                results = query.all()
                return [job.to_dict() for job in results]
                
        except Exception as e:
            logger.error(f"搜索职位信息失败: {e}")
            raise
    
    def get_job_count(self, **filters) -> int:
        """
        获取职位数量
        
        Args:
            **filters: 过滤条件
            
        Returns:
            职位数量
        """
        try:
            with self.get_session() as session:
                query = session.query(JobListing)
                
                # 应用过滤条件
                for key, value in filters.items():
                    if hasattr(JobListing, key) and value is not None:
                        query = query.filter(getattr(JobListing, key) == value)
                
                return query.count()
                
        except Exception as e:
            logger.error(f"获取职位数量失败: {e}")
            raise
    
    def export_to_dataframe(self, query: Optional[str] = None, **filters) -> pd.DataFrame:
        """
        导出数据到DataFrame
        
        Args:
            query: 自定义查询语句
            **filters: 过滤条件
            
        Returns:
            pandas DataFrame
        """
        try:
            if query:
                # 使用自定义查询
                data = self.execute_query(query)
            else:
                # 使用搜索方法
                data = self.search_job_listings(**filters)
            
            df = pd.DataFrame(data)
            logger.info(f"导出数据到DataFrame: {len(df)} 行")
            return df
            
        except Exception as e:
            logger.error(f"导出数据失败: {e}")
            raise
    
    def import_from_dataframe(self, df: pd.DataFrame, 
                             if_exists: str = 'append') -> Tuple[int, int]:
        """
        从DataFrame导入数据
        
        Args:
            df: pandas DataFrame
            if_exists: 如果表存在的处理方式 ('fail', 'replace', 'append')
            
        Returns:
            (成功导入数量, 跳过数量)
        """
        try:
            # 转换DataFrame为字典列表
            job_data_list = df.to_dict('records')
            
            # 批量插入
            return self.batch_insert_job_listings(job_data_list)
            
        except Exception as e:
            logger.error(f"从DataFrame导入数据失败: {e}")
            raise
    
    def cleanup_old_data(self, days: int = 30) -> int:
        """
        清理旧数据
        
        Args:
            days: 保留天数
            
        Returns:
            删除的记录数
        """
        try:
            cutoff_time = time.time() - (days * 24 * 3600)
            
            with self.get_session() as session:
                deleted_count = session.query(JobListing).filter(
                    JobListing.created_at < cutoff_time
                ).delete()
                
                self.stats.total_deletes += deleted_count
                logger.info(f"清理旧数据完成: 删除 {deleted_count} 条记录")
                return deleted_count
                
        except Exception as e:
            logger.error(f"清理旧数据失败: {e}")
            raise
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        获取数据库统计信息
        
        Returns:
            统计信息字典
        """
        try:
            with self.get_session() as session:
                total_jobs = session.query(JobListing).count()
                
                # 按城市统计
                city_stats = session.execute(text("""
                    SELECT location_city, COUNT(*) as count 
                    FROM job_listings 
                    WHERE location_city IS NOT NULL 
                    GROUP BY location_city 
                    ORDER BY count DESC 
                    LIMIT 10
                """)).fetchall()
                
                # 按公司统计
                company_stats = session.execute(text("""
                    SELECT company_name, COUNT(*) as count 
                    FROM job_listings 
                    WHERE company_name IS NOT NULL 
                    GROUP BY company_name 
                    ORDER BY count DESC 
                    LIMIT 10
                """)).fetchall()
                
                # 薪资统计
                salary_stats = session.execute(text("""
                    SELECT 
                        AVG(salary_min) as avg_min_salary,
                        AVG(salary_max) as avg_max_salary,
                        MIN(salary_min) as min_salary,
                        MAX(salary_max) as max_salary
                    FROM job_listings 
                    WHERE salary_min IS NOT NULL AND salary_max IS NOT NULL
                """)).fetchone()
                
                return {
                    'database_stats': self.stats,
                    'total_jobs': total_jobs,
                    'top_cities': [{'city': row[0], 'count': row[1]} for row in city_stats],
                    'top_companies': [{'company': row[0], 'count': row[1]} for row in company_stats],
                    'salary_stats': {
                        'avg_min_salary': salary_stats[0] if salary_stats else None,
                        'avg_max_salary': salary_stats[1] if salary_stats else None,
                        'min_salary': salary_stats[2] if salary_stats else None,
                        'max_salary': salary_stats[3] if salary_stats else None
                    } if salary_stats else {}
                }
                
        except Exception as e:
            logger.error(f"获取统计信息失败: {e}")
            raise
    
    def test_connection(self) -> bool:
        """
        测试数据库连接
        
        Returns:
            连接是否正常
        """
        try:
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
                logger.info("数据库连接测试成功")
                return True
                
        except Exception as e:
            logger.error(f"数据库连接测试失败: {e}")
            return False
    
    def get_stats(self) -> DatabaseStats:
        """
        获取数据库管理器统计信息
        
        Returns:
            数据库统计信息
        """
        return self.stats
    
    def reset_stats(self):
        """重置统计信息"""
        self.stats = DatabaseStats()
        logger.info("数据库统计信息已重置")
    
    def close(self):
        """关闭数据库连接"""
        if hasattr(self, 'engine'):
            self.engine.dispose()
            logger.info("数据库连接已关闭")
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close()
    
    def __str__(self) -> str:
        """字符串表示"""
        return f"DatabaseManager(queries={self.stats.total_queries}, success_rate={self.stats.success_rate:.2%})"
    
    def __repr__(self) -> str:
        """对象表示"""
        return self.__str__()


def create_database_manager_from_config(config_manager: ConfigManager) -> DatabaseManager:
    """
    从配置管理器创建数据库管理器
    
    Args:
        config_manager: 配置管理器实例
        
    Returns:
        数据库管理器实例
    """
    db_manager = DatabaseManager(config_manager=config_manager)
    
    # 测试连接
    if not db_manager.test_connection():
        logger.warning("数据库连接测试失败，但继续创建管理器")
    
    logger.info(f"从配置创建数据库管理器: {db_manager}")
    return db_manager


def create_sqlite_database_manager(database_path: str = "./data/job_crawler.db") -> DatabaseManager:
    """
    创建SQLite数据库管理器（用于本地测试）
    
    Args:
        database_path: SQLite数据库文件路径
        
    Returns:
        数据库管理器实例
    """
    import os
    
    # 确保数据目录存在
    os.makedirs(os.path.dirname(database_path), exist_ok=True)
    
    # 构建SQLite连接URL
    database_url = f"sqlite:///{database_path}"
    
    # 创建数据库管理器
    db_manager = DatabaseManager(database_url=database_url)
    
    # SQLite特定优化
    if 'sqlite' in database_url:
        try:
            with db_manager.get_session() as session:
                # 启用WAL模式以提高并发性能
                session.execute(text("PRAGMA journal_mode=WAL"))
                # 启用外键约束
                session.execute(text("PRAGMA foreign_keys=ON"))
                # 设置同步模式为NORMAL以提高性能
                session.execute(text("PRAGMA synchronous=NORMAL"))
                # 增加缓存大小
                session.execute(text("PRAGMA cache_size=10000"))
                # 设置临时存储为内存
                session.execute(text("PRAGMA temp_store=MEMORY"))
                
            logger.info(f"SQLite数据库优化完成: {database_path}")
        except Exception as e:
            logger.warning(f"SQLite优化失败: {e}")
    
    # 测试连接
    if not db_manager.test_connection():
        logger.warning("SQLite数据库连接测试失败")
    
    logger.info(f"创建SQLite数据库管理器: {database_path}")
    return db_manager