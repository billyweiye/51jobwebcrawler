#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库连接管理器
提供安全的数据库连接和事务管理
"""

import logging
import pandas as pd
from sqlalchemy import create_engine, text
from contextlib import contextmanager
import time
from sqlalchemy.exc import OperationalError, DisconnectionError
import pymysql.err

logger = logging.getLogger(__name__)

class DatabaseManager:
    """数据库连接管理器"""
    
    def __init__(self, mysql_config):
        """
        初始化数据库管理器
        
        Args:
            mysql_config: MySQL配置字典，包含user, password, host, port, database
        """
        self.mysql_config = mysql_config
        self.connection_string = (
            f"mysql+pymysql://{mysql_config['user']}:{mysql_config['password']}"
            f"@{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"
            f"?charset=utf8mb4"
        )
        
    def create_engine(self):
        """创建数据库引擎"""
        try:
            engine = create_engine(
                self.connection_string,
                pool_pre_ping=True,      # 连接前检查连接是否有效
                pool_recycle=1800,       # 30分钟后回收连接（减少超时风险）
                pool_size=3,             # 减少连接池大小，避免过多连接
                max_overflow=5,          # 减少最大溢出连接数
                pool_timeout=60,         # 增加获取连接的超时时间
                connect_args={
                    'connect_timeout': 60,   # MySQL连接超时时间
                    'read_timeout': 60,      # 读取超时时间
                    'write_timeout': 60,     # 写入超时时间
                    'charset': 'utf8mb4',
                    'autocommit': False
                },
                echo=False               # 不输出SQL语句
            )
            logger.debug("数据库引擎创建成功")
            return engine
        except Exception as e:
            logger.error(f"创建数据库引擎失败: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """获取数据库连接的上下文管理器"""
        engine = None
        connection = None
        try:
            engine = self.create_engine()
            connection = engine.connect()
            logger.debug("数据库连接获取成功")
            yield connection
        except Exception as e:
            logger.error(f"数据库连接操作失败: {e}")
            raise
        finally:
            if connection:
                connection.close()
                logger.debug("数据库连接已关闭")
            if engine:
                engine.dispose()
                logger.debug("数据库引擎已释放")
    
    @contextmanager
    def get_transaction(self):
        """获取数据库事务的上下文管理器"""
        engine = None
        connection = None
        trans = None
        try:
            engine = self.create_engine()
            connection = engine.connect()
            trans = connection.begin()
            logger.debug("数据库事务开始")
            yield connection
            trans.commit()
            logger.debug("数据库事务提交成功")
        except Exception as e:
            if trans:
                trans.rollback()
                logger.warning(f"数据库事务已回滚: {e}")
            logger.error(f"数据库事务操作失败: {e}")
            raise
        finally:
            if connection:
                connection.close()
                logger.debug("数据库连接已关闭")
            if engine:
                engine.dispose()
                logger.debug("数据库引擎已释放")
    
    def save_dataframe(self, df, table_name, if_exists='append', chunksize=1000, max_retries=3):
        """
        安全地保存DataFrame到数据库，支持重试机制
        
        Args:
            df: 要保存的DataFrame
            table_name: 目标表名
            if_exists: 如果表存在的处理方式 ('fail', 'replace', 'append')
            chunksize: 批量插入的大小
            max_retries: 最大重试次数
            
        Returns:
            bool: 是否保存成功
        """
        for attempt in range(max_retries + 1):
            try:
                with self.get_transaction() as connection:
                    df.to_sql(
                        table_name, 
                        con=connection, 
                        if_exists=if_exists, 
                        index=False, 
                        chunksize=chunksize, 
                        method='multi'
                    )
                    logger.info(f"成功保存 {len(df)} 条数据到表 {table_name}")
                    return True
            except (OperationalError, DisconnectionError, pymysql.err.OperationalError) as e:
                if attempt < max_retries:
                    wait_time = (attempt + 1) * 2  # 递增等待时间：2秒、4秒、6秒
                    logger.warning(f"数据库连接异常，第 {attempt + 1} 次重试失败: {e}")
                    logger.info(f"等待 {wait_time} 秒后进行第 {attempt + 2} 次重试")
                    time.sleep(wait_time)
                else:
                    logger.error(f"保存数据到表 {table_name} 失败，已达到最大重试次数 {max_retries}: {e}")
                    return False
            except Exception as e:
                logger.error(f"保存数据到表 {table_name} 失败: {e}")
                return False
        return False
    
    def execute_query(self, query, params=None, max_retries=3):
        """
        执行查询并返回结果
        
        Args:
            query: SQL查询语句
            params: 查询参数（列表或元组）
            max_retries: 最大重试次数
            
        Returns:
            pandas.DataFrame: 查询结果
        """
        for attempt in range(max_retries + 1):
            try:
                with self.get_connection() as connection:
                    if params:
                        # 使用text()包装查询并传递参数
                        from sqlalchemy import text
                        result = pd.read_sql(text(query), connection, params=params)
                    else:
                        result = pd.read_sql(query, connection)
                    logger.debug(f"查询执行成功，返回 {len(result)} 行数据")
                    return result
                    
            except (OperationalError, DisconnectionError, pymysql.err.OperationalError) as e:
                if attempt < max_retries:
                    wait_time = 2 ** attempt
                    logger.warning(f"数据库查询失败 (尝试 {attempt + 1}/{max_retries + 1}): {e}")
                    logger.info(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"数据库查询最终失败: {e}")
                    raise
            except Exception as e:
                logger.error(f"执行查询失败: {e}")
                raise
    
    def execute_sql(self, sql, params=None, max_retries=3):
        """
        执行SQL语句（非查询），支持重试机制
        
        Args:
            sql: SQL语句
            params: SQL参数
            max_retries: 最大重试次数
            
        Returns:
            bool: 是否执行成功
        """
        for attempt in range(max_retries + 1):
            try:
                with self.get_transaction() as connection:
                    if params:
                        # 确保参数格式正确
                        if isinstance(params, dict):
                            connection.execute(text(sql), params)
                        elif isinstance(params, (list, tuple)):
                            # 转换为字典格式
                            param_dict = {f'param_{i}': param for i, param in enumerate(params)}
                            # 更新SQL中的占位符
                            import re
                            sql_updated = sql
                            for i, param in enumerate(params):
                                sql_updated = re.sub(r'%s', f':param_{i}', sql_updated, count=1)
                            connection.execute(text(sql_updated), param_dict)
                        else:
                            connection.execute(text(sql), {'param_0': params})
                    else:
                        connection.execute(text(sql))
                    logger.debug(f"SQL执行成功: {sql[:50]}...")
                    return True
            except (OperationalError, DisconnectionError, pymysql.err.OperationalError) as e:
                if attempt < max_retries:
                    wait_time = (attempt + 1) * 2
                    logger.warning(f"数据库连接异常，SQL执行重试第 {attempt + 1} 次失败: {e}")
                    logger.info(f"等待 {wait_time} 秒后进行第 {attempt + 2} 次重试")
                    time.sleep(wait_time)
                else:
                    logger.error(f"执行SQL失败，已达到最大重试次数 {max_retries}: {e}")
                    return False
            except Exception as e:
                logger.error(f"执行SQL失败: {e}")
                return False
        return False
    
    def test_connection(self):
        """
        测试数据库连接
        
        Returns:
            bool: 连接是否成功
        """
        try:
            with self.get_connection() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"数据库连接测试失败: {e}")
            return False
    
    def get_table_info(self, table_name):
        """
        获取表信息
        
        Args:
            table_name: 表名
            
        Returns:
            dict: 表信息，包含exists和row_count
        """
        info = {
            'exists': False,
            'row_count': 0
        }
        
        try:
            # 检查表是否存在
            result = self.execute_query(
                "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = :schema AND table_name = :table",
                params={'schema': self.mysql_config['database'], 'table': table_name}
            )
            
            if result.iloc[0]['count'] > 0:
                info['exists'] = True
                
                # 获取行数
                count_result = self.execute_query(f"SELECT COUNT(*) as count FROM {table_name}")
                info['row_count'] = count_result.iloc[0]['count']
            
        except Exception as e:
            logger.error(f"获取表信息失败: {e}")
        
        return info