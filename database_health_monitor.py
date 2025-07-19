#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库健康监控器
监控数据库连接状态，提供连接健康检查和故障诊断
"""

import logging
import time
import threading
from datetime import datetime, timedelta
from database_manager import DatabaseManager
import configparser

logger = logging.getLogger(__name__)

class DatabaseHealthMonitor:
    """数据库健康监控器"""
    
    def __init__(self, mysql_config, check_interval=300):
        """
        初始化监控器
        
        Args:
            mysql_config: MySQL配置
            check_interval: 检查间隔（秒），默认5分钟
        """
        self.mysql_config = mysql_config
        self.check_interval = check_interval
        self.db_manager = DatabaseManager(mysql_config)
        self.is_monitoring = False
        self.monitor_thread = None
        
        # 监控统计
        self.stats = {
            'total_checks': 0,
            'successful_checks': 0,
            'failed_checks': 0,
            'last_check_time': None,
            'last_success_time': None,
            'last_failure_time': None,
            'consecutive_failures': 0,
            'max_consecutive_failures': 0,
            'connection_errors': []
        }
    
    def check_database_health(self):
        """
        检查数据库健康状态
        
        Returns:
            dict: 健康检查结果
        """
        check_time = datetime.now()
        self.stats['total_checks'] += 1
        self.stats['last_check_time'] = check_time
        
        health_result = {
            'timestamp': check_time,
            'is_healthy': False,
            'response_time': None,
            'error': None,
            'details': {}
        }
        
        try:
            # 测试基本连接
            start_time = time.time()
            connection_success = self.db_manager.test_connection()
            response_time = time.time() - start_time
            
            health_result['response_time'] = response_time
            
            if connection_success:
                # 连接成功，进行更详细的检查
                health_result['is_healthy'] = True
                self.stats['successful_checks'] += 1
                self.stats['last_success_time'] = check_time
                self.stats['consecutive_failures'] = 0
                
                # 检查数据库状态
                try:
                    # 检查表是否存在
                    table_info = self.db_manager.get_table_info('job_listings')
                    health_result['details']['table_exists'] = table_info['exists']
                    health_result['details']['table_row_count'] = table_info['row_count']
                    
                    # 检查最近的数据插入
                    recent_data = self.db_manager.execute_query(
                        "SELECT COUNT(*) as count FROM job_listings WHERE updateDateTime > :timestamp",
                        params={'timestamp': int(time.time()) - 3600}  # 最近1小时的数据
                    )
                    health_result['details']['recent_inserts'] = recent_data.iloc[0]['count']
                    
                except Exception as e:
                    health_result['details']['check_error'] = str(e)
                    logger.warning(f"数据库详细检查失败: {e}")
                
                logger.debug(f"数据库健康检查成功，响应时间: {response_time:.2f}秒")
            else:
                # 连接失败
                health_result['is_healthy'] = False
                health_result['error'] = "数据库连接测试失败"
                self._record_failure(check_time, "连接测试失败")
                
        except Exception as e:
            # 检查过程中发生异常
            health_result['is_healthy'] = False
            health_result['error'] = str(e)
            self._record_failure(check_time, str(e))
            logger.error(f"数据库健康检查异常: {e}")
        
        return health_result
    
    def _record_failure(self, failure_time, error_msg):
        """记录失败信息"""
        self.stats['failed_checks'] += 1
        self.stats['last_failure_time'] = failure_time
        self.stats['consecutive_failures'] += 1
        
        if self.stats['consecutive_failures'] > self.stats['max_consecutive_failures']:
            self.stats['max_consecutive_failures'] = self.stats['consecutive_failures']
        
        # 记录错误信息（保留最近50条）
        error_record = {
            'timestamp': failure_time,
            'error': error_msg
        }
        self.stats['connection_errors'].append(error_record)
        if len(self.stats['connection_errors']) > 50:
            self.stats['connection_errors'].pop(0)
        
        # 连续失败告警
        if self.stats['consecutive_failures'] >= 3:
            logger.error(f"数据库连续失败 {self.stats['consecutive_failures']} 次，请检查数据库状态")
    
    def start_monitoring(self):
        """开始监控"""
        if self.is_monitoring:
            logger.warning("监控已在运行中")
            return
        
        self.is_monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        logger.info(f"数据库健康监控已启动，检查间隔: {self.check_interval}秒")
    
    def stop_monitoring(self):
        """停止监控"""
        self.is_monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("数据库健康监控已停止")
    
    def _monitor_loop(self):
        """监控循环"""
        while self.is_monitoring:
            try:
                health_result = self.check_database_health()
                
                # 根据健康状态调整日志级别
                if health_result['is_healthy']:
                    if health_result['response_time'] > 5.0:  # 响应时间过长
                        logger.warning(f"数据库响应较慢: {health_result['response_time']:.2f}秒")
                else:
                    logger.error(f"数据库健康检查失败: {health_result['error']}")
                
                # 等待下次检查
                time.sleep(self.check_interval)
                
            except Exception as e:
                logger.exception(f"监控循环异常: {e}")
                time.sleep(min(self.check_interval, 60))  # 异常时最多等待1分钟
    
    def get_health_report(self):
        """获取健康报告"""
        if self.stats['total_checks'] == 0:
            return "尚未进行健康检查"
        
        success_rate = (self.stats['successful_checks'] / self.stats['total_checks']) * 100
        
        report = f"""
数据库健康监控报告
==================
总检查次数: {self.stats['total_checks']}
成功次数: {self.stats['successful_checks']}
失败次数: {self.stats['failed_checks']}
成功率: {success_rate:.1f}%
连续失败次数: {self.stats['consecutive_failures']}
最大连续失败次数: {self.stats['max_consecutive_failures']}
最后检查时间: {self.stats['last_check_time']}
最后成功时间: {self.stats['last_success_time']}
最后失败时间: {self.stats['last_failure_time']}
"""
        
        # 添加最近的错误信息
        if self.stats['connection_errors']:
            report += "\n最近的连接错误:\n"
            for error in self.stats['connection_errors'][-5:]:  # 显示最近5条错误
                report += f"  {error['timestamp']}: {error['error']}\n"
        
        return report
    
    def diagnose_connection_issues(self):
        """诊断连接问题"""
        diagnosis = []
        
        # 检查连续失败
        if self.stats['consecutive_failures'] > 0:
            diagnosis.append(f"当前连续失败 {self.stats['consecutive_failures']} 次")
        
        # 检查成功率
        if self.stats['total_checks'] > 0:
            success_rate = (self.stats['successful_checks'] / self.stats['total_checks']) * 100
            if success_rate < 90:
                diagnosis.append(f"成功率较低: {success_rate:.1f}%")
        
        # 分析错误模式
        if self.stats['connection_errors']:
            recent_errors = self.stats['connection_errors'][-10:]
            error_types = {}
            for error in recent_errors:
                error_msg = error['error']
                if 'timeout' in error_msg.lower():
                    error_types['timeout'] = error_types.get('timeout', 0) + 1
                elif 'connection' in error_msg.lower():
                    error_types['connection'] = error_types.get('connection', 0) + 1
                elif 'operational' in error_msg.lower():
                    error_types['operational'] = error_types.get('operational', 0) + 1
            
            if error_types:
                diagnosis.append("错误类型分析:")
                for error_type, count in error_types.items():
                    diagnosis.append(f"  {error_type}: {count} 次")
        
        # 提供建议
        suggestions = []
        if self.stats['consecutive_failures'] >= 3:
            suggestions.append("建议检查MySQL服务器状态")
            suggestions.append("检查网络连接")
            suggestions.append("检查数据库配置")
        
        if 'timeout' in str(self.stats['connection_errors']):
            suggestions.append("考虑增加连接超时时间")
            suggestions.append("检查数据库服务器负载")
        
        result = {
            'diagnosis': diagnosis,
            'suggestions': suggestions,
            'stats': self.stats
        }
        
        return result

def main():
    """主函数 - 用于测试监控器"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # 读取配置
        config = configparser.ConfigParser()
        config.read("config.ini", encoding='utf-8')
        mysql_config = config['mysql']
        
        # 创建监控器
        monitor = DatabaseHealthMonitor(mysql_config, check_interval=30)  # 30秒检查一次
        
        # 进行一次健康检查
        logger.info("进行数据库健康检查...")
        health_result = monitor.check_database_health()
        logger.info(f"健康检查结果: {health_result}")
        
        # 显示健康报告
        logger.info("\n" + monitor.get_health_report())
        
        # 诊断连接问题
        diagnosis = monitor.diagnose_connection_issues()
        if diagnosis['diagnosis'] or diagnosis['suggestions']:
            logger.info("\n连接问题诊断:")
            for item in diagnosis['diagnosis']:
                logger.info(f"  {item}")
            if diagnosis['suggestions']:
                logger.info("建议:")
                for suggestion in diagnosis['suggestions']:
                    logger.info(f"  {suggestion}")
        
    except Exception as e:
        logger.exception(f"监控器测试异常: {e}")

if __name__ == "__main__":
    main()