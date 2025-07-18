# 爬虫监控和统计模块

import json
import time
from datetime import datetime, timedelta
from collections import defaultdict, deque
import logging

logger = logging.getLogger(__name__)

class CrawlerMonitor:
    """爬虫监控类，用于统计和分析爬虫运行状况"""
    
    def __init__(self):
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'empty_responses': 0,
            'anti_spider_hits': 0,
            'cities_completed': 0,
            'cities_failed': 0,
            'total_jobs_crawled': 0,
            'start_time': datetime.now(),
            'last_success_time': None,
            'consecutive_failures': 0,
            'error_types': defaultdict(int),
            'response_times': deque(maxlen=100),  # 保存最近100次请求的响应时间
            'hourly_stats': defaultdict(lambda: {'requests': 0, 'success': 0, 'jobs': 0})
        }
        
    def record_request(self, success=True, response_time=None, error_type=None, jobs_count=0):
        """记录请求统计"""
        self.stats['total_requests'] += 1
        current_hour = datetime.now().strftime('%Y-%m-%d %H:00')
        self.stats['hourly_stats'][current_hour]['requests'] += 1
        
        if success:
            self.stats['successful_requests'] += 1
            self.stats['hourly_stats'][current_hour]['success'] += 1
            self.stats['hourly_stats'][current_hour]['jobs'] += jobs_count
            self.stats['total_jobs_crawled'] += jobs_count
            self.stats['last_success_time'] = datetime.now()
            self.stats['consecutive_failures'] = 0
            
            if jobs_count == 0:
                self.stats['empty_responses'] += 1
        else:
            self.stats['failed_requests'] += 1
            self.stats['consecutive_failures'] += 1
            if error_type:
                self.stats['error_types'][error_type] += 1
                if 'anti_spider' in error_type.lower() or 'blocked' in error_type.lower():
                    self.stats['anti_spider_hits'] += 1
        
        if response_time:
            self.stats['response_times'].append(response_time)
    
    def record_city_completion(self, success=True):
        """记录城市完成情况"""
        if success:
            self.stats['cities_completed'] += 1
        else:
            self.stats['cities_failed'] += 1
    
    def get_success_rate(self):
        """获取成功率"""
        if self.stats['total_requests'] == 0:
            return 0
        return (self.stats['successful_requests'] / self.stats['total_requests']) * 100
    
    def get_avg_response_time(self):
        """获取平均响应时间"""
        if not self.stats['response_times']:
            return 0
        return sum(self.stats['response_times']) / len(self.stats['response_times'])
    
    def is_health_critical(self):
        """检查爬虫健康状况是否严重"""
        # 连续失败超过10次
        if self.stats['consecutive_failures'] > 10:
            return True, "连续失败次数过多"
        
        # 成功率低于30%
        if self.stats['total_requests'] > 20 and self.get_success_rate() < 30:
            return True, "成功率过低"
        
        # 反爬虫命中率过高
        if self.stats['total_requests'] > 10 and (self.stats['anti_spider_hits'] / self.stats['total_requests']) > 0.5:
            return True, "反爬虫命中率过高"
        
        # 超过1小时没有成功请求
        if (self.stats['last_success_time'] and 
            datetime.now() - self.stats['last_success_time'] > timedelta(hours=1)):
            return True, "长时间无成功请求"
        
        return False, "健康状况良好"
    
    def get_recommendations(self):
        """根据统计数据给出建议"""
        recommendations = []
        
        # 成功率分析
        success_rate = self.get_success_rate()
        if success_rate < 50:
            recommendations.append("成功率较低，建议增加请求间隔时间")
        
        # 反爬虫分析
        if self.stats['total_requests'] > 0:
            anti_spider_rate = (self.stats['anti_spider_hits'] / self.stats['total_requests']) * 100
            if anti_spider_rate > 30:
                recommendations.append("反爬虫命中率高，建议更换User-Agent或增加代理")
        
        # 空响应分析
        if self.stats['successful_requests'] > 0:
            empty_rate = (self.stats['empty_responses'] / self.stats['successful_requests']) * 100
            if empty_rate > 50:
                recommendations.append("空响应率高，可能需要调整搜索参数或检查目标网站变化")
        
        # 响应时间分析
        avg_response_time = self.get_avg_response_time()
        if avg_response_time > 10:
            recommendations.append("平均响应时间较长，建议检查网络状况或减少并发")
        
        return recommendations
    
    def generate_report(self):
        """生成监控报告"""
        runtime = datetime.now() - self.stats['start_time']
        is_critical, health_msg = self.is_health_critical()
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'runtime_hours': runtime.total_seconds() / 3600,
            'health_status': 'CRITICAL' if is_critical else 'NORMAL',
            'health_message': health_msg,
            'statistics': {
                'total_requests': self.stats['total_requests'],
                'success_rate': round(self.get_success_rate(), 2),
                'total_jobs_crawled': self.stats['total_jobs_crawled'],
                'cities_completed': self.stats['cities_completed'],
                'cities_failed': self.stats['cities_failed'],
                'consecutive_failures': self.stats['consecutive_failures'],
                'anti_spider_hits': self.stats['anti_spider_hits'],
                'avg_response_time': round(self.get_avg_response_time(), 2),
                'last_success_time': self.stats['last_success_time'].isoformat() if self.stats['last_success_time'] else None
            },
            'error_breakdown': dict(self.stats['error_types']),
            'recommendations': self.get_recommendations(),
            'recent_hourly_stats': dict(list(self.stats['hourly_stats'].items())[-24:])  # 最近24小时
        }
        
        return report
    
    def log_report(self):
        """记录监控报告到日志"""
        report = self.generate_report()
        
        logger.info("=== 爬虫监控报告 ===")
        logger.info(f"运行时间: {report['runtime_hours']:.2f} 小时")
        logger.info(f"健康状况: {report['health_status']} - {report['health_message']}")
        logger.info(f"总请求数: {report['statistics']['total_requests']}")
        logger.info(f"成功率: {report['statistics']['success_rate']}%")
        logger.info(f"已爬取职位数: {report['statistics']['total_jobs_crawled']}")
        logger.info(f"完成城市数: {report['statistics']['cities_completed']}")
        logger.info(f"失败城市数: {report['statistics']['cities_failed']}")
        logger.info(f"连续失败次数: {report['statistics']['consecutive_failures']}")
        logger.info(f"反爬虫命中次数: {report['statistics']['anti_spider_hits']}")
        
        if report['recommendations']:
            logger.warning("建议:")
            for rec in report['recommendations']:
                logger.warning(f"  - {rec}")
        
        if report['error_breakdown']:
            logger.info("错误类型统计:")
            for error_type, count in report['error_breakdown'].items():
                logger.info(f"  {error_type}: {count}")
    
    def save_report(self, filepath):
        """保存监控报告到文件"""
        report = self.generate_report()
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

# 全局监控实例
monitor = CrawlerMonitor()