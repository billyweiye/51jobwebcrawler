import time
import configparser
from typing import Dict, List
from spiders.job_spider import JobSpider
from data.processors import DataProcessor, D1Client
from utils.logger import setup_logger

def load_config() -> Dict:
    """加载所有配置"""
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8')
    
    return {
        'd1': {
            'account_id': config['D1']['ACCOUNT_ID'],
            'api_token': config['D1']['API_TOKEN'],
            'database_name': config['D1']['DATABASE_NAME']
        },
        'spider': {
            'base_url': 'https://we.51job.com/api/job/search-pc',
            'cookies': {
                'guid': '21ae369c1fecc95608a454bacdd16b41',
                'acw_tc': config['SPIDER']['ACW_TC']
            },
            'headers': {
                'User-Agent': config['SPIDER']['USER_AGENT']
            }
        },
        'search': {
            'keywords': config['SEARCH']['KEYWORDS'].split(','),
            'cities': config['SEARCH']['CITIES'].split(','),
            'max_page': config.getint('SEARCH', 'MAX_PAGE', fallback=3)
        }
    }

def main():
    # 初始化组件
    logger = setup_logger('JobCrawler')
    config = load_config()
    
    # 初始化爬虫
    spider = JobSpider(
        base_url=config['spider']['base_url'],
        cookies=config['spider']['cookies'],
        headers=config['spider']['headers']
    )
    
    # 初始化数据处理器和数据库客户端
    processor = DataProcessor(logger)
    d1_client = D1Client(
        account_id=config['d1']['account_id'],
        api_token=config['d1']['api_token'],
        database_name=config['d1']['database_name']
    )

    # 执行爬取任务
    for keyword in config['search']['keywords']:
        for city in config['search']['cities']:
            logger.info(f"开始搜索关键词：{keyword} 城市：{city}")
            
            try:
                city_code = spider.get_city_code(city.strip())
                params = build_search_params(keyword, city_code)
                
                raw_data = spider.fetch_jobs(params)
                processed_data = processor.process(raw_data)
                d1_records = processor.convert_to_d1_format(processed_data)
                
                if d1_client.batch_insert(d1_records):
                    logger.info(f"成功写入{len(d1_records)}条数据到D1数据库")
                else:
                    logger.error("数据库写入失败")

            except Exception as e:
                logger.error(f"处理失败: {str(e)}")
                continue
                
            time.sleep(5)  # 遵守爬虫礼仪

def build_search_params(keyword: str, city_code: str) -> Dict:
    """构建搜索参数"""
    return {
        'api_key': '51job',
        'timestamp': str(int(time.time())),
        'keyword': keyword,
        'searchType': '2',
        'jobArea': city_code,
        'pageNum': '1',
        'pageSize': '50'
    }

if __name__ == "__main__":
    main()