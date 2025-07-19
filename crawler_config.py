# 爬虫配置文件

# 延时配置
DELAY_CONFIG = {
    'base_page_delay': (3, 8),  # 每页抓取后的基础延时范围（秒）
    'high_page_delay': (2, 5),  # 高页数时的额外延时范围（秒）
    'city_task_delay': (15, 30),  # 每个城市任务完成后的延时范围（秒）
    'no_data_penalty': (30, 60),  # 无数据时的惩罚延时范围（秒）
    'anti_spider_delay': (30, 60),  # 触发反爬虫时的延时范围（秒）
    'retry_delay_base': (5, 15),  # 重试时的基础延时范围（秒）
}

# 重试配置
RETRY_CONFIG = {
    'max_retries': 5,  # 最大重试次数
    'timeout': 30,  # 请求超时时间（秒）
    'anti_spider_codes': [403, 429, 503, 521],  # 反爬虫状态码
}

# User-Agent池
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0'
]

# 数据验证配置
DATA_VALIDATION = {
    'required_fields': ['status', 'resultbody'],  # API响应必需字段
    'success_status': '1',  # 成功状态值
    'min_items_per_page': 1,  # 每页最少职位数量
}

# 日志配置
LOG_CONFIG = {
    'log_request_details': True,  # 是否记录请求详情
    'log_response_preview': True,  # 是否记录响应预览
    'preview_length': 200,  # 响应预览长度
}

# 数据库配置
DB_CONFIG = {
    'batch_size': 1000,  # 批量插入大小
    'connection_retry': 5,  # 数据库连接重试次数
}