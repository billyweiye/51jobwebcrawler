import time
import pandas as pd
import random
from jobSearch import JobSearch
import logging
import configparser
import schedule
import pytz
import datetime
import os
from sqlalchemy import create_engine
from logging.handlers import TimedRotatingFileHandler
from crawler_config import DELAY_CONFIG, RETRY_CONFIG, USER_AGENTS, DATA_VALIDATION
from crawler_monitor import monitor
from database_manager import DatabaseManager
from auth_manager import get_auth_manager


# 配置根日志记录器，确保所有模块的日志都能输出到文件
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# 创建 TimedRotatingFileHandler，每 6 小时滚动一次日志文件
log_handler = TimedRotatingFileHandler('app.log', when='H', interval=6, backupCount=4)
log_handler.setLevel(logging.DEBUG)

# 配置日志格式
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_handler.setFormatter(log_formatter)

# 添加处理器到根记录器，这样所有模块的日志都会输出到文件
root_logger.addHandler(log_handler)

# 获取当前模块的logger
logger = logging.getLogger(__name__)

# 省级代码
province_codes = {
    "010000": "北京",
    "020000": "上海",
    "030000": "广东省",
    "040000": "深圳",
    "050000": "天津",
    "060000": "重庆",
    "070000": "江苏省",
    "080000": "浙江省",
    "090000": "四川省",
    "100000": "海南省",
    "110000": "福建省",
    "120000": "山东省",
    "130000": "江西省",
    "140000": "广西",
    "150000": "安徽省",
    "160000": "河北省",
    "170000": "河南省",
    "180000": "湖北省",
    "190000": "湖南省",
    "200000": "陕西省",
    "210000": "山西省",
    "220000": "黑龙江省",
    "230000": "辽宁省",
    "240000": "吉林省",
    "250000": "云南省",
    "260000": "贵州省",
    "270000": "甘肃省",
    "280000": "内蒙古",
    "290000": "宁夏",
    "300000": "西藏",
    "310000": "新疆",
    "320000": "青海省" 
    }

# 读取配置文件
config = configparser.ConfigParser()
config.read("config.ini",encoding='utf-8')


def search():
    try:
        logger.info("开始一次新的搜索任务")

        kws = config["job_search"]["kws"].split(",")
        cities = config["job_search"]["cities"].split(",")
        account_id=config["job_search"]['account_id']

        if '000000' in cities:
            logger.info("检测到全省搜索，使用所有省级代码")
            cities = list(province_codes.keys())

        url = "https://we.51job.com/api/job/search-pc"

        # 获取认证管理器实例
        auth_manager = get_auth_manager()
        logger.info("认证管理器已准备就绪，将为每个搜索任务动态生成认证信息")

        for kw in kws:
            for city in cities:
                logger.info(f"开始搜索关键词：{kw} 地区：{city}")
                result = []
                page = 1
                max_page = config.getint("job_search", "max_page", fallback=10)
                
                # 为当前搜索任务获取动态认证信息
                auth_info = auth_manager.get_current_auth_info(keyword=kw, job_area=city)
                headers = auth_info['headers']
                cookies = auth_info['cookies']
                logger.info(f"为关键词 {kw} 地区 {city} 生成动态认证信息")

                jobs = JobSearch(url, headers, cookies)

                while True:
                    logger.info(f"正在抓取第 {page} 页")
                    user_params = {
                        "api_key": "51job",
                        "timestamp": f"{int(time.time())}",
                        "keyword": kw,
                        "searchType": "2",
                        "function": "",
                        "industry": "",
                        "jobArea": city,
                        "jobArea2": "",
                        "landmark": "",
                        "metro": "",
                        "salary": "",
                        "workYear": "",
                        "degree": "",
                        "companyType": "",
                        "companySize": "",
                        "jobType": "",
                        "issueDate": "",
                        "sortType": "1",  # 改为0，与浏览器请求一致
                        "pageNum": page,
                        "requestId": "",  # 新增requestId参数
                        "keywordType": "",  # 新增keywordType参数
                        "pageSize": "20",
                        "source": "1",
                        "accountId": account_id,
                        "pageCode": "sou%7Csou%7Csoulb",
                        "scene": "7"  # 保持scene参数
                    }

                    try:
                        res_json = jobs.get_jobs_json(params=user_params)
                    except Exception as e:
                        logger.error(f"请求接口异常: {e}, params: {user_params}")
                        monitor.record_request(success=False, error_type=f"request_exception_{type(e).__name__}")
                        # 遇到异常时增加延时，避免频繁请求
                        time.sleep(random.uniform(*DELAY_CONFIG['retry_delay_base']))
                        break

                    if res_json and isinstance(res_json, dict):
                        if res_json.get("resultbody", {}).get("job", {}).get("items"):
                            result.append(res_json)
                            jobs_count = len(res_json['resultbody']['job']['items'])
                            monitor.record_request(success=True, jobs_count=jobs_count)
                            logger.info(f"成功获取 {jobs_count} 条数据, 关键词: {kw}, 页码: {page}")
                        else:
                            logger.warning(f"无数据返回: {res_json}")
                            monitor.record_request(success=True, jobs_count=0)  # 记录空响应
                            # 如果是第一页就无数据，可能是被反爬虫了，增加延时
                            if page == 1:
                                logger.warning("第一页就无数据，可能触发反爬虫机制，增加延时")
                                monitor.record_request(success=False, error_type="anti_spider_suspected")
                                time.sleep(random.uniform(*DELAY_CONFIG['anti_spider_delay']))
                            break
                    else:
                        logger.error(f"获取数据失败: {res_json}, params: {user_params}")
                        break

                    df = pd.DataFrame(data=res_json["resultbody"]["job"]["items"])
                    df["search_kw"] = kw
                    df["search_city"] = province_codes.get(city, city)  

                    expected_columns = ['jobId','jobType','jobName','jobTags','workAreaCode','jobAreaCode','jobAreaString','hrefAreaPinYin','provideSalaryString','issueDateString','confirmDateString','workYear','workYearString','degreeString','industryType1','industryType2','industryType1Str','industryType2Str','major1Str','companyName','fullCompanyName','companyLogo','companyTypeString','companySizeString','companySizeCode','companyIndustryType1Str','companyIndustryType2Str','hrUid','hrName','smallHrLogoUrl','hrPosition','hrLabels','updateDateTime','lon','lat','jobHref','jobDescribe','companyHref','term','termStr','jobTagsForOrder','jobSalaryMax','jobSalaryMin','isReprintJob','applyTimeText','jobReleaseType','coId','search_kw','search_city']
                    existing_columns = [col for col in expected_columns if col in df.columns]
                    df = df[existing_columns]

                    # 数据预处理
                    list_columns = ['jobTags', 'hrLabels', 'jobTagsForOrder']
                    for col in list_columns:
                        if col in df.columns:
                            df[col] = df[col].apply(lambda x: ','.join(x) if isinstance(x, list) else (str(x) if pd.notnull(x) else None))

                    for col in ['jobSalaryMax', 'jobSalaryMin']:
                        if col in df.columns:
                            df[col] = df[col].replace('', None)
                            try:
                                df[col] = df[col].astype(float)
                            except Exception as e:
                                logger.warning(f"薪资字段转换异常: {e}")

                    # 数据库操作 - 使用DatabaseManager
                    mysql_config = config['mysql']
                    try:
                        # 使用DatabaseManager保存数据，启用重试机制
                        db_manager = DatabaseManager(mysql_config)
                        success = db_manager.save_dataframe(df, 'job_listings', if_exists='append', max_retries=5)
                        
                        if success:
                            logger.info(f"成功写入 {len(df)} 条数据到MySQL数据库, 关键词: {kw}, 页码: {page}")
                        else:
                            logger.error("数据库保存失败，跳过当前页面数据")
                            # 数据库保存失败时不中断整个抓取流程，继续下一页
                            monitor.record_request(success=False, error_type="database_save_failed")
                        
                    except Exception as e:
                        logger.error(f"数据库操作异常: {e}，跳过当前页面数据")
                        monitor.record_request(success=False, error_type=f"database_exception_{type(e).__name__}")
                        # 数据库异常时不中断整个抓取流程，继续下一页

                    page += 1

                    if page > max_page:
                        logger.info(f"关键词: {kw} 城市： {city} 达到最大页数 {max_page}，结束抓取")
                        break

                    # 每次抓取后随机休眠，根据页数动态调整延时
                    base_delay = random.uniform(*DELAY_CONFIG['base_page_delay'])
                    if page > 10:  # 页数较多时增加延时
                        base_delay += random.uniform(*DELAY_CONFIG['high_page_delay'])
                    sleep_time = int(base_delay)
                    logger.info(f"关键词: {kw} 城市： {city}，第 {page-1} 页抓取完成，休眠 {sleep_time} 秒")
                    time.sleep(sleep_time)
                # 记录城市任务完成情况
                if len(result) > 0:
                    monitor.record_city_completion(success=True)
                    logger.info(f"城市 {province_codes.get(city, city)} 关键词 {kw} 任务完成，共获取 {len(result)} 条数据")
                else:
                    monitor.record_city_completion(success=False)
                    logger.warning(f"城市 {province_codes.get(city, city)} 关键词 {kw} 任务失败，未获取到数据")
                
                # 每个城市/关键词任务完成后休眠，根据数据量动态调整
                city_delay = random.uniform(*DELAY_CONFIG['city_task_delay'])
                if len(result) == 0:  # 如果没有获取到数据，增加更长延时
                    city_delay += random.uniform(*DELAY_CONFIG['no_data_penalty'])
                    logger.warning(f"城市 {province_codes.get(city, city)} 关键词 {kw} 未获取到数据，增加延时")
                sleep_time = int(city_delay)
                logger.info(f"关键词: {kw} 城市： {city}  抓取任务完成，休眠 {sleep_time} 秒")
                time.sleep(sleep_time)
                
                # 每完成5个城市任务后生成监控报告
                if (monitor.stats['cities_completed'] + monitor.stats['cities_failed']) % 5 == 0:
                    monitor.log_report()
            sleep_time=random.randint(3, 30)
            logger.info(f"关键词: {kw} 的所有城市搜索任务已完成，休眠 {sleep_time} 秒")
            time.sleep(sleep_time)
        logger.info("本次搜索任务全部完成")
    except Exception as e:
        logger.exception(f"search 函数发生未捕获异常: {e}")


timezone = pytz.timezone("Asia/Shanghai")

# 随机生成每天下午6-9点之间的一个时间点
random_hour = random.randint(6, 9)
random_minute = random.randint(0, 59)
random_time = f"{random_hour:02d}:{random_minute:02d}"
logger.info(f"next running time (evening): {random_time}")

# 新增：随机生成每天晚上21-22点之间的一个时间点
night_hour = random.randint(20, 23)
night_minute = random.randint(0, 59)
night_time = f"{night_hour:02d}:{night_minute:02d}"
logger.info(f"next running time (night): {night_time}")

def scheduled_search():
    logger.info("开始执行搜索任务")
    try:
        search()
    except Exception as e:
        logger.error(f"搜索任务执行异常: {e}")
        monitor.record_request(success=False, error_type=f"search_task_exception_{type(e).__name__}")
    finally:
        # 任务结束时生成最终报告
        monitor.log_report()
        # 检查是否存在logs文件夹，如果不存在则创建
        logs_dir = 'logs'
        if not os.path.exists(logs_dir):
            try:
                os.makedirs(logs_dir)
                logger.info(f"创建logs目录: {logs_dir}")
            except Exception as e:
                logger.error(f"创建logs目录失败: {e}")
                return
        # 保存详细报告到文件
        report_file = f"{logs_dir}/crawler_report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        try:
            monitor.save_report(report_file)
            logger.info(f"监控报告已保存到: {report_file}")
        except Exception as e:
            logger.error(f"保存监控报告失败: {e}")

# 在每天下午6-9点之间的随机时间点启动任务
scheduler = schedule.Scheduler()
scheduler.every().day.at(random_time, timezone).do(scheduled_search)
# 在每天晚上21-22点之间的随机时间点启动任务
scheduler.every().day.at(night_time, timezone).do(scheduled_search)

logger.info("程序启动，配置定时任务")

# 启动认证管理器的自动更新功能
auth_manager = get_auth_manager()
auth_manager.start_auto_update()
logger.info("认证管理器自动更新已启动")

while True:
    try:
        scheduler.run_pending()
    except Exception as e:
        logger.exception(f"定时任务运行异常: {e}")
    time.sleep(0.5)
