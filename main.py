import time
import pandas as pd
import random
from jobSearch import JobSearch
import logging
import configparser
import schedule
import pytz
import datetime
from sqlalchemy import create_engine
from logging.handlers import TimedRotatingFileHandler


# 配置日志记录器
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 创建 TimedRotatingFileHandler，每 6 小时滚动一次日志文件
log_handler = TimedRotatingFileHandler('app.log', when='H', interval=6, backupCount=4)
log_handler.setLevel(logging.DEBUG)


# 配置日志格式
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_handler.setFormatter(log_formatter)

# 添加处理器到记录器
logger.addHandler(log_handler)

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

        if '000000' in cities:
            logger.info("检测到全省搜索，使用所有省级代码")
            cities = list(province_codes.keys())

        url = "https://we.51job.com/api/job/search-pc"

        initial_cookies = {
            "privacy": str(int(time.time())),
            "guid": "21ae369c1fecc95608a454bacdd16b41",
            "acw_tc": "ac11000117130121464015687e00d6b80852550fe03e0fd08cf69bf654d5a3",
            "JSESSIONID": "2F0AFC4C5819D899810516F3424C7A87",
            "NSC_ohjoy-bmjzvo-200-159": "ffffffffc3a0d42e45525d5f4f58455e445a4a423660",
        }

        headers = {
            "Accept": "application/json,text/plain,*/*",
            "Accept-Encoding": "gzip,deflate,br,zstd",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "DNT": "1",
            "From-Domain": "51job_web",
            "Host": "we.51job.com",
            "Referer": "https://we.51job.com/pc/search?jobArea=000000&keyword=%E6%95%B0%E6%8D%AE%E5%88%86%E6%9E%90&searchType=2",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0(Macintosh;IntelMacOSX10_15_7)AppleWebKit/537.36(KHTML,likeGecko)Chrome/123.0.0.0Safari/537.36",
            "account-id": "",
            "partner": "",
            "property": "%7B%22partner%22%3A%22%22%2C%22webId%22%3A2%2C%22fromdomain%22%3A%2251job_web%22%2C%22frompageUrl%22%3A%22https%3A%2F%2Fwe.51job.com%2F%22%2C%22pageUrl%22%3A%22https%3A%2F%2Fwe.51job.com%2Fpc%2Fsearch%3FjobArea%3D000000%26keyword%3D%25E6%2595%25B0%25E6%258D%25AE%25E5%2588%2586%25E6%259E%2590%26searchType%3D2%22%2C%22identityType%22%3A%22%22%2C%22userType%22%3A%22%22%2C%22isLogin%22%3A%22%E5%90%A6%22%2C%22accountid%22%3A%22%22%2C%22keywordType%22%3A%22%22%7D",
            "sec-ch-ua": '"GoogleChrome";v="123","Not:A-Brand";v="8","Chromium";v="123"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sign": "f858c5f418f30e4dd65f3c7f86d03a8e87e87302695161c82a9b37f04d790287",
            "user-token": "",
            "uuid": "21ae369c1fecc95608a454bacdd16b4",
        }

        for kw in kws:
            for city in cities:
                logger.info(f"开始搜索关键词：{kw} 地区：{city}")
                result = []
                page = 1
                max_page = config.getint("job_search", "max_page", fallback=10)

                jobs = JobSearch(url, initial_cookies, headers)

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
                        "sortType": "1",
                        "pageNum": page,
                        "pageSize": "20",
                        "source": "1",
                        "accountId": "",
                        "pageCode": "sou%7Csou%7Csoulb",
                        "userLonLat": "",
                    }

                    try:
                        res_json = jobs.get_jobs_json(params=user_params)
                    except Exception as e:
                        logger.error(f"请求接口异常: {e}, params: {user_params}")
                        break

                    if res_json and isinstance(res_json, dict):
                        if res_json.get("resultbody", {}).get("job", {}).get("items"):
                            result.append(res_json)
                            logger.info(f"成功获取 {len(res_json['resultbody']['job']['items'])} 条数据, 关键词: {kw}, 页码: {page}")
                        else:
                            logger.warning(f"无数据返回: {res_json}")
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

                    mysql_config = config['mysql']
                    try:
                        engine = create_engine(
                            f"mysql+pymysql://{mysql_config['user']}:{mysql_config['password']}@{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}?charset=utf8mb4"
                        )
                    except Exception as e:
                        logger.error(f"数据库连接失败: {e}")
                        break

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

                    try:
                        df.to_sql('job_listings', con=engine, if_exists='append', index=False, chunksize=1000, method='multi')
                        logger.info(f"成功写入 {len(df)} 条数据到MySQL数据库, 关键词: {kw}, 页码: {page}")
                    except Exception as e:
                        logger.error(f"写入数据库失败: {e}")
                        break

                    page += 1

                    if page > max_page:
                        logger.info(f"关键词: {kw} 城市： {city} 达到最大页数 {max_page}，结束抓取")
                        break

                    sleep_time = random.randint(3, 15)
                    logger.info(f"关键词: {kw} 城市： {city}，第 {page-1} 页抓取完成，休眠 {sleep_time} 秒")
                    time.sleep(sleep_time)
                sleep_time=random.randint(3, 30)
                logger.info(f"关键词: {kw} 城市： {city}  抓取任务完成，休眠 {sleep_time} 秒")
                time.sleep(sleep_time)
            sleep_time=random.randint(3, 30)
            logger.info(f"关键词: {kw} 的所有城市搜索任务已完成，休眠 {sleep_time} 秒")
            time.sleep(sleep_time)
        logger.info("本次搜索任务全部完成")
    except Exception as e:
        logger.exception(f"search 函数发生未捕获异常: {e}")


timezone = pytz.timezone("Asia/Shanghai")

#随机生成每天下午5-8点之间的一个时间点
random_hour = random.randint(6,9)
random_minute = random.randint(0, 59)
random_time = f"{random_hour:02d}:{random_minute:02d}"
logger.info(f"next running time: {random_time}")



# 在每天下午5-8点之间的随机时间点启动任务
scheduler=schedule.Scheduler()
scheduler.every().day.at(random_time,timezone).do(search)

logger.info("程序启动，配置定时任务")

while True:
    try:
        scheduler.run_pending()
    except Exception as e:
        logger.exception(f"定时任务运行异常: {e}")
    time.sleep(0.5)
