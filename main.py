import time
import pandas as pd
import random
from jobSearch import JobSearch
from feishu_doc import Feishu
import logging
import configparser
import schedule
import pytz
import datetime
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

# 读取配置文件
config = configparser.ConfigParser()
config.read("config.ini")


def search():
    # 获取配置值
    app_id = config["tokens"]["app_id"]
    app_secret = config["tokens"]["app_secret"]

    db_id = config["db_info"]["db_id"]
    table_id = config["db_info"]["table_id"]

    # kws=['data analyst','business analyst','crm analyst','sap analyst','BI','data scientist','data engineer']
    kws = config["job_search"]["kws"].split(",")
    cities = config["job_search"]["cities"].split(",")

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
        result = []
        page = 1
        max_page = 3

        # initialize search engine
        jobs = JobSearch(url, initial_cookies, headers)

        while True:
            print("Page Num: ", page)
            user_params = {
                "api_key": "51job",
                "timestamp": f"{int(time.time())}",
                "keyword": kw,
                "searchType": "2",
                "function": "",
                "industry": "",
                "jobArea": f"{'%'.join(cities)}",
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

            res_json = jobs.get_jobs_json(params=user_params)

            if res_json is not str:
                if res_json["resultbody"]["job"]["items"]:

                    result.append(res_json)
            else:
                print(res_json)

            page += 1

            if page > max_page:
                break

            time.sleep(random.randint(1, 8))

        df = pd.DataFrame()
        for page in result:
            df = df.append(pd.DataFrame(data=page["resultbody"]["job"]["items"]))
        df["search_kw"] = kw

        # 写入飞书多维表
        fs = Feishu(app_id=app_id, app_secret=app_secret)

        fields = [
            {"fields": {key: str(value) for key, value in row.items()}}
            for row in df.to_dict("records")
        ]

        for i in range(1, len(fields) // 400 + 2):
            res = fs.add_records(db_id, table_id, fields[400 * (i - 1) : 400 * i])

            time.sleep(0.3)

        time.sleep(random.randint(1, 30))


timezone = pytz.timezone("Asia/Shanghai")

#随机生成每天下午5-8点之间的一个时间点
random_hour = random.randint(23, 24)
random_minute = random.randint(0, 10)
random_time = f"{random_hour:02d}:{random_minute:02d}"



# 在每天下午5-8点之间的随机时间点启动任务
# scheduler=schedule.Scheduler()
# scheduler.every().day.at(random_time,timezone).do(search)

# while True:
#     scheduler.run_pending()
#     time.sleep(0.5)


search()

