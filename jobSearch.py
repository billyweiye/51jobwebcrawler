#impport modules
import requests
import re
from acwCookie import getAcwScV2
import certifi 
import logging

logger=logging.getLogger(__name__)

class JobSearch:
    def __init__(self, url, cookies, headers):
        self.url = url
        self.headers = headers
        self.cookies = cookies
        self.max_retry = 10
        logger.info(f"JobSearch 实例初始化: url={url}")

    # get the code of the city 
    def citycoder(city):
        url = 'https://js.51jobcdn.com/in/js/2016/layer/area_array_c.js'
        try:
            logger.info(f"请求城市编码: {city}")
            r = requests.get(url, timeout=30, verify=certifi.where())
            fl = r.text
            geocode = re.findall('"([0-9]+)":"{}"'.format(city), fl)[0]
            logger.info(f"获取城市编码成功: {city} -> {geocode}")
            return geocode
        except Exception as e:
            logger.error(f"获取城市编码失败: {city}, 错误: {e}")
            return None

    def search_jobs(self, params):
        self.max_retry = self.max_retry - 1
        logger.debug(f"开始请求，剩余重试次数: {self.max_retry}")
        if self.max_retry >= 0:
            try:
                response = requests.get(self.url, cookies=self.cookies, headers=self.headers, params=params, verify=certifi.where())
                logger.info(f"请求URL: {response.url}，状态码: {response.status_code}")
            except Exception as e:
                logger.error(f"请求异常: {e}")
                raise

            pattern = r"var arg1='([A-F0-9]+)';"
            arg1 = re.search(pattern, response.text)
            if self.cookies.get('acw_sc__v2') is None and arg1 is None:
                logger.error("未获取到 arg1，无法设置 acw_sc__v2")
                raise Exception("Failed to Get arg1")
            if arg1:
                logger.info(f"retry:{10 - self.max_retry} set_cookies，设置 acw_sc__v2")
                arg1 = arg1.group(1)
                self.cookies['acw_sc__v2'] = getAcwScV2(arg1)
                logger.debug(f"已设置 acw_sc__v2，重新请求")
                response = self.search_jobs(params)
            else:
                self.max_retry = 10
        else:
            logger.error("超过最大重试次数，抛出异常")
            raise Exception("Max Retry Exceeded!")

        return response

    def get_jobs_json(self, params):
        try:
            logger.info(f"开始获取职位JSON数据，参数: {params}")
            response = self.search_jobs(params)
            if response.status_code == 200 and response.text.strip():
                logger.info("获取JSON数据成功")
                return response.json()
            else:
                logger.error(f"请求失败，状态码: {response.status_code}, 内容: {response.text[:200]}")
                return None
        except Exception as e:
            logger.error(f"解析JSON失败: {e}, 响应内容: {getattr(locals().get('response', None), 'text', '')[:200]}")
            return None

