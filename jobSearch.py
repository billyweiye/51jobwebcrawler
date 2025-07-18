#impport modules
import requests
import re
import certifi
import time
import random
from acwCookie import getAcwScV2
import logging
from crawler_config import DELAY_CONFIG, RETRY_CONFIG, DATA_VALIDATION

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
        import time
        import random
        
        max_retries = RETRY_CONFIG['max_retries']
        for attempt in range(max_retries):
            try:
                logger.info(f"第 {attempt + 1} 次尝试请求，参数: {params}")
                
                # 每次重试时随机延时，避免请求过于频繁
                if attempt > 0:
                    delay = random.uniform(*DELAY_CONFIG['retry_delay_base']) * (attempt + 1)
                    logger.info(f"重试前延时 {delay:.1f} 秒")
                    time.sleep(delay)
                
                response = requests.get(self.url, params=params, headers=self.headers, cookies=self.cookies, timeout=RETRY_CONFIG['timeout'], verify=certifi.where())
                logger.info(f"请求URL: {response.url}，状态码: {response.status_code}")
                
                # 处理acw_sc__v2 cookie的逻辑
                pattern = r"var arg1='([A-F0-9]+)';"
                arg1 = re.search(pattern, response.text)
                if arg1:
                    logger.info(f"retry:{attempt + 1} set_cookies，设置 acw_sc__v2")
                    arg1 = arg1.group(1)
                    self.cookies['acw_sc__v2'] = getAcwScV2(arg1)
                    logger.debug(f"已设置 acw_sc__v2，重新请求")
                    continue
                
                if response.status_code == 200:
                    # 检查响应内容是否有效
                    if response.text.strip() and not 'error' in response.text.lower():
                        logger.info(f"请求成功，状态码: {response.status_code}")
                        return response
                    else:
                        logger.warning(f"响应内容异常: {response.text[:200]}")
                        continue
                else:
                    logger.warning(f"请求失败，状态码: {response.status_code}, 响应: {response.text[:200]}")
                    
                    # 对于特定错误码增加更长延时
                    if response.status_code in RETRY_CONFIG['anti_spider_codes']:
                        logger.warning("遇到反爬虫限制，增加延时")
                        time.sleep(random.uniform(*DELAY_CONFIG['anti_spider_delay']))
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"请求异常 (尝试 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(*DELAY_CONFIG['retry_delay_base']) * (attempt + 1))  # 随机指数退避
                    
        logger.error(f"所有重试都失败了，参数: {params}")
        raise Exception("Max Retry Exceeded!")

    def get_jobs_json(self, params):
        try:
            logger.info(f"开始获取职位JSON数据，参数: {params}")
            response = self.search_jobs(params)
            if response.status_code == 200 and response.text.strip():
                try:
                    json_data = response.json()
                    # 检查返回的数据是否有效
                    if (json_data.get('status') == DATA_VALIDATION['success_status'] and 
                        json_data.get('resultbody', {}).get('job', {}).get('items')):
                        logger.info("获取JSON数据成功")
                        return json_data
                    else:
                        logger.warning(f"API返回无数据或状态异常: {json_data}")
                        return None
                except ValueError as e:
                    logger.error(f"JSON解析失败: {e}, 响应内容: {response.text[:200]}")
                    return None
            else:
                logger.error(f"请求失败，状态码: {response.status_code}, 内容: {response.text[:200]}")
                return None
        except Exception as e:
            logger.error(f"获取职位数据异常: {e}, 响应内容: {getattr(locals().get('response', None), 'text', '')[:200]}")
            return None

