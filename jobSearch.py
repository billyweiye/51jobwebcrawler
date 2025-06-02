#impport modules
import requests
import re
from acwCookie import getAcwScV2
import certifi 
import logging

logger=logging.getLogger(__name__)

class JobSearch:
    def __init__(self, url, cookies,headers):
        self.url = url
        self.headers = headers
        self.cookies=cookies
        self.max_retry=10

    #get the code of the city 
    def citycoder(city):
        url='https://js.51jobcdn.com/in/js/2016/layer/area_array_c.js'
        r= requests.get(url,timeout=30,verify=certifi.where())
        fl=r.text
        geocode = re.findall('"([0-9]+)":"{}"'.format(city),fl)[0]
        return geocode




    def search_jobs(self, params):
        self.max_retry=self.max_retry-1
        if self.max_retry>=0:
            response = requests.get(self.url,cookies=self.cookies, headers=self.headers, params=params,verify=certifi.where())
            pattern = r"var arg1='([A-F0-9]+)';"
            # Search for the pattern in the JavaScript code
            arg1 = re.search(pattern, response.text)
            if self.cookies.get('acw_sc__v2') is None and arg1 is None:
                raise Exception("Failed to Get arg1")
            if arg1:
                logger.info(f"retry:{10-self.max_retry} set_cookies")
                arg1=arg1.group(1)
                self.cookies['acw_sc__v2']=getAcwScV2(arg1)
                response = self.search_jobs(params)
            else:
                self.max_retry=10
        else:
            raise Exception("Max Retry Exceeded!")
        

        return response

    def get_jobs_json(self, params):
        try:
            response = self.search_jobs(params)
            if response.status_code == 200 and response.text.strip():
                return response.json()
            else:
                logger.error(f"请求失败，状态码: {response.status_code}, 内容: {response.text[:200]}")
                return None
        except Exception as e:
            logger.error(f"解析JSON失败: {e}, 响应内容: {getattr(response, 'text', '')[:200]}")
            return None

