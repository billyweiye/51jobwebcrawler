import requests
import re
from typing import Dict, Any
from acwCookie import getAcwScV2
import certifi

class JobSpider:
    def __init__(self, base_url: str, cookies: Dict, headers: Dict):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.cookies.update(cookies)
        self.session.headers.update(headers)
        self.max_retries = 5  # 可配置化参数
        self.timeout = 30     # 可配置化参数

    @staticmethod
    def get_city_code(city: str) -> str:
        """优化城市编码获取逻辑"""
        url = f'https://js.51jobcdn.com/in/js/2016/layer/area_array_c.js?t={int(time.time())}'
        response = requests.get(url, timeout=30, verify=certifi.where())
        return re.search(fr'"(\d+)":"{re.escape(city)}"', response.text).group(1)

    def fetch_jobs(self, params: Dict) -> Dict:
        """带重试机制的请求方法"""
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(
                    self.base_url,
                    params=params,
                    timeout=self.timeout,
                    verify=certifi.where()
                )
                response.raise_for_status()
                
                if 'acw_sc__v2' not in self.session.cookies:
                    if arg1 := re.search(r"var arg1='([A-F0-9]+)';", response.text):
                        self.session.cookies['acw_sc__v2'] = getAcwScV2(arg1.group(1))
                        return self.fetch_jobs(params)  # 递归重试
                
                return response.json()
            
            except (requests.exceptions.RequestException, ValueError) as e:
                if attempt == self.max_retries - 1:
                    raise RuntimeError(f"请求失败: {str(e)}")
                wait_time = 2 ** attempt  # 指数退避策略
                time.sleep(wait_time)