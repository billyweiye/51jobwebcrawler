#impport modules
import requests
import re
from acwCookie import getAcwScV2




class JobSearch:
    def __init__(self, url, cookies,headers):
        self.url = url
        self.headers = headers
        self.cookies=cookies
        self.max_retry=10

    #get the code of the city 
    def citycoder(city):
        url='https://js.51jobcdn.com/in/js/2016/layer/area_array_c.js'
        r= requests.get(url,timeout=30,verify=False)
        fl=r.text
        geocode = re.findall('"([0-9]+)":"{}"'.format(city),fl)[0]
        return geocode




    def search_jobs(self, params):
        self.max_retry=self.max_retry-1
        if self.max_retry>=0:
            response = requests.get(self.url,cookies=self.cookies, headers=self.headers, params=params,verify=False)
            pattern = r"var arg1='([A-F0-9]+)';"
            # Search for the pattern in the JavaScript code
            arg1 = re.search(pattern, response.text)
            if self.cookies.get('acw_sc__v2') is None and arg1 is None:
                raise Exception("Failed to Get arg1")
            if arg1:
                print(f"retry:{10-self.max_retry} set_cookies")
                arg1=arg1.group(1)
                self.cookies['acw_sc__v2']=getAcwScV2(arg1)
                response = self.search_jobs(params)
            else:
                self.max_retry=10
        else:
            raise Exception("Max Retry Exceeded!")
        

        return response

    def get_jobs_json(self,params):
        return self.search_jobs(params).json()
    
