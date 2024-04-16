 
import requests
import json

class Feishu:
    def __init__(self,token) -> None:
        self.token=f'Bearer {token}'

    def create_db(self,folder_id,title):

        url = "https://open.feishu.cn/open-apis/bitable/v1/apps"
        payload = json.dumps({
            "folder_token": folder_id,
            "name": title
        })


        headers = {
        'Content-Type': 'application/json',
        'Authorization': self.token
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        print(response.text)


    def create_table(self,db_id,table_name,default_view_name,fields:list):

        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{db_id}/tables"
        payload = json.dumps({
            "table": {
                "default_view_name": default_view_name,         
                "fields": fields,
                "name": table_name
            }
        })

        # fields sturcture
        # [
        #             {
        #                 "field_name": "多行文本",
        #                 "type": 1
        #             }
        #         ]


        headers = {
        'Content-Type': 'application/json',
        'Authorization': self.token
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        return response.json()



    def add_record(self,db_id,table_id,fields:dict):

        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{db_id}/tables/{table_id}/records"
        payload = json.dumps({
            "fields": fields
        })


        headers = {
        'Content-Type': 'application/json',
        'Authorization': self.token
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        return response.json()
    

    def add_records(self,db_id,table_id,fields:list):

        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{db_id}/tables/{table_id}/records/batch_create"
        payload = json.dumps({
            "records": fields
        })


        headers = {
        'Content-Type': 'application/json',
        'Authorization': self.token
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        return response.json()
