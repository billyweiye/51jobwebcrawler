import requests
import json
import time


class Notion:
    def __init__(self,token) -> None:
        self.token=token

    def create_db(self,parent_page_id,title, properties:dict ):

        p={}
        for k,v in properties.items():
            p[k]={v:{}}
        
        #add default properties
        p['id']={"unique_id":{}}
        p['created_time']={"created_time":{}}
        p['created_by']={'created_by':{}}

        url = "https://api.notion.com/v1/databases/"

        payload = json.dumps({
        "parent": {
            "type": "page_id",
            "page_id": parent_page_id
        },
        "title": [
            {
            "type": "text",
            "text": {
                "content": title,
                "link": None
            }
            }
        ],
        "properties": p
        })
        headers = {
        'Content-Type': 'application/json',
        'Notion-Version': '2022-02-22',
        'Authorization': self.token,
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        return response.json()
    
    def convert_df_to_notion_properties(self,df):
        dict_list = []
        for index, row in df.iterrows():
            item_dict = {}
            for column_name, value in row.items():
                item_dict[column_name] = {"rich_text": [{"text": {"content": str(value)}}]}
            dict_list.append(item_dict)
        return dict_list
    
    def convert_df_to_notion_block(self,df):
        dict_list = []
        for index, row in df.iterrows():
            item_list = []
            for column_name, value in row.items():
                item_list.append({
                                    "object": "block",
                                    "paragraph": {
                                        "rich_text": [
                                            {
                                                "text": {
                                                    "content": str(value)
                                                }
                                            }
                                        ]
                                    }
                                })
            dict_list.append(item_list)
        return dict_list
    
    def add_content(self,database_id,properties:dict,body_content:list):

        url = "https://api.notion.com/v1/pages/"

        #check if title in properties, if not, change the first key into title type
        property_types=[next(iter(t.keys())) for t in list(properties.values())]
        if not 'title' in property_types:
            properties[next(iter(properties))]['title']=properties[next(iter(properties))].pop(next(iter(properties[next(iter(properties))])))

        payload = json.dumps({
        "parent": {
            "database_id": database_id
        },
        "properties": properties,
        "children":body_content
        })


        headers = {
        'Content-Type': 'application/json',
        'Notion-Version': '2022-02-22',
        'Authorization': self.token
        }

        status_code=''
        max_retry=10
        while status_code!=200:
            max_retry-=1
            if max_retry<0:
                raise Exception("MaxRetry")

            response = requests.request("POST", url, headers=headers, data=payload) 

            status_code=response.status_code

            if response.status_code==200:
                print("content added")
                break
            elif response.status_code==429:
                time.sleep(1)
            else:
                print(f"Failed: {response.status_code}")
                print(response.json()['message'])
        
        return response.json()
