#impport modules
import requests
import re
from bs4 import BeautifulSoup
import os
import pandas as pd
import time
from urllib.request import quote



#get the html text from the webpage
def getHTMLText(url):
    try:
        r=requests.get(url,timeout=30)
        r.raise_for_status()
        r.encoding=r.apparent_encoding
        html=r.text
        print('html is ready')
        return html
    except:
        print('get html text failed')

#get the code of the city 
def citycoder(city):
    url='https://js.51jobcdn.com/in/js/2016/layer/area_array_c.js'
    r= requests.get(url,timeout=30)
    fl=r.text
    geocode = re.findall('"([0-9]+)":"{}"'.format(city),fl)[0]
    return geocode

#soup the html file
def soupmaker(html):
    soup = BeautifulSoup(html,'html.parser')
    return soup 
    print('soup is ready')

#extract data we want from the soup
def listfiller(tags): #get the information we need
    jobtitle=list()
    joburl=list()
    company=list()
    location=list()
    salary=list()
    for tag in tags:
        try:
            if 't1' in tag.get('class',None):
                jobtitle.append(tag.find('a').string.strip())
                joburl.append(tag.find('a').get('href',None))
            if 't2' in tag.get('class',None):
                company.append(tag.string)
            if 't3' in tag.get('class',None):
                location.append(tag.string)
            if 't4' in tag.get('class',None):
                salary.append(tag.string)
            else:
                continue
        except:
            continue
    company=company[1:]
    location=location[1:]
    salary=salary[1:]
    print('list has been filled')
    return jobtitle,joburl,company,location,salary




jobtitle=[]
joburl=[]
company=[]
location=[]
salary=[]

kw = quote(input('职位名称').strip())
dump=int(int(input('爬取多少页').strip())+1)
geocode=citycoder(str(input('城市').strip()))
for page in range(1,dump):
    url='https://search.51job.com/list/{},000000,0000,00,9,99,{},2,{}.html'.format(geocode,kw,page)
    print(url)
    html = getHTMLText(url)
    soup = soupmaker(html)
    
    tags=soup(class_=['t1','t2','t3','t4'])
    L=listfiller(tags)

    jobtitle += L[0]
    joburl+=L[1]
    company+=L[2]
    location+=L[3]
    salary+=L[4]

    time.sleep(2) # Slow things down so as to not hammer Wikipedia's servers




raw = {
    'jobtitle' : jobtitle,
    'company': company,
    'location': location,
    'salary': salary,
    'link': joburl
}

df = pd.DataFrame(raw)
print('dataframe has been created')

df.head(10)
