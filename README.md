# 51jobwebcrawler
crawl job openings from 51job.com


## 环境
- python 3.6
- scrapy 1.4.0
- selenium 3.141.0
- chromedriver 79.0.3945.36
- redis 5.0.3
- mysql 8.0.17
## 运行
```
scrapy crawl 51job
``` 


```sql
CREATE TABLE IF NOT EXISTS job_postings (
    job_id TEXT PRIMARY KEY NOT NULL,      -- 职位唯一标识
    job_title TEXT NOT NULL,               -- 职位名称
    company_name TEXT NOT NULL,            -- 公司名称
    company_type TEXT,                     -- 公司类型（外资/国企等）
    company_size TEXT,                     -- 公司规模
    
    -- 薪资信息
    salary_min INTEGER,                    -- 最低薪资（单位：元）
    salary_max INTEGER,                    -- 最高薪资
    salary_range TEXT,                     -- 原薪资范围文本
    
    -- 地理位置
    location_code TEXT NOT NULL,           -- 城市编码
    province_code TEXT,                    -- 省份编码
    city_code TEXT,                        -- 城市编码
    district TEXT,                         -- 区县名称
    longitude REAL,                        -- 经度
    latitude REAL,                         -- 纬度
    
    -- 职位详情
    job_describe TEXT NOT NULL,            -- 职位描述
    job_requirements TEXT,                 -- 任职要求
    work_experience TEXT,                   -- 工作经验要求
    degree_requirement TEXT,                -- 学历要求
    
    -- 分类标签
    industry_type TEXT,                     -- 行业类型
    job_tags TEXT,                          -- 标签列表（JSON数组）
    welfare_tags TEXT,                      -- 福利标签（JSON数组）
    
    -- 时间信息
    publish_date TEXT NOT NULL,            -- 发布时间（ISO格式）
    update_date TEXT,                      -- 最后更新时间
    created_at TEXT DEFAULT CURRENT_TIMESTAMP, -- 数据创建时间
    
    -- 其他元数据
    job_url TEXT NOT NULL,                 -- 职位详情页URL
    company_url TEXT,                      -- 公司详情页URL
    raw_data TEXT NOT NULL,                -- 原始JSON数据
    
    -- 索引配置
    INDEX idx_company (company_name),
    INDEX idx_location (location_code),
    INDEX idx_salary_range (salary_min, salary_max)
);
```