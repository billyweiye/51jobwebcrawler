# 51Job 招聘数据爬虫

一个用于从51job.com抓取招聘信息的Python爬虫工具，支持多城市、多关键词的批量数据采集，具备完善的数据库存储和监控功能。

## ✨ 功能特性

- 🔍 **智能搜索**: 支持多关键词、多城市的组合搜索
- 🔄 **自动认证**: 自动管理cookies和请求头，模拟真实浏览器行为
- 💾 **数据存储**: 支持MySQL数据库存储，包含完整的数据预处理
- 🛡️ **稳定可靠**: 内置重试机制、连接池管理和异常处理
- 📊 **实时监控**: 数据库健康监控和爬虫状态监控
- ⚡ **高性能**: 支持并发请求和智能延时控制
- 📝 **详细日志**: 完整的日志记录和错误追踪

## 🛠️ 技术栈

- **Python 3.11+**
- **数据库**: MySQL 8.0+
- **HTTP库**: requests
- **数据处理**: pandas
- **数据库连接**: pymysql, sqlalchemy
- **日志**: logging with TimedRotatingFileHandler

## 📦 安装

1. 克隆项目
```bash
git clone https://github.com/your-username/51jobwebcrawler.git
cd 51jobwebcrawler
```

2. 安装依赖
```bash
pip install -r requirements.txt
```

3. 配置数据库
```bash
# 复制配置文件模板
cp config.ini.example config.ini
# 编辑配置文件，填入你的数据库信息
```

## ⚙️ 配置

创建 `config.ini` 文件并配置以下信息：

```ini
[mysql]
host = your_mysql_host
port = 3306
user = your_username
password = your_password
database = your_database

[crawler]
keywords = 数据分析师,Python开发,机器学习
cities = 020000,010000,030200  # 广州,北京,深圳
max_pages = 10
delay_range = 1,3
```

## 🚀 使用方法

### 基本使用

```bash
# 运行主程序
python main.py
```

### 数据库健康监控

```bash
# 检查数据库连接
python database_health_monitor.py
```

### 测试功能

```bash
# 运行测试脚本
python test.py
```

## 📊 数据库结构

```sql
CREATE TABLE IF NOT EXISTS job_listings (
    job_id VARCHAR(50) PRIMARY KEY,        -- 职位唯一标识
    job_title VARCHAR(200) NOT NULL,       -- 职位名称
    company_name VARCHAR(200) NOT NULL,    -- 公司名称
    company_type VARCHAR(50),              -- 公司类型
    company_size VARCHAR(50),              -- 公司规模
    
    -- 薪资信息
    salary_min INT,                        -- 最低薪资（单位：元）
    salary_max INT,                        -- 最高薪资
    salary_range VARCHAR(50),              -- 薪资范围文本
    
    -- 地理位置
    location_code VARCHAR(20) NOT NULL,    -- 城市编码
    province_code VARCHAR(20),             -- 省份编码
    city_name VARCHAR(50),                 -- 城市名称
    district VARCHAR(50),                  -- 区县名称
    
    -- 职位详情
    job_describe TEXT,                     -- 职位描述
    work_experience VARCHAR(50),           -- 工作经验要求
    degree_requirement VARCHAR(50),        -- 学历要求
    
    -- 时间信息
    publish_date DATETIME,                 -- 发布时间
    update_date DATETIME,                  -- 更新时间
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 其他信息
    job_url TEXT,                          -- 职位链接
    company_url TEXT,                      -- 公司链接
    
    INDEX idx_company (company_name),
    INDEX idx_location (location_code),
    INDEX idx_salary (salary_min, salary_max),
    INDEX idx_publish_date (publish_date)
);
```

## 📁 项目结构

```
51jobwebcrawler/
├── main.py                    # 主程序入口
├── jobSearch.py              # 核心爬虫逻辑
├── auth_manager.py           # 认证管理
├── cookie_manager.py         # Cookie管理
├── database_manager.py       # 数据库管理
├── database_health_monitor.py # 数据库监控
├── crawler_config.py         # 爬虫配置
├── crawler_monitor.py        # 爬虫监控
├── requirements.txt          # 依赖包列表
├── config.ini.example        # 配置文件模板
└── docs/                     # 文档目录
    ├── AUTH_MANAGER_GUIDE.md
    ├── DATABASE_IMPROVEMENTS.md
    └── SECURITY_CHECKLIST.md
```

## 🔧 高级功能

### 自定义搜索参数

```python
from jobSearch import JobSearch

searcher = JobSearch()
result = searcher.get_job_json(
    keyword="Python开发",
    job_area="020000",  # 广州
    page_num=1
)
```

### 数据库批量操作

```python
from database_manager import DatabaseManager

db_manager = DatabaseManager(mysql_config)
success = db_manager.save_dataframe(df, 'job_listings', if_exists='append')
```

## 🛡️ 安全说明

- 所有敏感配置信息请存放在 `config.ini` 文件中
- 不要将包含真实数据库密码的配置文件提交到版本控制
- 建议在生产环境中使用环境变量管理敏感信息
- 详细安全指南请参考 [SECURITY_CHECKLIST.md](SECURITY_CHECKLIST.md)

## 📝 日志

程序运行时会生成详细的日志文件：
- `app.log` - 主程序日志
- 日志文件会自动轮转，保留最近的备份

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📄 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件

## ⚠️ 免责声明

本工具仅供学习和研究使用，请遵守相关网站的robots.txt和使用条款。使用者需要自行承担使用本工具的风险和责任。