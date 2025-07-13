# 51job 高可维护性爬虫系统

一个专为 51job.com 设计的高可维护性、可扩展的招聘信息爬虫系统。

## 🎯 项目概述

本项目是一个企业级的招聘信息爬虫系统，专门针对前程无忧（51job.com）网站进行数据采集。系统采用模块化设计，具备高可维护性、可扩展性和健壮性。

## 🔍 目标网站分析

### 网站基本信息
- **目标网站**: [前程无忧 (51job.com)](https://51job.com/)
- **主要API**: `https://we.51job.com/api/job/search-pc`
- **数据格式**: JSON API响应
- **反爬机制**: 
  - acw_sc__v2 动态Cookie验证
  - 请求频率限制
  - User-Agent检测
  - IP访问限制

### 数据结构分析
- **职位列表**: 通过搜索API获取分页数据
- **关键字段**: jobId, jobName, companyName, provideSalaryString, jobAreaString
- **分页机制**: pageNum参数控制，每页最多20条记录
- **地区编码**: 使用6位数字编码标识不同城市/省份

## 🏗️ 解决方案架构

### 技术栈选择
- **核心框架**: Scrapy (企业级爬虫框架)
- **轻量请求**: Requests + BeautifulSoup (简单页面)
- **数据存储**: MySQL + SQLAlchemy ORM
- **任务调度**: APScheduler (替代简单的schedule)
- **配置管理**: YAML配置文件
- **日志系统**: 结构化日志 + 文件轮转

### 核心特性
1. **健壮性设计**
   - 指数退避重试机制
   - 多层异常处理
   - 请求超时控制
   - 数据验证与清洗

2. **反爬对策**
   - 用户代理轮换池
   - 代理IP支持
   - 智能延迟控制
   - Cookie管理机制

3. **可维护性**
   - 模块化代码结构
   - 配置文件驱动
   - 完整的日志记录
   - 单元测试覆盖

4. **可扩展性**
   - 插件化数据处理
   - 多种存储后端支持
   - 分布式部署就绪
   - 监控指标接口

## 📁 项目结构

```
51job-crawler/
├── crawler/                    # 爬虫核心模块
│   ├── __init__.py
│   ├── spiders/               # 爬虫实现
│   │   ├── __init__.py
│   │   ├── base_spider.py     # 基础爬虫类
│   │   └── job_spider.py      # 职位爬虫
│   ├── items.py               # 数据模型定义
│   ├── pipelines.py           # 数据处理管道
│   ├── middlewares.py         # 中间件
│   └── settings.py            # Scrapy配置
├── utils/                     # 工具模块
│   ├── __init__.py
│   ├── cookie_handler.py      # Cookie处理
│   ├── proxy_manager.py       # 代理管理
│   ├── user_agent.py          # UA轮换
│   ├── retry_handler.py       # 重试机制
│   └── data_validator.py      # 数据验证
├── storage/                   # 存储模块
│   ├── __init__.py
│   ├── mysql_handler.py       # MySQL操作
│   └── models.py              # 数据模型
├── config/                    # 配置文件
│   ├── settings.yaml          # 主配置
│   ├── database.yaml          # 数据库配置
│   └── logging.yaml           # 日志配置
├── tests/                     # 测试文件
│   ├── __init__.py
│   ├── test_spiders.py
│   └── test_utils.py
├── logs/                      # 日志目录
├── data/                      # 数据目录
├── requirements.txt           # 依赖包
├── scrapy.cfg                 # Scrapy项目配置
├── run.py                     # 启动脚本
└── README.md                  # 项目文档
```

## 🚀 快速开始

### 环境要求
- Python 3.8+
- MySQL 8.0+ (生产环境)
- SQLite 3.0+ (本地测试，Python内置)
- Redis 6.0+ (可选，用于分布式)

### 安装依赖
```bash
pip install -r requirements.txt
```

### 快速开始 (SQLite本地测试)

**推荐：使用SQLite进行本地测试，无需配置MySQL**

```bash
# 1. 快速设置SQLite环境
python setup_sqlite.py

# 2. 测试SQLite功能
python test_sqlite.py

# 3. 开始爬取测试
python run.py crawl Python 上海

# 4. 完整功能测试
python main_new.py --once --keywords Python --cities 上海
```

### 生产环境配置 (MySQL)

1. 复制配置模板：`cp config/settings.yaml.example config/settings.yaml`
2. 修改数据库连接信息
3. 配置代理设置（可选）
4. 在config/database.yaml中禁用SQLite，启用MySQL

### 运行爬虫
```bash
# 快速启动（推荐）
python run.py crawl "Python开发" "上海,北京"

# 完整功能
python main_new.py --once --keywords "Python开发" --cities "上海,北京"

# 定时任务模式
python main_new.py --schedule

# 系统测试
python test_system.py
```

## 🗄️ 数据库支持

### SQLite (推荐用于本地测试)

**优势：**
- 🚀 **零配置**：无需安装和配置数据库服务器
- 📁 **文件数据库**：数据存储在单个文件中，便于管理和备份
- 🔧 **轻量级**：占用资源少，启动速度快
- 🧪 **测试友好**：可以轻松创建和删除测试数据库
- 📦 **便携性**：数据库文件可以轻松备份和迁移
- 🔄 **兼容性**：与MySQL使用相同的ORM代码

**使用方法：**
```bash
# 快速设置SQLite环境
python setup_sqlite.py

# 测试SQLite功能
python test_sqlite.py

# 直接创建SQLite数据库管理器
from utils import create_sqlite_database_manager
db_manager = create_sqlite_database_manager('./data/my_test.db')
```

**配置示例：**
```yaml
# config/database.yaml
sqlite:
  enabled: true
  database_path: "./data/job_crawler.db"

mysql:
  enabled: false  # 禁用MySQL
```

### MySQL (生产环境)

适用于大规模数据存储和高并发访问场景。

## 📊 数据库设计

### 主表结构
```sql
CREATE TABLE job_listings (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_id VARCHAR(50) UNIQUE NOT NULL COMMENT '职位ID',
    job_name VARCHAR(200) NOT NULL COMMENT '职位名称',
    company_name VARCHAR(200) NOT NULL COMMENT '公司名称',
    company_type VARCHAR(50) COMMENT '公司类型',
    company_size VARCHAR(50) COMMENT '公司规模',
    
    -- 薪资信息
    salary_min INT COMMENT '最低薪资(K)',
    salary_max INT COMMENT '最高薪资(K)', 
    salary_text VARCHAR(100) COMMENT '薪资描述',
    
    -- 位置信息
    province_code VARCHAR(10) COMMENT '省份编码',
    city_code VARCHAR(10) COMMENT '城市编码',
    location_text VARCHAR(100) COMMENT '位置描述',
    
    -- 职位详情
    job_description TEXT COMMENT '职位描述',
    work_experience VARCHAR(50) COMMENT '工作经验要求',
    education VARCHAR(50) COMMENT '学历要求',
    job_tags JSON COMMENT '职位标签',
    
    -- 时间信息
    publish_date DATETIME COMMENT '发布时间',
    update_date DATETIME COMMENT '更新时间',
    crawl_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '爬取时间',
    
    -- 元数据
    job_url VARCHAR(500) COMMENT '职位链接',
    search_keyword VARCHAR(100) COMMENT '搜索关键词',
    raw_data JSON COMMENT '原始数据',
    
    INDEX idx_job_id (job_id),
    INDEX idx_company (company_name),
    INDEX idx_location (province_code, city_code),
    INDEX idx_salary (salary_min, salary_max),
    INDEX idx_publish_date (publish_date),
    INDEX idx_crawl_time (crawl_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='职位信息表';
```

## 📋 待办事项 (Roadmap)

### MVP版本 (v1.0) ✅
- [x] 基础爬虫框架搭建
- [x] 职位列表数据采集
- [x] MySQL数据存储
- [x] 基础日志记录
- [x] 简单重试机制

### 增强版本 (v1.1) 🚧
- [ ] Scrapy框架重构
- [ ] 配置文件管理
- [ ] 用户代理轮换
- [ ] 代理IP支持
- [ ] 指数退避重试
- [ ] 数据去重机制

### 企业版本 (v2.0) 📋
- [ ] 分布式爬取支持
- [ ] 实时监控面板
- [ ] 数据质量检测
- [ ] 增量更新机制
- [ ] API接口服务
- [ ] 容器化部署

### 高级功能 (v3.0) 💡
- [ ] 机器学习数据分析
- [ ] 职位推荐算法
- [ ] 薪资趋势预测
- [ ] 行业报告生成
- [ ] 可视化大屏

## 🔧 优化方向

### 性能优化
1. **并发控制**: 实现智能并发数调节
2. **缓存机制**: Redis缓存热点数据
3. **数据库优化**: 分表分库策略
4. **网络优化**: HTTP/2支持，连接池复用

### 稳定性提升
1. **监控告警**: 集成Prometheus + Grafana
2. **故障恢复**: 自动重启机制
3. **数据备份**: 定期数据备份策略
4. **版本管理**: 数据库版本迁移

### 扩展性增强
1. **多站点支持**: 抽象化爬虫框架
2. **插件系统**: 可插拔的数据处理器
3. **API网关**: 统一数据访问接口
4. **微服务架构**: 服务拆分与治理

## 📈 监控指标

- 爬取成功率
- 数据质量分数
- 响应时间分布
- 错误类型统计
- 存储性能指标

## 🤝 贡献指南

1. Fork 项目
2. 创建特性分支
3. 提交变更
4. 推送到分支
5. 创建 Pull Request

## 📄 许可证

MIT License - 详见 [LICENSE](LICENSE) 文件