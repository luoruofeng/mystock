# 板块行情功能更新说明

## 更新内容

已成功更新板块行情页面，现在展示以下字段：

### 数据库字段

在 `sectors` 表中包含以下字段：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| name | String(100) | 板块名称 |
| market | String(10) | 所属市场 (SS, SZ, HK, ALL) |
| change_percent | Float | 涨跌幅（加权平均）|
| total_volume | BigInteger | 总成交量 |
| total_amount | BigInteger | 总成交额 |
| total_market_cap | BigInteger | 总市值（待完善）|
| top_gainer_ticker | String(20) | 领涨股代码 |
| top_gainer_name | String(100) | 领涨股名称 |
| top_gainer_change | Float | 领涨股涨幅 |
| top_loser_ticker | String(20) | 领跌股代码 |
| top_loser_name | String(100) | 领跌股名称 |
| top_loser_change | Float | 领跌股跌幅 |
| stock_count | Integer | 股票数量 |
| rising_count | Integer | 上涨股票数 |
| falling_count | Integer | 下跌股票数 |
| last_updated | DateTime | 最后更新时间 |

### 前端展示

板块行情页面现在展示：

1. **板块行情概览表格**
   - 板块名称
   - 涨跌幅（带颜色标识）
   - 股票数量
   - 上涨/下跌股票数
   - 总成交量（亿股）
   - 总成交额（亿元）
   - 领涨股（带链接）
   - 领跌股（带链接）

2. **板块卡片视图**
   - 板块名称和涨跌幅
   - 股票数量和涨跌比
   - 总成交量和成交额
   - 领涨股和领跌股详情

## 使用方法

### 1. 更新板块数据

运行以下命令来更新板块行情数据：

```bash
python scripts/update_sector_data.py
```

该脚本会：
- 从 `stocks` 表中获取股票数据
- 从 `stock_history` 表中获取最新价格数据
- 按板块聚合计算行情数据
- 更新 `sectors` 表

### 2. 定时更新

建议设置定时任务（如 cron）每天收盘后自动更新：

```bash
# 每天下午 16:00 更新板块数据
0 16 * * * cd /path/to/mystock && python scripts/update_sector_data.py
```

### 3. 访问页面

启动应用后，访问以下地址查看板块行情：

```
http://localhost:8000/sectors
```

## 注意事项

1. **数据依赖**
   - 板块数据依赖于 `stock_history` 表中的历史数据
   - 需要先有股票历史数据才能计算板块行情
   - 建议先运行股票数据更新脚本

2. **数据准确性**
   - 涨跌幅采用加权平均算法
   - 成交量和成交额基于实际交易数据
   - 领涨股和领跌股基于当日涨跌幅排序

3. **性能优化**
   - 建议定期清理过期的历史数据
   - 可考虑添加缓存机制
   - 大量数据时注意查询性能

## 后续优化建议

1. **添加更多指标**
   - 板块市盈率
   - 板块市净率
   - 板块资金流向

2. **图表展示**
   - 板块涨跌幅柱状图
   - 板块成交额饼图
   - 板块走势折线图

3. **数据源优化**
   - 考虑从专业数据源获取板块数据
   - 添加板块成分股管理
   - 支持自定义板块

## 文件说明

### 新增文件

- `app/models.py` - 添加了 Sector 模型
- `scripts/update_sector_data.py` - 板块数据更新脚本
- `app/templates/sectors.html` - 更新了板块行情页面

### 修改文件

- `app/routers/pages.py` - 更新了 `/sectors` 路由

## 技术栈

- **后端**: FastAPI + SQLAlchemy
- **前端**: Jinja2 Templates + Bootstrap 5
- **数据库**: MySQL
- **图标**: Font Awesome
