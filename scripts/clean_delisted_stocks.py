#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
使用 akshare 获取当前活跃股票，清理退市股票
"""
import os
import sys
import re
import akshare as ak
from datetime import datetime

# 设置输出编码为 UTF-8
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def get_active_stocks_from_akshare():
    """从 akshare 获取当前活跃的股票列表"""
    active_stocks = set()
    
    print("正在从 akshare 获取当前活跃股票列表...")
    print("=" * 80)
    
    # 获取 A 股股票列表
    try:
        print("获取 A 股股票列表...")
        # 获取 A 股所有股票
        stock_info_a_code_name_df = ak.stock_info_a_code_name()
        for _, row in stock_info_a_code_name_df.iterrows():
            code = row['code']
            # 转换为 yfinance 格式
            if code.startswith('6'):
                ticker = f"{code}.SS"
            else:
                ticker = f"{code}.SZ"
            active_stocks.add(ticker)
        print(f"A 股股票数量: {len(active_stocks)}")
    except Exception as e:
        print(f"获取 A 股股票列表失败: {e}")
    
    # 获取港股股票列表
    try:
        print("获取港股股票列表...")
        # 获取港股所有股票
        stock_hk_spot_df = ak.stock_hk_spot()
        for _, row in stock_hk_spot_df.iterrows():
            code = str(row['代码']).zfill(5)  # 港股代码补齐到 5 位
            ticker = f"{code}.HK"
            active_stocks.add(ticker)
        print(f"总股票数量（含港股）: {len(active_stocks)}")
    except Exception as e:
        print(f"获取港股股票列表失败: {e}")
    
    print("=" * 80)
    print(f"共获取到 {len(active_stocks)} 只活跃股票\n")
    
    return active_stocks

def parse_sql_file(sql_file_path):
    """解析 SQL 文件，提取所有股票信息"""
    stocks = []
    
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 提取 INSERT 语句中的所有股票
    # 匹配格式: ('ticker','name','market','sector','industry')
    pattern = r"\('([^']+)','([^']+)','([^']+)','([^']*)','([^']*)'\)"
    matches = re.findall(pattern, content)
    
    for match in matches:
        ticker, name, market, sector, industry = match
        stocks.append({
            'ticker': ticker,
            'name': name,
            'market': market,
            'sector': sector,
            'industry': industry
        })
    
    return stocks

def filter_stocks(stocks, active_stocks):
    """过滤出活跃股票"""
    active = []
    delisted = []
    
    for stock in stocks:
        if stock['ticker'] in active_stocks:
            active.append(stock)
        else:
            delisted.append(stock)
    
    return active, delisted

def generate_new_sql(active_stocks, output_file):
    """生成新的 SQL 文件"""
    with open(output_file, 'w', encoding='utf-8') as f:
        # 写入数据库和表创建语句
        f.write("CREATE DATABASE IF NOT EXISTS mystock CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;\n")
        f.write("USE mystock;\n\n")
        
        f.write("CREATE TABLE IF NOT EXISTS stocks (\n")
        f.write("  ticker VARCHAR(20) PRIMARY KEY,\n")
        f.write("  name VARCHAR(100) NULL,\n")
        f.write("  market VARCHAR(10) NULL,\n")
        f.write("  sector VARCHAR(100) NULL,\n")
        f.write("  industry VARCHAR(100) NULL,\n")
        f.write("  description TEXT NULL,\n")
        f.write("  last_updated DATETIME NULL\n")
        f.write(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n")
        f.write("ALTER TABLE stocks CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;\n\n")
        
        f.write("CREATE TABLE IF NOT EXISTS company_seed (\n")
        f.write("  ticker VARCHAR(20) PRIMARY KEY,\n")
        f.write("  name VARCHAR(100) NOT NULL,\n")
        f.write("  market VARCHAR(10) NOT NULL,\n")
        f.write("  sector VARCHAR(50) NULL,\n")
        f.write("  industry VARCHAR(50) NULL\n")
        f.write(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n")
        f.write("ALTER TABLE company_seed CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;\n\n")
        
        # 清空表
        f.write("TRUNCATE TABLE company_seed;\n\n")
        
        # 写入 INSERT 语句
        f.write("INSERT INTO company_seed (ticker, name, market, sector, industry) VALUES\n")
        
        for i, stock in enumerate(active_stocks):
            # 转义单引号
            name = stock['name'].replace("'", "''")
            sector = stock['sector'].replace("'", "''") if stock['sector'] else ""
            industry = stock['industry'].replace("'", "''") if stock['industry'] else ""
            
            line = f"('{stock['ticker']}','{name}','{stock['market']}','{sector}','{industry}')"
            
            if i < len(active_stocks) - 1:
                f.write(line + ",\n")
            else:
                f.write(line + ";\n")
    
    print(f"新的 SQL 文件已生成: {output_file}")

def save_delisted_list(delisted_stocks, output_file):
    """保存退市股票列表"""
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"退市股票列表\n")
        f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")
        
        # 按市场分组
        hk_stocks = [s for s in delisted_stocks if s['market'] == 'HK']
        ss_stocks = [s for s in delisted_stocks if s['market'] == 'SS']
        sz_stocks = [s for s in delisted_stocks if s['market'] == 'SZ']
        
        f.write(f"港股退市: {len(hk_stocks)} 只\n")
        for stock in hk_stocks:
            f.write(f"  {stock['ticker']} - {stock['name']}\n")
        
        f.write(f"\n沪市退市: {len(ss_stocks)} 只\n")
        for stock in ss_stocks:
            f.write(f"  {stock['ticker']} - {stock['name']}\n")
        
        f.write(f"\n深市退市: {len(sz_stocks)} 只\n")
        for stock in sz_stocks:
            f.write(f"  {stock['ticker']} - {stock['name']}\n")
    
    print(f"退市股票列表已保存: {output_file}")

def main():
    sql_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'sql', 'import_all_companies.sql')
    
    if not os.path.exists(sql_file):
        print(f"错误: 找不到文件 {sql_file}")
        return
    
    print("=" * 80)
    print("开始解析 SQL 文件...")
    stocks = parse_sql_file(sql_file)
    print(f"SQL 文件中共有 {len(stocks)} 只股票\n")
    
    # 从 akshare 获取当前活跃股票
    active_set = get_active_stocks_from_akshare()
    
    # 过滤股票
    print("正在过滤股票...")
    active_stocks, delisted_stocks = filter_stocks(stocks, active_set)
    
    print("=" * 80)
    print(f"过滤完成！")
    print(f"活跃股票: {len(active_stocks)}")
    print(f"退市股票: {len(delisted_stocks)}")
    print()
    
    # 保存结果
    output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'sql')
    
    # 生成新的 SQL 文件
    new_sql_file = os.path.join(output_dir, 'import_all_companies_cleaned.sql')
    generate_new_sql(active_stocks, new_sql_file)
    
    # 保存退市股票列表
    if delisted_stocks:
        delisted_file = os.path.join(output_dir, 'delisted_stocks.txt')
        save_delisted_list(delisted_stocks, delisted_file)
    
    print("\n" + "=" * 80)
    print("处理完成！")
    print(f"原始股票数量: {len(stocks)}")
    print(f"活跃股票数量: {len(active_stocks)}")
    print(f"退市股票数量: {len(delisted_stocks)}")
    print(f"\n新的 SQL 文件: {new_sql_file}")
    if delisted_stocks:
        print(f"退市股票列表: {delisted_file}")

if __name__ == "__main__":
    main()
