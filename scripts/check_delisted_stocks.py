#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检测退市股票的脚本
通过 yfinance 检查股票是否能够获取到数据来判断是否退市
"""
import os
import sys
import re
import time
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
warnings.filterwarnings('ignore')

# 设置输出编码为 UTF-8
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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

def check_stock_status(ticker):
    """检查单个股票状态"""
    try:
        stock = yf.Ticker(ticker)
        # 尝试获取最近的历史数据
        hist = stock.history(period="5d", timeout=30)
        
        if hist is None or len(hist) == 0:
            # 没有历史数据，可能是退市
            return ticker, False, "No history data"
        
        # 尝试获取基本信息
        try:
            info = stock.info
            if not info or len(info) == 0:
                return ticker, False, "No info available"
        except:
            # 有些股票可能没有 info，但有历史数据，也算活跃
            pass
        
        return ticker, True, "Active"
    except Exception as e:
        error_msg = str(e)
        # 简化错误消息
        if "timeout" in error_msg.lower() or "timed out" in error_msg.lower():
            return ticker, False, "Timeout"
        return ticker, False, error_msg[:50]  # 限制错误消息长度

def check_all_stocks(stocks, max_workers=10):
    """批量检查股票状态"""
    delisted_stocks = []
    active_stocks = []
    
    print(f"开始检查 {len(stocks)} 只股票...")
    print("=" * 80)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(check_stock_status, stock['ticker']): stock 
                   for stock in stocks}
        
        completed = 0
        for future in as_completed(futures):
            try:
                ticker, is_active, message = future.result()
                stock_info = futures[future]
                completed += 1
                
                if completed % 100 == 0:
                    print(f"进度: {completed}/{len(stocks)} ({completed/len(stocks)*100:.1f}%)")
                
                if is_active:
                    active_stocks.append(stock_info)
                else:
                    delisted_stocks.append({
                        **stock_info,
                        'reason': message
                    })
                    print(f"[退市] {ticker} - {stock_info['name']} ({message})")
            except Exception as e:
                print(f"[错误] 处理股票时出错: {e}")
                continue
    
    print("=" * 80)
    print(f"检查完成！")
    print(f"[活跃] 股票: {len(active_stocks)}")
    print(f"[退市] 股票: {len(delisted_stocks)}")
    
    return active_stocks, delisted_stocks

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
    
    print(f"\n新的 SQL 文件已生成: {output_file}")

def save_delisted_list(delisted_stocks, output_file):
    """保存退市股票列表"""
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("退市股票列表\n")
        f.write("=" * 80 + "\n\n")
        
        for stock in delisted_stocks:
            f.write(f"{stock['ticker']} - {stock['name']} ({stock['market']})\n")
            f.write(f"  原因: {stock['reason']}\n\n")
    
    print(f"退市股票列表已保存: {output_file}")

def main():
    sql_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'sql', 'import_all_companies.sql')
    
    if not os.path.exists(sql_file):
        print(f"错误: 找不到文件 {sql_file}")
        return
    
    print("=" * 80)
    print("开始解析 SQL 文件...")
    stocks = parse_sql_file(sql_file)
    print(f"共找到 {len(stocks)} 只股票\n")
    
    # 检查所有股票
    active_stocks, delisted_stocks = check_all_stocks(stocks, max_workers=20)
    
    # 保存结果
    output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'sql')
    
    # 生成新的 SQL 文件
    new_sql_file = os.path.join(output_dir, 'import_all_companies_cleaned.sql')
    generate_new_sql(active_stocks, new_sql_file)
    
    # 保存退市股票列表
    delisted_file = os.path.join(output_dir, 'delisted_stocks.txt')
    save_delisted_list(delisted_stocks, delisted_file)
    
    print("\n" + "=" * 80)
    print("处理完成！")
    print(f"原始股票数量: {len(stocks)}")
    print(f"活跃股票数量: {len(active_stocks)}")
    print(f"退市股票数量: {len(delisted_stocks)}")
    print(f"\n请检查以下文件:")
    print(f"  1. 新的 SQL 文件: {new_sql_file}")
    print(f"  2. 退市股票列表: {delisted_file}")

if __name__ == "__main__":
    main()
