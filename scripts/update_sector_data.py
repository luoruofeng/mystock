#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
更新板块行情数据的脚本
从 stocks 表中聚合数据计算板块行情
"""
import os
import sys
from datetime import datetime

# 设置输出编码为 UTF-8
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import SessionLocal, engine
from app.models import Stock, StockHistory, Sector, Base
from sqlalchemy import func, and_, desc
from collections import defaultdict

def create_sector_table():
    """创建 sectors 表"""
    print("正在创建 sectors 表...")
    Base.metadata.create_all(bind=engine)
    print("[OK] sectors 表创建完成")

def calculate_sector_data():
    """计算板块行情数据"""
    db = SessionLocal()
    
    try:
        print("正在计算板块行情数据...")
        
        # 获取所有股票的最新数据
        # 获取每个股票的最新收盘价和前一日收盘价
        latest_date_query = db.query(func.max(StockHistory.date)).scalar()
        
        if not latest_date_query:
            print("警告: 没有找到股票历史数据")
            return
        
        latest_date = latest_date_query
        print(f"使用日期: {latest_date}")
        
        # 获取最新一天的股票数据
        latest_stocks = db.query(
            StockHistory.ticker,
            StockHistory.close,
            StockHistory.volume
        ).filter(StockHistory.date == latest_date).all()
        
        # 获取前一天的数据
        prev_date_query = db.query(func.max(StockHistory.date)).filter(
            StockHistory.date < latest_date
        ).scalar()
        
        prev_stocks = {}
        if prev_date_query:
            prev_date = prev_date_query
            prev_data = db.query(
                StockHistory.ticker,
                StockHistory.close
            ).filter(StockHistory.date == prev_date).all()
            
            prev_stocks = {item.ticker: item.close for item in prev_data}
        
        # 获取股票基本信息
        stock_info = {}
        stocks = db.query(Stock).all()
        for stock in stocks:
            stock_info[stock.ticker] = {
                'name': stock.name,
                'sector': stock.sector,
                'industry': stock.industry,
                'market': stock.market
            }
        
        # 按板块聚合数据
        sector_data = defaultdict(lambda: {
            'stocks': [],
            'total_volume': 0,
            'total_amount': 0,
            'rising_count': 0,
            'falling_count': 0
        })
        
        for stock in latest_stocks:
            ticker = stock.ticker
            info = stock_info.get(ticker, {})
            sector_name = info.get('sector') or 'Unknown'
            
            close = stock.close or 0
            volume = stock.volume or 0
            
            # 计算涨跌幅
            prev_close = prev_stocks.get(ticker)
            change_percent = 0
            if prev_close and prev_close > 0:
                change_percent = ((close - prev_close) / prev_close) * 100
            
            # 更新板块数据
            sector_data[sector_name]['stocks'].append({
                'ticker': ticker,
                'name': info.get('name', ticker),
                'close': close,
                'volume': volume,
                'change_percent': change_percent,
                'market': info.get('market', 'Unknown')
            })
            
            sector_data[sector_name]['total_volume'] += volume
            sector_data[sector_name]['total_amount'] += volume * close
            
            if change_percent > 0:
                sector_data[sector_name]['rising_count'] += 1
            elif change_percent < 0:
                sector_data[sector_name]['falling_count'] += 1
        
        # 计算板块整体涨跌幅（加权平均）
        sectors_to_save = []
        
        for sector_name, data in sector_data.items():
            stocks_list = data['stocks']
            
            if not stocks_list:
                continue
            
            # 计算加权平均涨跌幅
            total_weight = sum(abs(s['change_percent']) for s in stocks_list)
            if total_weight > 0:
                weighted_change = sum(
                    s['change_percent'] * abs(s['change_percent']) / total_weight 
                    for s in stocks_list
                )
            else:
                weighted_change = sum(s['change_percent'] for s in stocks_list) / len(stocks_list)
            
            # 找出领涨和领跌股
            sorted_by_change = sorted(stocks_list, key=lambda x: x['change_percent'], reverse=True)
            top_gainer = sorted_by_change[0] if sorted_by_change else None
            top_loser = sorted_by_change[-1] if sorted_by_change else None
            
            # 计算市值（如果有数据）
            total_market_cap = 0  # 暂时设为 0，需要额外数据
            
            # 确定主要市场
            markets = [s['market'] for s in stocks_list]
            market_counts = defaultdict(int)
            for m in markets:
                market_counts[m] += 1
            main_market = max(market_counts.items(), key=lambda x: x[1])[0] if market_counts else 'ALL'
            
            sector_obj = Sector(
                name=sector_name,
                market=main_market,
                change_percent=round(weighted_change, 2),
                total_volume=data['total_volume'],
                total_amount=int(data['total_amount']),
                total_market_cap=total_market_cap,
                top_gainer_ticker=top_gainer['ticker'] if top_gainer else None,
                top_gainer_name=top_gainer['name'] if top_gainer else None,
                top_gainer_change=top_gainer['change_percent'] if top_gainer else None,
                top_loser_ticker=top_loser['ticker'] if top_loser else None,
                top_loser_name=top_loser['name'] if top_loser else None,
                top_loser_change=top_loser['change_percent'] if top_loser else None,
                stock_count=len(stocks_list),
                rising_count=data['rising_count'],
                falling_count=data['falling_count'],
                last_updated=datetime.utcnow()
            )
            
            sectors_to_save.append(sector_obj)
        
        # 保存到数据库
        print(f"正在保存 {len(sectors_to_save)} 个板块数据...")
        
        # 先删除旧数据
        db.query(Sector).delete()
        
        # 批量插入新数据
        for sector in sectors_to_save:
            db.add(sector)
        
        db.commit()
        
        print(f"[OK] 成功更新 {len(sectors_to_save)} 个板块数据")
        
        # 显示一些统计信息
        print("\n板块行情统计:")
        print("-" * 80)
        for sector in sorted(sectors_to_save, key=lambda x: x.change_percent or 0, reverse=True)[:10]:
            print(f"{sector.name:30s} {sector.change_percent:+6.2f}%  "
                  f"涨:{sector.rising_count:3d} 跌:{sector.falling_count:3d}  "
                  f"领涨: {sector.top_gainer_name or 'N/A'}")
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()

def main():
    print("=" * 80)
    print("板块行情数据更新脚本")
    print("=" * 80)
    
    # 创建表
    create_sector_table()
    
    # 计算并更新数据
    calculate_sector_data()
    
    print("\n" + "=" * 80)
    print("更新完成！")

if __name__ == "__main__":
    main()
