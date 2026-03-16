from sqlalchemy import Column, String, Float, Date, DateTime, Integer, Text, BigInteger
from .database import Base
from datetime import datetime

class Stock(Base):
    __tablename__ = "stocks"

    ticker = Column(String(20), primary_key=True, index=True)
    name = Column(String(100))
    market = Column(String(10))  # SS, SZ, HK
    sector = Column(String(100))
    industry = Column(String(100))
    description = Column(Text)
    last_updated = Column(DateTime, default=datetime.utcnow)

class StockHistory(Base):
    __tablename__ = "stock_history"

    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String(20), index=True)
    date = Column(Date, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(BigInteger)
    adj_close = Column(Float)

class MarketIndex(Base):
    __tablename__ = "market_indices"
    
    ticker = Column(String(20), primary_key=True)
    name = Column(String(100))
    value = Column(Float)
    change = Column(Float)
    change_percent = Column(Float)
    last_updated = Column(DateTime, default=datetime.utcnow)

class Sector(Base):
    __tablename__ = "sectors"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False)  # 板块名称
    market = Column(String(10))  # 所属市场 (SS, SZ, HK, ALL)
    
    # 行情数据
    change_percent = Column(Float)  # 涨跌幅
    total_volume = Column(BigInteger)  # 总成交量
    total_amount = Column(BigInteger)  # 总成交额
    total_market_cap = Column(BigInteger)  # 总市值
    
    # 领涨领跌股
    top_gainer_ticker = Column(String(20))  # 领涨股代码
    top_gainer_name = Column(String(100))  # 领涨股名称
    top_gainer_change = Column(Float)  # 领涨股涨幅
    
    top_loser_ticker = Column(String(20))  # 领跌股代码
    top_loser_name = Column(String(100))  # 领跌股名称
    top_loser_change = Column(Float)  # 领跌股跌幅
    
    # 统计数据
    stock_count = Column(Integer)  # 股票数量
    rising_count = Column(Integer)  # 上涨股票数
    falling_count = Column(Integer)  # 下跌股票数
    
    last_updated = Column(DateTime, default=datetime.utcnow)
