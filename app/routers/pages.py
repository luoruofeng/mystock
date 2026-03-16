import math
import asyncio
from datetime import datetime, date
from fastapi import APIRouter, Request, Depends, Query
from fastapi.templating import Jinja2Templates
from ..services.yfinance_service import (
    get_market_indices_data, 
    get_stock_profile_from_db,
    get_thread_pool_executor,
    get_stock_data
)
from ..database import get_db
from sqlalchemy.orm import Session
from sqlalchemy import or_, desc, func, and_
from ..models import Stock, MarketIndex, Sector, StockHistory

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

def get_stock_latest_prices(db: Session, tickers: list) -> dict:
    """获取股票最新价格信息"""
    if not tickers:
        return {}
    
    # 获取最新日期
    latest_date = db.query(func.max(StockHistory.date)).scalar()
    
    # 如果没有历史数据，返回默认值
    if not latest_date:
        result = {}
        for ticker in tickers:
            result[ticker] = {
                "close": 0,
                "change": 0,
                "change_percent": 0,
                "volume": 0,
                "date": None
            }
        return result
    
    # 获取前一天日期
    prev_date = db.query(func.max(StockHistory.date)).filter(
        StockHistory.date < latest_date
    ).scalar()
    
    # 查询最新价格
    latest_prices = db.query(
        StockHistory.ticker,
        StockHistory.close,
        StockHistory.volume
    ).filter(
        StockHistory.date == latest_date,
        StockHistory.ticker.in_(tickers)
    ).all()
    
    latest_dict = {item.ticker: {"close": item.close, "volume": item.volume} for item in latest_prices}
    
    # 查询前一天价格
    prev_prices = {}
    if prev_date:
        prev_data = db.query(
            StockHistory.ticker,
            StockHistory.close
        ).filter(
            StockHistory.date == prev_date,
            StockHistory.ticker.in_(tickers)
        ).all()
        prev_prices = {item.ticker: item.close for item in prev_data}
    
    # 组合结果
    result = {}
    for ticker in tickers:
        latest = latest_dict.get(ticker, {})
        prev_close = prev_prices.get(ticker)
        close = latest.get("close", 0)
        volume = latest.get("volume", 0)
        
        if prev_close and prev_close > 0:
            change = close - prev_close
            change_percent = ((close - prev_close) / prev_close) * 100
        else:
            change = 0
            change_percent = 0
        
        result[ticker] = {
            "close": close,
            "change": change,
            "change_percent": change_percent,
            "volume": volume,
            "date": latest_date
        }
    
    return result



@router.get("/")
async def index(
    request: Request,
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=50, ge=10, le=200),
    market: str = Query(default="ALL"),
    keyword: str = Query(default=""),
    db: Session = Depends(get_db),
):
    loop = asyncio.get_running_loop()
    executor = get_thread_pool_executor()
    indices = await loop.run_in_executor(executor, get_market_indices_data)
    market = (market or "ALL").upper()
    keyword = (keyword or "").strip()

    # If keyword is a valid ticker that is not in our DB, fetch it from Yahoo.
    if keyword and ('.SS' in keyword.upper() or '.SZ' in keyword.upper() or '.HK' in keyword.upper()):
        stock_exists = db.query(Stock).filter(func.upper(Stock.ticker) == keyword.upper()).first()
        if not stock_exists:
            # We don't have this stock in our DB, let's fetch it.
            # This will also save it to the DB for future queries.
            print(f"Ticker {keyword} not found in DB, fetching...")
            await loop.run_in_executor(
                executor,
                get_stock_data,
                keyword, "1y", "1d", True
            )
    
    # 查询股票列表
    query = db.query(Stock)
    if market in {"HK", "SS", "SZ"}:
        query = query.filter(Stock.market == market)
    else:
        market = "ALL"
    if keyword:
        fuzzy = f"%{keyword}%"
        query = query.filter(or_(Stock.ticker.ilike(fuzzy), Stock.name.ilike(fuzzy)))
    total = query.count()
    total_pages = max(1, math.ceil(total / per_page)) if total > 0 else 1
    if page > total_pages:
        page = total_pages
    stocks = (
        query.order_by(Stock.market.asc(), Stock.ticker.asc())
        .offset((page - 1) * per_page)
        .limit(per_page)
        .all()
    )
    
    # 获取股票价格信息并合并到 stocks
    stock_tickers = [s.ticker for s in stocks]
    stock_prices = get_stock_latest_prices(db, stock_tickers)
    
    # 将价格信息附加到 stocks 对象
    for stock in stocks:
        price_info = stock_prices.get(stock.ticker, {})
        stock.price_close = price_info.get('close')
        stock.price_change = price_info.get('change')
        stock.price_change_percent = price_info.get('change_percent')
        stock.price_volume = price_info.get('volume')
        stock.price_date = price_info.get('date')
    
    return templates.TemplateResponse("index.html", {
        "request": request,
        "indices": indices,
        "stocks": stocks,
        "active_page": "home",
        "page": page,
        "per_page": per_page,
        "market": market,
        "keyword": keyword,
        "total": total,
        "total_pages": total_pages,
    })

@router.get("/stock/{ticker}")
async def stock_detail(request: Request, ticker: str, db: Session = Depends(get_db)):
    profile = get_stock_profile_from_db(ticker)
    name = profile["name"]
    info = profile["info"]

    return templates.TemplateResponse("stock_detail.html", {
        "request": request,
        "ticker": ticker,
        "name": name,
        "info": info,
        "active_page": "stock"
    })

@router.get("/market")
async def market_overview(request: Request, db: Session = Depends(get_db)):
    # List all stocks available or top movers
    # For now, just show a list of tracked stocks from DB + popular ones
    stocks = db.query(Stock).all()
    return templates.TemplateResponse("market.html", {
        "request": request,
        "stocks": stocks,
        "active_page": "market"
    })

@router.get("/sectors")
async def sectors(request: Request, db: Session = Depends(get_db)):
    # 从数据库获取板块行情数据，按涨跌幅排序
    sectors_list = db.query(Sector).order_by(desc(Sector.change_percent)).all()
    
    return templates.TemplateResponse("sectors.html", {
        "request": request,
        "sectors": sectors_list,
        "active_page": "sectors"
    })

@router.get("/rankings")
async def rankings(
    request: Request,
    market: str = Query(default="ALL"),
    limit: int = Query(default=10, ge=5, le=50),
    db: Session = Depends(get_db)
):
    """涨跌排行页面"""
    # 获取最新日期
    latest_date = db.query(func.max(StockHistory.date)).scalar()
    
    if not latest_date:
        # 如果没有历史数据，返回空页面
        return templates.TemplateResponse("rankings.html", {
            "request": request,
            "active_page": "rankings",
            "market": market,
            "limit": limit,
            "latest_date": None,
            "ss_top_gainers": [],
            "ss_top_losers": [],
            "sz_top_gainers": [],
            "sz_top_losers": [],
            "hk_top_gainers": [],
            "hk_top_losers": []
        })
    
    # 获取前一天日期
    prev_date = db.query(func.max(StockHistory.date)).filter(
        StockHistory.date < latest_date
    ).scalar()
    
    # 构建查询：获取股票代码、名称、市场、最新价格、前一日价格、涨跌幅
    def get_rankings(market_filter=None):
        """获取指定市场的涨跌排行"""
        # 查询最新一天的股票数据
        query = db.query(
            StockHistory.ticker,
            StockHistory.close,
            StockHistory.volume
        ).filter(StockHistory.date == latest_date)
        
        # 如果有市场过滤
        if market_filter:
            tickers = db.query(Stock.ticker).filter(Stock.market == market_filter).all()
            ticker_list = [t[0] for t in tickers]
            query = query.filter(StockHistory.ticker.in_(ticker_list))
        
        latest_data = query.all()
        
        # 获取前一天的数据
        if prev_date:
            prev_query = db.query(
                StockHistory.ticker,
                StockHistory.close
            ).filter(StockHistory.date == prev_date)
            
            if market_filter:
                prev_query = prev_query.filter(StockHistory.ticker.in_(ticker_list))
            
            prev_data = {item.ticker: item.close for item in prev_query.all()}
        else:
            prev_data = {}
        
        # 获取股票名称和市场
        stock_info_query = db.query(Stock.ticker, Stock.name, Stock.market)
        if market_filter:
            stock_info_query = stock_info_query.filter(Stock.market == market_filter)
        
        stock_info = {item.ticker: {"name": item.name, "market": item.market} 
                      for item in stock_info_query.all()}
        
        # 计算涨跌幅
        results = []
        for item in latest_data:
            ticker = item.ticker
            close = item.close or 0
            volume = item.volume or 0
            prev_close = prev_data.get(ticker)
            
            if prev_close and prev_close > 0:
                change_percent = ((close - prev_close) / prev_close) * 100
            else:
                change_percent = 0
            
            info = stock_info.get(ticker, {})
            results.append({
                "ticker": ticker,
                "name": info.get("name", ticker),
                "market": info.get("market", "Unknown"),
                "close": close,
                "change_percent": round(change_percent, 2),
                "volume": volume
            })
        
        # 按涨跌幅排序
        top_gainers = sorted(results, key=lambda x: x["change_percent"], reverse=True)[:limit]
        top_losers = sorted(results, key=lambda x: x["change_percent"])[:limit]
        
        return top_gainers, top_losers
    
    # 获取各市场的排行
    market = (market or "ALL").upper()
    
    if market == "SS":
        ss_gainers, ss_losers = get_rankings("SS")
        sz_gainers, sz_losers = [], []
        hk_gainers, hk_losers = [], []
    elif market == "SZ":
        sz_gainers, sz_losers = get_rankings("SZ")
        ss_gainers, ss_losers = [], []
        hk_gainers, hk_losers = [], []
    elif market == "HK":
        hk_gainers, hk_losers = get_rankings("HK")
        ss_gainers, ss_losers = [], []
        sz_gainers, sz_losers = [], []
    else:
        # ALL: 显示所有市场
        ss_gainers, ss_losers = get_rankings("SS")
        sz_gainers, sz_losers = get_rankings("SZ")
        hk_gainers, hk_losers = get_rankings("HK")
    
    return templates.TemplateResponse("rankings.html", {
        "request": request,
        "active_page": "rankings",
        "market": market,
        "limit": limit,
        "latest_date": latest_date,
        "ss_top_gainers": ss_gainers,
        "ss_top_losers": ss_losers,
        "sz_top_gainers": sz_gainers,
        "sz_top_losers": sz_losers,
        "hk_top_gainers": hk_gainers,
        "hk_top_losers": hk_losers
    })
