import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
from ..database import SessionLocal
from ..models import Stock, StockHistory, MarketIndex
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import yfinance as yf
from ..constants import THREAD_POOL_MAX_WORKERS, THREAD_POOL_THREAD_NAME_PREFIX

# 缓存有效期配置（小时）
CACHE_VALIDITY_HOURS = {
    "1d": 6,      # 日线数据缓存6小时
    "1wk": 12,    # 周线数据缓存12小时
    "1mo": 24,    # 月线数据缓存24小时
    "1y": 48,     # 年线数据缓存48小时
    "default": 24
}


def period_to_start_date(period: str) -> datetime.date:
    """将 period 参数转换为起始日期"""
    today = datetime.now().date()
    period_map = {
        "1d": timedelta(days=1),
        "5d": timedelta(days=5),
        "1wk": timedelta(weeks=1),
        "1mo": relativedelta(months=1),
        "3mo": relativedelta(months=3),
        "6mo": relativedelta(months=6),
        "1y": relativedelta(years=1),
        "2y": relativedelta(years=2),
        "5y": relativedelta(years=5),
        "10y": relativedelta(years=10),
        "ytd": datetime(today.year, 1, 1).date(),
        "max": today - timedelta(days=365 * 20),  # 最大20年
    }
    delta = period_map.get(period, relativedelta(months=1))
    if isinstance(delta, timedelta):
        return today - delta
    return delta


def get_cache_validity_hours(period: str) -> int:
    """根据 period 获取缓存有效时间"""
    for key, hours in CACHE_VALIDITY_HOURS.items():
        if key in period:
            return hours
    return CACHE_VALIDITY_HOURS["default"]

# 配置 yfinance 代理（在环境变量中设置）

_executor = None

def get_thread_pool_executor():
    global _executor
    if _executor is None:
        _executor = ThreadPoolExecutor(
            max_workers=THREAD_POOL_MAX_WORKERS,
            thread_name_prefix=THREAD_POOL_THREAD_NAME_PREFIX
        )
    return _executor

MARKET_INDICES = {
    "000001.SS": "上证指数",
    "399001.SZ": "深证成指",
    "399006.SZ": "创业板指",
    "^HSI": "恒生指数"
}

POPULAR_STOCKS = [
    # HK
    "0700.HK", "9988.HK", "3690.HK", "0005.HK", "1810.HK",
    # SS
    "600519.SS", "601398.SS", "600036.SS", "601857.SS", "601127.SS",
    # SZ
    "000858.SZ", "002594.SZ", "000333.SZ", "300750.SZ", "000001.SZ"
]

def update_market_indices():
    db = SessionLocal()
    try:
        # 使用 yfinance 批量获取指数数据
        tickers = list(MARKET_INDICES.keys())
        data = yf.download(tickers, period="1d", progress=False)

        for ticker, name in MARKET_INDICES.items():
            try:
                if ticker in data.columns.get_level_values(0):
                    close = data['Close'][ticker].iloc[-1] if len(data['Close'][ticker]) > 0 else 0
                    prev_close = data['Close'][ticker].iloc[-2] if len(data['Close'][ticker]) > 1 else close
                    change = close - prev_close
                    change_pct = (change / prev_close * 100) if prev_close else 0

                    index_obj = db.query(MarketIndex).filter(MarketIndex.ticker == ticker).first()
                    if not index_obj:
                        index_obj = MarketIndex(ticker=ticker, name=name)
                        db.add(index_obj)
                    index_obj.value = float(close or 0.0)
                    index_obj.change = float(change or 0.0)
                    index_obj.change_percent = float(change_pct or 0.0)
                    index_obj.last_updated = datetime.utcnow()
            except Exception as e:
                print(f"Error updating index {ticker}: {e}")
                continue

        db.commit()
    except Exception as e:
        print(f"Error updating market indices: {e}")
    finally:
        db.close()

def get_market_indices_data():
    db = SessionLocal()
    try:
        indices = db.query(MarketIndex).all()
        if not indices:
            update_market_indices()
            indices = db.query(MarketIndex).all()
        return indices
    finally:
        db.close()

def get_stock_data_from_db(ticker: str, start_date, db) -> tuple:
    """从数据库获取股票历史数据"""
    rows = (
        db.query(StockHistory)
        .filter(StockHistory.ticker == ticker, StockHistory.date >= start_date)
        .order_by(StockHistory.date.asc())
        .all()
    )
    history_list = []
    for row in rows:
        history_list.append({
            "Date": row.date.strftime("%Y-%m-%d"),
            "Open": float(row.open or 0),
            "High": float(row.high or 0),
            "Low": float(row.low or 0),
            "Close": float(row.close or 0),
            "Volume": int(row.volume or 0)
        })
    return history_list


def get_stock_info_from_db(ticker: str, db) -> dict:
    """从数据库获取股票基本信息"""
    stock_obj = db.query(Stock).filter(Stock.ticker == ticker).first()
    if stock_obj:
        return {
            "shortName": stock_obj.name or ticker,
            "sector": stock_obj.sector or "-",
            "industry": stock_obj.industry or "-",
            "longBusinessSummary": stock_obj.description or "",
            "marketCap": "-",
            "trailingPE": "-",
            "trailingEps": "-",
            "fiftyTwoWeekHigh": "-",
            "fiftyTwoWeekLow": "-",
        }, stock_obj
    return None, None


def is_cache_valid(stock_obj: Stock, period: str) -> bool:
    """检查缓存是否仍然有效"""
    if not stock_obj or not stock_obj.last_updated:
        return False
    cache_hours = get_cache_validity_hours(period)
    valid_until = stock_obj.last_updated + timedelta(hours=cache_hours)
    return datetime.utcnow() < valid_until


def fetch_and_save_stock_data(ticker: str, period: str, interval: str, db) -> tuple:
    """从 Yahoo Finance 获取数据并保存到数据库"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 📡 正在从Yahoo Finance获取历史数据...")

    ticker_obj = yf.Ticker(ticker)
    hist = ticker_obj.history(period=period)

    history_list = []
    stock_obj = db.query(Stock).filter(Stock.ticker == ticker).first()

    if hist is not None and len(hist) > 0:
        for idx, row in hist.iterrows():
            date_val = idx.date() if hasattr(idx, 'date') else idx.date()
            history_list.append({
                "Date": date_val.strftime("%Y-%m-%d"),
                "Open": float(row['Open']) if row['Open'] else 0,
                "High": float(row['High']) if row['High'] else 0,
                "Low": float(row['Low']) if row['Low'] else 0,
                "Close": float(row['Close']) if row['Close'] else 0,
                "Volume": int(row['Volume']) if row['Volume'] else 0
            })
            if interval == "1d":
                existing = db.query(StockHistory).filter(
                    StockHistory.ticker == ticker,
                    StockHistory.date == date_val
                ).first()
                if not existing:
                    new_record = StockHistory(
                        ticker=ticker,
                        date=date_val,
                        open=float(row['Open']) if row['Open'] else 0,
                        high=float(row['High']) if row['High'] else 0,
                        low=float(row['Low']) if row['Low'] else 0,
                        close=float(row['Close']) if row['Close'] else 0,
                        volume=int(row['Volume']) if row['Volume'] else 0,
                        adj_close=float(row['Close']) if row['Close'] else 0
                    )
                    db.add(new_record)
                else:
                    existing.open = float(row['Open']) if row['Open'] else 0
                    existing.high = float(row['High']) if row['High'] else 0
                    existing.low = float(row['Low']) if row['Low'] else 0
                    existing.close = float(row['Close']) if row['Close'] else 0
                    existing.volume = int(row['Volume']) if row['Volume'] else 0

        # 获取股票基本信息
        try:
            info_data = ticker_obj.info
            market = "HK" if ".HK" in ticker else ("SS" if ".SS" in ticker else "SZ")
            if not stock_obj:
                stock_obj = Stock(
                    ticker=ticker,
                    name=info_data.get("shortName", ticker),
                    market=market,
                    sector=info_data.get("sector", "Unknown"),
                    industry=info_data.get("industry", "Unknown"),
                    description=info_data.get("longBusinessSummary", ""),
                )
                db.add(stock_obj)
            else:
                # 保留原有市场信息，避免错误覆盖
                if not stock_obj.market:
                    stock_obj.market = market
                # 更新其他信息，但避免用空值覆盖
                if info_data.get("shortName"):
                    stock_obj.name = info_data.get("shortName", stock_obj.name)
                if info_data.get("sector"):
                    stock_obj.sector = info_data.get("sector", stock_obj.sector)
                if info_data.get("industry"):
                    stock_obj.industry = info_data.get("industry", stock_obj.industry)
                if info_data.get("longBusinessSummary"):
                    stock_obj.description = info_data.get("longBusinessSummary", stock_obj.description)
                stock_obj.last_updated = datetime.utcnow()

            # 格式化市值等信息
            market_cap = info_data.get("marketCap")
            if market_cap:
                if market_cap >= 1e12:
                    market_cap_str = f"${market_cap/1e12:.2f}T"
                elif market_cap >= 1e9:
                    market_cap_str = f"${market_cap/1e9:.2f}B"
                else:
                    market_cap_str = str(market_cap)
            else:
                market_cap_str = "-"

            pe = info_data.get("trailingPE")
            eps = info_data.get("trailingEps")

            info = {
                "shortName": stock_obj.name,
                "sector": stock_obj.sector,
                "industry": stock_obj.industry,
                "longBusinessSummary": stock_obj.description,
                "marketCap": market_cap_str,
                "trailingPE": f"{pe:.2f}" if pe else "-",
                "trailingEps": f"{eps:.2f}" if eps else "-",
                "fiftyTwoWeekHigh": info_data.get("fiftyTwoWeekHigh", "-"),
                "fiftyTwoWeekLow": info_data.get("fiftyTwoWeekLow", "-"),
            }
            db.commit()
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ 成功获取并保存 {len(history_list)} 条K线数据")
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ 获取股票信息失败: {e}")
            db.commit()

    return history_list, info if 'info' in dir() else {}


def get_stock_data(ticker: str, period="1mo", interval="1d", force_refresh: bool = False):
    """
    获取股票数据（优先从数据库获取，缓存失效或无数据时从 Yahoo Finance 获取）

    Args:
        ticker: 股票代码
        period: 时间周期 (1d, 5d, 1wk, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
        interval: 时间间隔 (1d, 1wk, 1mo)
        force_refresh: 是否强制从远程获取数据

    Returns:
        tuple: (history_list, info)
    """
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🚀 开始获取股票数据: {ticker}, period={period}, interval={interval}")
    db = SessionLocal()
    try:
        start_date = period_to_start_date(period)
        stock_obj = db.query(Stock).filter(Stock.ticker == ticker).first()

        # 检查是否可以使用缓存数据
        use_cache = False
        if not force_refresh and interval == "1d":
            # 检查数据库中是否有数据
            db_data = get_stock_data_from_db(ticker, start_date, db)
            if db_data:
                # 检查缓存是否有效
                if is_cache_valid(stock_obj, period):
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] 💾 使用数据库缓存数据 ({len(db_data)} 条)")
                    use_cache = True
                    history_list = db_data
                else:
                    # 检查数据是否覆盖了请求的时间范围
                    earliest_date = db.query(StockHistory.date).filter(
                        StockHistory.ticker == ticker
                    ).order_by(StockHistory.date.asc()).first()

                    if earliest_date and earliest_date[0] <= start_date:
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] 💾 数据库数据覆盖完整，使用缓存 ({len(db_data)} 条)")
                        use_cache = True
                        history_list = db_data

        if use_cache:
            # 从数据库获取基本信息
            info, _ = get_stock_info_from_db(ticker, db)
            if not info:
                info = {
                    "shortName": ticker,
                    "sector": "-",
                    "industry": "-",
                    "longBusinessSummary": "",
                    "marketCap": "-",
                    "trailingPE": "-",
                    "trailingEps": "-",
                    "fiftyTwoWeekHigh": "-",
                    "fiftyTwoWeekLow": "-",
                }
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 🎉 股票 {ticker} 数据获取完成! 共 {len(history_list)} 条记录 (来自缓存)")
            return history_list, info

        # 缓存不可用，从 Yahoo Finance 获取
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 📡 缓存不可用，从 Yahoo Finance 获取...")
        return fetch_and_save_stock_data(ticker, period, interval, db)

    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ 股票 {ticker} 数据获取失败: {e}")
        return [], {}
    finally:
        db.close()


def get_stock_profile_from_db(ticker: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔍 检查股票 {ticker} 是否存在于数据库...")
    db = SessionLocal()
    try:
        stock_obj = db.query(Stock).filter(Stock.ticker == ticker).first()
        if not stock_obj:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ 股票 {ticker} 不在数据库中")
            return {
                "name": ticker,
                "info": {
                    "shortName": ticker,
                    "sector": "-",
                    "industry": "-",
                    "longBusinessSummary": "",
                    "marketCap": "-",
                    "trailingPE": "-",
                    "trailingEps": "-",
                    "fiftyTwoWeekHigh": "-",
                    "fiftyTwoWeekLow": "-",
                },
            }
        return {
            "name": stock_obj.name or ticker,
            "info": {
                "shortName": stock_obj.name or ticker,
                "sector": stock_obj.sector or "-",
                "industry": stock_obj.industry or "-",
                "longBusinessSummary": stock_obj.description or "",
                "marketCap": "-",
                "trailingPE": "-",
                "trailingEps": "-",
                "fiftyTwoWeekHigh": "-",
                "fiftyTwoWeekLow": "-",
            },
        }
    finally:
        db.close()

def get_popular_stocks_snapshot():
    db = SessionLocal()
    data = []
    try:
        # 使用 yfinance 批量获取数据
        tickers = yf.Tickers(POPULAR_STOCKS)
        for ticker in POPULAR_STOCKS:
            try:
                ticker_obj = tickers.tickers.get(ticker)
                if ticker_obj:
                    info = ticker_obj.info
                    current = info.get("currentPrice", info.get("regularMarketPreviousClose", 0))
                    change_pct = info.get("regularMarketChangePercent", 0)
                    volume = info.get("volume", 0)
                    name = info.get("shortName", ticker)

                    stock_obj = db.query(Stock).filter(Stock.ticker == ticker).first()
                    if not stock_obj:
                        stock_obj = Stock(
                            ticker=ticker,
                            name=name,
                            market="HK" if ".HK" in ticker else ("SS" if ".SS" in ticker else "SZ"),
                            sector=info.get("sector", "Unknown"),
                            industry=info.get("industry", "Unknown"),
                            description=info.get("longBusinessSummary", "")
                        )
                        db.add(stock_obj)
                        db.commit()
                    data.append({
                        "ticker": ticker,
                        "name": name,
                        "price": round(float(current), 2) if current else 0,
                        "change_percent": round(float(change_pct), 2) if change_pct else 0,
                        "volume": int(volume) if volume else 0
                    })
            except Exception as e:
                print(f"Error fetching popular stock {ticker}: {e}")
                continue
    finally:
        db.close()
    data.sort(key=lambda x: x["change_percent"], reverse=True)
    return data
