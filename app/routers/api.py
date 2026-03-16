import asyncio
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from ..database import get_db
from ..services import yfinance_service
from ..services.yfinance_service import get_thread_pool_executor

router = APIRouter()

@router.get("/indices")
async def get_indices(db: Session = Depends(get_db)):
    loop = asyncio.get_running_loop()
    executor = get_thread_pool_executor()
    return await loop.run_in_executor(executor, yfinance_service.get_market_indices_data)

@router.get("/stock/{ticker}/history")
async def get_stock_history(ticker: str, period: str = "1mo", interval: str = "1d", force_refresh: bool = False, db: Session = Depends(get_db)):
    """
    获取股票历史数据（优先从数据库缓存获取）

    Args:
        ticker: 股票代码
        period: 时间周期
        interval: 时间间隔
        force_refresh: 是否强制从远程刷新数据
    """
    print(f"[API] 获取股票历史数据: {ticker}, period={period}, interval={interval}, force_refresh={force_refresh}")
    loop = asyncio.get_running_loop()
    executor = get_thread_pool_executor()
    try:
        history, info = await loop.run_in_executor(
            executor,
            yfinance_service.get_stock_data,
            ticker, period, interval, force_refresh
        )
        print(f"[API] 历史数据获取完成: {ticker}, 共 {len(history)} 条")
        return {"history": history, "info": info}
    except Exception as e:
        print(f"[API] 历史数据获取失败: {ticker}, 错误: {e}")
        return {"history": [], "info": {}, "error": str(e)}

@router.get("/stock/{ticker}/has-data")
async def check_stock_has_data(ticker: str, db: Session = Depends(get_db)):
    """检查股票是否有历史数据"""
    from ..models import StockHistory
    try:
        count = db.query(StockHistory).filter(StockHistory.ticker == ticker).count()
        print(f"[API] 检查股票 {ticker} 是否有数据: {count} 条")
        return {"hasData": count > 0, "count": count}
    except Exception as e:
        print(f"[API] 检查数据失败: {ticker}, 错误: {e}")
        return {"hasData": False, "count": 0, "error": str(e)}


@router.get("/stock/{ticker}/fetch")
async def fetch_stock_data(ticker: str, period: str = "1y", interval: str = "1d", db: Session = Depends(get_db)):
    """触发股票数据拉取（强制从远程获取并更新缓存）"""
    print(f"[API] 开始拉取股票数据: {ticker}")
    loop = asyncio.get_running_loop()
    executor = get_thread_pool_executor()
    try:
        history, info = await loop.run_in_executor(
            executor,
            yfinance_service.get_stock_data,
            ticker, period, interval, True  # force_refresh=True
        )
        print(f"[API] 拉取完成: {ticker}, 历史数据: {len(history)} 条")
        # 返回与 /history 相同的格式，以便前端统一处理
        return {"history": history, "info": info, "success": True}
    except Exception as e:
        print(f"[API] 拉取失败: {ticker}, 错误: {e}")
        return {"history": [], "info": {}, "success": False, "error": str(e)}


@router.get("/top-gainers")
async def get_top_gainers():
    loop = asyncio.get_running_loop()
    executor = get_thread_pool_executor()
    return await loop.run_in_executor(executor, yfinance_service.get_popular_stocks_snapshot)
