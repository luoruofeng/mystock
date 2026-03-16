import logging
import os
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("yahoo_client")


class YahooFinanceClient:
    _session = None
    _warmed = False
    _blocked_until = 0
    _last_block_log_at = 0

    @classmethod
    def _init_session(cls):
        if cls._session is not None:
            return
        cls._session = requests.Session()
        cls._session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Accept": "application/json,text/plain,*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://finance.yahoo.com/",
                "Connection": "keep-alive",
            }
        )
        retry = Retry(
            total=3,
            backoff_factor=0.6,
            status_forcelist=[403, 429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        cls._session.mount("https://", adapter)
        https_proxy = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
        http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
        if https_proxy or http_proxy:
            cls._session.proxies.update(
                {
                    "https": https_proxy or http_proxy,
                    "http": http_proxy or https_proxy,
                }
            )

    @classmethod
    def _warmup(cls):
        if cls._warmed:
            return
        cls._init_session()
        try:
            cls._session.get("https://finance.yahoo.com/quote/AAPL", timeout=10)
            cls._warmed = True
        except Exception as e:
            logger.error(f"warmup failed: {e}")

    @classmethod
    def _get_json(cls, url, params):
        now = time.time()
        if now < cls._blocked_until:
            if now - cls._last_block_log_at > 30:
                logger.error("yahoo endpoint temporarily blocked, skip requests")
                cls._last_block_log_at = now
            return None
        cls._warmup()
        try:
            response = cls._session.get(url, params=params, timeout=12)
            if response.status_code == 403:
                cls._blocked_until = time.time() + 300
                cls._last_block_log_at = now
                text = response.text or ""
                if "无法从中国大陆使用 Yahoo 的产品与服务" in text:
                    logger.error("request blocked 403: Yahoo 服务在中国大陆不可用，请配置代理后重试")
                else:
                    logger.error(f"request blocked 403: {url}")
                return None
            if response.status_code >= 400:
                logger.error(f"request failed {response.status_code}: {url}")
                return None
            return response.json()
        except Exception as e:
            logger.error(f"request exception: {e}")
            return None

    @classmethod
    def get_history(cls, ticker: str, period1: int, period2: int, interval: str = "1d"):
        url = f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}"
        params = {
            "symbol": ticker,
            "period1": period1,
            "period2": period2,
            "interval": interval,
            "includePrePost": "false",
            "events": "div,split",
        }
        return cls._get_json(url, params)

    @classmethod
    def get_quote(cls, ticker: str):
        url = "https://query2.finance.yahoo.com/v7/finance/quote"
        params = {"symbols": ticker, "lang": "en-US", "region": "US"}
        data = cls._get_json(url, params)
        if not data:
            return None
        results = data.get("quoteResponse", {}).get("result", [])
        if not results:
            return None
        return results[0]

    @classmethod
    def get_quotes(cls, tickers):
        symbols = ",".join(tickers)
        url = "https://query2.finance.yahoo.com/v7/finance/quote"
        params = {"symbols": symbols, "lang": "en-US", "region": "US"}
        data = cls._get_json(url, params)
        if not data:
            return {}
        results = data.get("quoteResponse", {}).get("result", [])
        return {item.get("symbol"): item for item in results if item.get("symbol")}

    @classmethod
    def get_quote_summary(cls, ticker: str):
        url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
        params = {
            "modules": "price,summaryDetail,defaultKeyStatistics,assetProfile,financialData",
            "formatted": "false",
            "lang": "en-US",
            "region": "US",
        }
        data = cls._get_json(url, params)
        if not data:
            return None
        summary = data.get("quoteSummary", {})
        results = summary.get("result", [])
        if not results:
            return None
        return results[0]
