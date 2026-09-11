from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import os
import time
import io
import math
import json
import secrets
import hmac
import hashlib
import base64
import threading
import queue
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, ImageEnhance, ImageOps, UnidentifiedImageError
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from collections import Counter, OrderedDict
from datetime import datetime, timezone
from urllib.request import Request as UrlRequest, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError

try:
    import websocket as _finnhub_ws
except Exception as _finnhub_ws_import_error:
    _finnhub_ws = None
    _finnhub_ws_import_error_text = f"{type(_finnhub_ws_import_error).__name__}: {_finnhub_ws_import_error}"
else:
    _finnhub_ws_import_error_text = ""


try:
    from native_broker_feed import get_native_broker_market_data, native_feed_status
except Exception as _native_feed_import_error:
    get_native_broker_market_data = None
    _native_feed_import_error_text = f"{type(_native_feed_import_error).__name__}: {_native_feed_import_error}"
    def native_feed_status(_err=_native_feed_import_error_text):
        return {
            "quotex": {"configured": False, "connected": False, "last_error": f"native feed import failed: {_err}"},
            "pocket_option": {"configured": False, "connected": False, "last_error": f"native feed import failed: {_err}"},
        }

# Lazy-load yfinance only when a market scan actually starts.
# This keeps the Flask/Gunicorn web shell lighter during Render cold boot.
_yf_module = None
_yf_import_lock = threading.Lock()

def _get_yfinance():
    global _yf_module
    if _yf_module is None:
        with _yf_import_lock:
            if _yf_module is None:
                import yfinance as _yf
                _yf_module = _yf
    return _yf_module


# Pandas is used only when Twelve Data backup candles are needed.
_pd_module = None
_pd_import_lock = threading.Lock()

def _get_pandas():
    global _pd_module
    if _pd_module is None:
        with _pd_import_lock:
            if _pd_module is None:
                import pandas as _pd
                _pd_module = _pd
    return _pd_module


try:
    import psycopg
except Exception:
    psycopg = None

app = Flask(__name__, static_folder=".", template_folder=".")
CORS(app)

# =========================================================
# RAJA AI MULTI-TIMEFRAME BACKEND
# V56 uses the selected broker native WebSocket for normal LIVE Forex.
# Quotex/Pocket Option broker data is used directly when the native session is configured.
# 2m, 5m, 10m, 15m and 30m are resampled from OANDA 1-minute candles.
# OANDA/Yahoo/Twelve are not used for normal LIVE Forex in this build.
#
# IMPORTANT: "(OTC)" assets are underlying-market/reference proxies.
# They are NOT exact broker OTC quotes (Quotex/Pocket Option).
# =========================================================


# =========================================================
# OANDA V20 FOREX DATA (V55 PRIMARY/ONLY NORMAL FOREX FEED)
# =========================================================
OANDA_API_TOKEN = str(os.getenv("OANDA_API_TOKEN") or "").strip()
OANDA_ENV = str(os.getenv("OANDA_ENV") or "practice").strip().lower()
OANDA_API_BASE = (
    "https://api-fxtrade.oanda.com"
    if OANDA_ENV in {"live", "fxtrade", "trade"}
    else "https://api-fxpractice.oanda.com"
)
OANDA_ENABLED = bool(OANDA_API_TOKEN)

# Normal Forex instruments supported by the RAJA live-market selector.
OANDA_INSTRUMENTS = {
    "EUR/USD": "EUR_USD",
    "GBP/USD": "GBP_USD",
    "USD/JPY": "USD_JPY",
    "AUD/USD": "AUD_USD",
    "USD/CAD": "USD_CAD",
    "USD/CHF": "USD_CHF",
    "NZD/USD": "NZD_USD",
    "EUR/GBP": "EUR_GBP",
    "EUR/JPY": "EUR_JPY",
    "GBP/JPY": "GBP_JPY",
    "AUD/JPY": "AUD_JPY",
    "EUR/AUD": "EUR_AUD",
    "GBP/AUD": "GBP_AUD",
    "CAD/JPY": "CAD_JPY",
    "EUR/CAD": "EUR_CAD",
    "GBP/CAD": "GBP_CAD",
    "NZD/JPY": "NZD_JPY",
    "AUD/NZD": "AUD_NZD",
    "EUR/CHF": "EUR_CHF",
    "GBP/CHF": "GBP_CHF",
    "AUD/CAD": "AUD_CAD",
    "AUD/CHF": "AUD_CHF",
    "CAD/CHF": "CAD_CHF",
    "CHF/JPY": "CHF_JPY",
    "GBP/NZD": "GBP_NZD",
    "EUR/NZD": "EUR_NZD",
    "NZD/CAD": "NZD_CAD",
    "NZD/CHF": "NZD_CHF",
    "USD/SGD": "USD_SGD",
    "USD/CNH": "USD_CNH",
    "USD/MXN": "USD_MXN",
    "USD/ZAR": "USD_ZAR",
    "EUR/TRY": "EUR_TRY",
    "XAUUSD": "XAU_USD",
}

_oanda_cache = {}
_oanda_cache_lock = threading.Lock()
OANDA_CACHE_SECONDS = 4.0

def _fetch_oanda_1m(pair, count=1200):
    """Fetch real OANDA midpoint M1 candles for a normal Forex pair."""
    instrument = OANDA_INSTRUMENTS.get(str(pair or "").strip())
    if not instrument:
        return None, None, instrument, {
            "source": "OANDA",
            "source_mode": "unsupported_oanda_instrument",
            "provider_symbol": instrument,
            "unavailable_reason": f"{pair} is not configured as an OANDA instrument.",
        }
    if not OANDA_ENABLED:
        return None, None, instrument, {
            "source": "OANDA",
            "source_mode": "oanda_not_configured",
            "provider_symbol": instrument,
            "unavailable_reason": "OANDA_API_TOKEN is not configured in Railway Variables.",
        }

    now = time.time()
    cache_key = (instrument, int(count))
    with _oanda_cache_lock:
        cached = _oanda_cache.get(cache_key)
        if cached and now - float(cached.get("ts") or 0) <= OANDA_CACHE_SECONDS:
            df = cached["df"].copy()
            age = _source_candle_age_seconds(df)
            return df, age, instrument, dict(cached["info"])

    params = urlencode({
        "granularity": "M1",
        "count": max(100, min(5000, int(count))),
        "price": "M",
        "smooth": "false",
    })
    url = f"{OANDA_API_BASE}/v3/instruments/{instrument}/candles?{params}"
    req = UrlRequest(
        url,
        headers={
            "Authorization": f"Bearer {OANDA_API_TOKEN}",
            "Accept": "application/json",
            "User-Agent": "RAJA-AI-OANDA/55",
        },
        method="GET",
    )
    try:
        with urlopen(req, timeout=12) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        try:
            body = exc.read().decode("utf-8", "ignore")
        except Exception:
            body = ""
        return None, None, instrument, {
            "source": "OANDA",
            "source_mode": "oanda_http_error",
            "provider_symbol": instrument,
            "unavailable_reason": f"OANDA HTTP {getattr(exc,'code','?')}: {body[:180] or 'request failed'}",
        }
    except Exception as exc:
        return None, None, instrument, {
            "source": "OANDA",
            "source_mode": "oanda_network_error",
            "provider_symbol": instrument,
            "unavailable_reason": f"OANDA request failed: {type(exc).__name__}: {exc}",
        }

    candles = payload.get("candles") or []
    if not candles:
        return None, None, instrument, {
            "source": "OANDA",
            "source_mode": "oanda_empty",
            "provider_symbol": instrument,
            "unavailable_reason": "OANDA returned no candles for this pair.",
        }

    pd = _get_pandas()
    rows = []
    for c in candles:
        mid = c.get("mid") or {}
        try:
            ts = pd.to_datetime(c.get("time"), utc=True)
            rows.append({
                "time": ts,
                "Open": float(mid["o"]),
                "High": float(mid["h"]),
                "Low": float(mid["l"]),
                "Close": float(mid["c"]),
                "Volume": float(c.get("volume") or 0),
                "_complete": bool(c.get("complete")),
            })
        except Exception:
            continue

    if len(rows) < 20:
        return None, None, instrument, {
            "source": "OANDA",
            "source_mode": "oanda_short",
            "provider_symbol": instrument,
            "unavailable_reason": f"OANDA returned only {len(rows)} usable candles.",
        }

    df = pd.DataFrame(rows).set_index("time").sort_index()
    # Keep the forming candle in the base feed so the chart can display it.
    # build_timeframe()/signal logic still removes forming buckets before SK25 evaluation.
    df = df[["Open", "High", "Low", "Close", "Volume"]]
    age = _source_candle_age_seconds(df)
    quality = _market_candle_quality(df)
    info = {
        "source": "OANDA",
        "source_mode": "oanda_v20_live",
        "provider_symbol": instrument,
        "oanda_environment": "live" if OANDA_ENV in {"live","fxtrade","trade"} else "practice",
        "feed_quality": quality,
        "backup_used": False,
        "tradingview_hint": f"TradingView source: OANDA:{instrument.replace('_','')}",
    }
    with _oanda_cache_lock:
        _oanda_cache[cache_key] = {"ts": now, "df": df.copy(), "info": dict(info)}
    return df, age, instrument, info


YAHOO_SYMBOLS = {
    # ---------------- Crypto Live ----------------
    "BTC-USD": "BTC-USD",
    "ETH-USD": "ETH-USD",
    "SOL-USD": "SOL-USD",
    "LTC-USD": "LTC-USD",
    "XRP-USD": "XRP-USD",
    "ADA-USD": "ADA-USD",
    "DOGE-USD": "DOGE-USD",

    # ---------------- Crypto OTC proxies ----------------
    # Yahoo underlying-market proxies; not exact Quotex OTC quotes.
    # Keep this list synchronized with the 17 Quotex Crypto OTC assets in index.html.
    "Zcash (OTC)": "ZEC-USD",
    "Chainlink (OTC)": "LINK-USD",
    "Bitcoin (OTC)": "BTC-USD",
    "Binance Coin (OTC)": "BNB-USD",
    "Ethereum (OTC)": "ETH-USD",
    "Bitcoin Cash (OTC)": "BCH-USD",
    "Cosmos (OTC)": "ATOM-USD",
    "Ethereum Classic (OTC)": "ETC-USD",
    "Axie Infinity (OTC)": "AXS-USD",
    "Trump (OTC)": "TRUMP35336-USD",
    "Dash (OTC)": "DASH-USD",
    "Solana (OTC)": "SOL-USD",
    "Toncoin (OTC)": "TON11419-USD",
    "Litecoin (OTC)": "LTC-USD",
    "Avalanche (OTC)": "AVAX-USD",
    "Polkadot (OTC)": "DOT-USD",
    "Ripple (OTC)": "XRP-USD",

    # ---------------- Forex Live ----------------
    "EUR/USD": "EURUSD=X",
    "GBP/USD": "GBPUSD=X",
    "USD/JPY": "USDJPY=X",
    "AUD/USD": "AUDUSD=X",
    "USD/CAD": "USDCAD=X",
    "USD/CHF": "USDCHF=X",
    "NZD/USD": "NZDUSD=X",
    "EUR/GBP": "EURGBP=X",
    "EUR/JPY": "EURJPY=X",
    "GBP/JPY": "GBPJPY=X",
    "AUD/JPY": "AUDJPY=X",
    "EUR/AUD": "EURAUD=X",
    "GBP/AUD": "GBPAUD=X",
    "CAD/JPY": "CADJPY=X",
    "EUR/CAD": "EURCAD=X",
    "GBP/CAD": "GBPCAD=X",
    "NZD/JPY": "NZDJPY=X",
    "AUD/NZD": "AUDNZD=X",
    "EUR/CHF": "EURCHF=X",
    "GBP/CHF": "GBPCHF=X",
    # Gold live reference uses Yahoo gold futures because the old spot-style symbol returned 404.
    "XAUUSD": "GC=F",

    # ---------------- Current Quotex Forex OTC list ----------------
    # These are Yahoo underlying/FX proxies; exact Quotex OTC candles can differ.
    "USD/BRL (OTC)": "USDBRL=X",
    "NZD/CHF (OTC)": "NZDCHF=X",
    "NZD/JPY (OTC)": "NZDJPY=X",
    "USD/COP (OTC)": "USDCOP=X",
    "USD/MXN (OTC)": "USDMXN=X",
    "AUD/NZD (OTC)": "AUDNZD=X",
    "USD/BDT (OTC)": "USDBDT=X",
    "USD/DZD (OTC)": "USDDZD=X",
    "USD/NGN (OTC)": "USDNGN=X",
    "USD/PHP (OTC)": "USDPHP=X",
    "USD/PKR (OTC)": "USDPKR=X",
    "USD/ZAR (OTC)": "USDZAR=X",
    "USD/INR (OTC)": "USDINR=X",
    "USD/EGP (OTC)": "USDEGP=X",
    "USD/IDR (OTC)": "USDIDR=X",
    "USD/ARS (OTC)": "USDARS=X",
    "GBP/NZD (OTC)": "GBPNZD=X",
    "EUR/NZD (OTC)": "EURNZD=X",
    "NZD/USD (OTC)": "NZDUSD=X",
    "NZD/CAD (OTC)": "NZDCAD=X",
    "CAD/CHF (OTC)": "CADCHF=X",

    # ---------------- Pocket Option OTC reference proxies ----------------
    # Pocket Option pair lists are broker-specific in index.html.
    # These Yahoo symbols are REFERENCE proxies only, not exact Pocket Option OTC candles.

    # Pocket Option Crypto OTC additions
    "BNB (OTC)": "BNB-USD",
    "Cardano (OTC)": "ADA-USD",
    "Polygon (OTC)": "MATIC-USD",
    "TRON (OTC)": "TRX-USD",
    "Bitcoin ETF (OTC)": "IBIT",
    "Dogecoin (OTC)": "DOGE-USD",

    # Pocket Option Forex OTC additions (30-pair preferred set)
    "EUR/USD (OTC)": "EURUSD=X",
    "GBP/USD (OTC)": "GBPUSD=X",
    "USD/JPY (OTC)": "USDJPY=X",
    "AUD/USD (OTC)": "AUDUSD=X",
    "USD/CAD (OTC)": "USDCAD=X",
    "USD/CHF (OTC)": "USDCHF=X",
    "EUR/JPY (OTC)": "EURJPY=X",
    "GBP/JPY (OTC)": "GBPJPY=X",
    "AUD/JPY (OTC)": "AUDJPY=X",
    "CAD/JPY (OTC)": "CADJPY=X",
    "EUR/CHF (OTC)": "EURCHF=X",
    "EUR/GBP (OTC)": "EURGBP=X",
    "AUD/CAD (OTC)": "AUDCAD=X",
    "AUD/CHF (OTC)": "AUDCHF=X",
    "GBP/AUD (OTC)": "GBPAUD=X",
    "CHF/JPY (OTC)": "CHFJPY=X",
    "USD/SGD (OTC)": "USDSGD=X",
    "USD/CNH (OTC)": "USDCNH=X",
    "USD/MYR (OTC)": "USDMYR=X",
    "EUR/TRY (OTC)": "EURTRY=X",

    # Pocket Option Stocks OTC
    "Apple (OTC)": "AAPL",
    "American Express (OTC)": "AXP",
    "Boeing Company (OTC)": "BA",
    "Cisco (OTC)": "CSCO",
    "Facebook Inc (OTC)": "META",
    "Intel (OTC)": "INTC",
    "Johnson & Johnson (OTC)": "JNJ",
    "McDonald's (OTC)": "MCD",
    "Microsoft (OTC)": "MSFT",
    "Pfizer Inc (OTC)": "PFE",
    "Tesla (OTC)": "TSLA",
    "ExxonMobil (OTC)": "XOM",
    "Advanced Micro Devices (OTC)": "AMD",
}
# Twelve Data LIVE BACKUP symbols. These are underlying/reference instruments,
# never exact Quotex/Pocket Option OTC candles.
TWELVE_DATA_SYMBOLS = {
    # Crypto Live
    "BTC-USD": "BTC/USD", "ETH-USD": "ETH/USD", "SOL-USD": "SOL/USD",
    "LTC-USD": "LTC/USD", "XRP-USD": "XRP/USD", "ADA-USD": "ADA/USD",
    "DOGE-USD": "DOGE/USD",

    # Crypto OTC reference instruments
    "Zcash (OTC)": "ZEC/USD", "Chainlink (OTC)": "LINK/USD",
    "Bitcoin (OTC)": "BTC/USD", "Binance Coin (OTC)": "BNB/USD",
    "Ethereum (OTC)": "ETH/USD", "Bitcoin Cash (OTC)": "BCH/USD",
    "Cosmos (OTC)": "ATOM/USD", "Ethereum Classic (OTC)": "ETC/USD",
    "Axie Infinity (OTC)": "AXS/USD", "Trump (OTC)": "TRUMP/USD",
    "Dash (OTC)": "DASH/USD", "Solana (OTC)": "SOL/USD",
    "Toncoin (OTC)": "TON/USD", "Litecoin (OTC)": "LTC/USD",
    "Avalanche (OTC)": "AVAX/USD", "Polkadot (OTC)": "DOT/USD",
    "Ripple (OTC)": "XRP/USD", "BNB (OTC)": "BNB/USD",
    "Cardano (OTC)": "ADA/USD", "Polygon (OTC)": "MATIC/USD",
    "TRON (OTC)": "TRX/USD", "Dogecoin (OTC)": "DOGE/USD",
    "Bitcoin ETF (OTC)": "IBIT",

    # Gold
    "XAUUSD": "XAU/USD",

    # Pocket Option Stocks OTC reference instruments
    "Apple (OTC)": "AAPL", "American Express (OTC)": "AXP",
    "Boeing Company (OTC)": "BA", "Cisco (OTC)": "CSCO",
    "Facebook Inc (OTC)": "META", "Intel (OTC)": "INTC",
    "Johnson & Johnson (OTC)": "JNJ", "McDonald's (OTC)": "MCD",
    "Microsoft (OTC)": "MSFT", "Pfizer Inc (OTC)": "PFE",
    "Tesla (OTC)": "TSLA", "ExxonMobil (OTC)": "XOM",
    "Advanced Micro Devices (OTC)": "AMD",
}

# Forex Live/OTC symbols already use slash notation in the app. Twelve Data accepts
# that notation directly, so populate the remaining FX mappings automatically.
for _raja_pair in YAHOO_SYMBOLS:
    _clean_pair = str(_raja_pair).replace(" (OTC)", "").strip()
    if "/" in _clean_pair:
        TWELVE_DATA_SYMBOLS.setdefault(_raja_pair, _clean_pair)


# V67 Twelve Data primary markets.
# Normal slash-format Forex + XAUUSD and Crypto Live use Twelve Data only.
TWELVE_DATA_CRYPTO_LIVE_PAIRS = {
    "BTC-USD", "ETH-USD", "SOL-USD", "LTC-USD",
    "XRP-USD", "ADA-USD", "DOGE-USD",
}
TWELVE_DATA_FOREX_LIVE_PAIRS = {
    p for p in YAHOO_SYMBOLS
    if "(OTC)" not in p and "/" in str(p)
}
TWELVE_DATA_FOREX_LIVE_PAIRS.add("XAUUSD")
TWELVE_DATA_PRIMARY_PAIRS = TWELVE_DATA_FOREX_LIVE_PAIRS | TWELVE_DATA_CRYPTO_LIVE_PAIRS

def _twelve_data_only_market_data(pair, force=False):
    """Twelve Data-only source for Forex Live and Crypto Live."""
    clean = str(pair or "").strip()
    provider_symbol = TWELVE_DATA_SYMBOLS.get(clean)
    if not TWELVE_DATA_ENABLED:
        return None, None, provider_symbol or clean, {
            "source": "Twelve Data",
            "source_mode": "twelve_data_not_configured",
            "provider_symbol": provider_symbol,
            "unavailable_reason": "TWELVE_DATA_API_KEY is not configured.",
            "backup_used": False,
        }
    if not provider_symbol:
        return None, None, clean, {
            "source": "Twelve Data",
            "source_mode": "twelve_data_symbol_missing",
            "provider_symbol": None,
            "unavailable_reason": f"No Twelve Data symbol is configured for {clean}.",
            "backup_used": False,
        }

    try:
        df, symbol = get_twelve_data_market_data(clean, force=force)
    except Exception as exc:
        return None, None, provider_symbol, {
            "source": "Twelve Data",
            "source_mode": "twelve_data_error",
            "provider_symbol": provider_symbol,
            "unavailable_reason": f"Twelve Data error: {type(exc).__name__}: {exc}",
            "backup_used": False,
        }

    if df is None or getattr(df, "empty", True):
        return None, None, symbol or provider_symbol, {
            "source": "Twelve Data",
            "source_mode": "twelve_data_no_candles",
            "provider_symbol": symbol or provider_symbol,
            "unavailable_reason": "Twelve Data returned no usable 1-minute candles. Check plan/API credits/symbol access.",
            "backup_used": False,
        }

    try:
        df = _raja_ensure_datetime_index(df)
    except Exception:
        pass
    age = _source_candle_age_seconds(df)
    return df, age, symbol or provider_symbol, {
        "source": "Twelve Data",
        "source_mode": "twelve_data_primary",
        "provider_symbol": symbol or provider_symbol,
        "feed_quality": _market_candle_quality(df),
        "backup_used": False,
        "credentials_required": True,
        "twelve_data_primary": True,
    }

ALL_PAIRS = list(YAHOO_SYMBOLS.keys())
UNIQUE_YAHOO_SYMBOLS = list(dict.fromkeys(YAHOO_SYMBOLS.values()))
FOREX_OTC_PAIRS = [pair for pair in YAHOO_SYMBOLS if pair.endswith(" (OTC)") and "/" in pair]

POCKET_OPTION_FOREX_OTC_PAIRS = ['EUR/USD (OTC)', 'GBP/USD (OTC)', 'USD/JPY (OTC)', 'AUD/USD (OTC)', 'USD/CAD (OTC)', 'USD/CHF (OTC)', 'EUR/JPY (OTC)', 'GBP/JPY (OTC)', 'AUD/JPY (OTC)', 'CAD/JPY (OTC)', 'EUR/CHF (OTC)', 'EUR/GBP (OTC)', 'AUD/CAD (OTC)', 'AUD/CHF (OTC)', 'CAD/CHF (OTC)', 'NZD/JPY (OTC)', 'EUR/NZD (OTC)', 'GBP/AUD (OTC)', 'CHF/JPY (OTC)', 'USD/MXN (OTC)', 'USD/BRL (OTC)', 'USD/INR (OTC)', 'USD/SGD (OTC)', 'USD/CNH (OTC)', 'USD/IDR (OTC)', 'USD/PHP (OTC)', 'USD/MYR (OTC)', 'USD/COP (OTC)', 'EUR/TRY (OTC)']
POCKET_OPTION_CRYPTO_OTC_PAIRS = ['BNB (OTC)', 'Polkadot (OTC)', 'Ethereum (OTC)', 'Toncoin (OTC)', 'Cardano (OTC)', 'Polygon (OTC)', 'TRON (OTC)', 'Avalanche (OTC)', 'Bitcoin (OTC)', 'Bitcoin ETF (OTC)', 'Solana (OTC)', 'Chainlink (OTC)', 'Litecoin (OTC)', 'Dogecoin (OTC)']
POCKET_OPTION_STOCKS_OTC_PAIRS = ['Apple (OTC)', 'American Express (OTC)', 'Boeing Company (OTC)', 'Cisco (OTC)', 'Facebook Inc (OTC)', 'Intel (OTC)', 'Johnson & Johnson (OTC)', "McDonald's (OTC)", 'Microsoft (OTC)', 'Pfizer Inc (OTC)', 'Tesla (OTC)', 'ExxonMobil (OTC)', 'Advanced Micro Devices (OTC)']


# Broker-side availability guard. Keep the backend aligned with index.html so
# an old browser/client cannot request a signal for a pair unavailable on the
# selected broker. Auto Scan will simply continue with the remaining pairs.
RAJA_BROKER_UNAVAILABLE_PAIR_KEYS = {
    "quotex": {"AUDNZD", "AUDNZDOTC"},
    "pocketoption": {"AUDNZD", "AUDNZDOTC"},
}

def _raja_broker_pair_key(pair):
    raw = str(pair or "").upper().replace("(OTC)", "OTC")
    return "".join(ch for ch in raw if ch.isalnum())

def raja_pair_allowed_for_broker(broker, pair):
    broker_key = str(broker or "").casefold().replace(" ", "").replace("_", "")
    if broker_key in {"pocket", "pocketoption"}:
        broker_key = "pocketoption"
    elif broker_key == "quotex":
        broker_key = "quotex"
    else:
        return True
    return _raja_broker_pair_key(pair) not in RAJA_BROKER_UNAVAILABLE_PAIR_KEYS.get(broker_key, set())

TIMEFRAMES = {
    "1m": 1,
    "2m": 2,
    "5m": 5,
    "10m": 10,
    "15m": 15,
    "30m": 30,
}

# Selected trade expiry must be confirmed by the matching analysis timeframe.
# 15s/30s are intentionally excluded because the base Yahoo feed is 1-minute.
EXPIRY_CONFIRMATION_TIMEFRAME = {
    "1m": "1m",
    "2m": "2m",
    "5m": "5m",
    "10m": "10m",
    "15m": "15m",
    "30m": "30m",
}

# One Yahoo 1m download per unique symbol; all higher TFs are resampled.
CACHE_DURATION = int(os.environ.get("RAJA_CACHE_SECONDS", "90"))
STALE_CACHE_MAX_AGE = int(os.environ.get("RAJA_STALE_CACHE_SECONDS", "180"))
YAHOO_FAILURE_COOLDOWN = int(os.environ.get("RAJA_YAHOO_FAILURE_COOLDOWN", "180"))
# Keep three Yahoo fetches in flight to match the default three batch workers; this reduces
# partial 21-pair scans without opening an aggressive request storm.
YAHOO_FETCH_CONCURRENCY = max(1, min(3, int(os.environ.get("RAJA_YAHOO_CONCURRENCY", "3"))))
YAHOO_MIN_GAP_SECONDS = max(0.0, float(os.environ.get("RAJA_YAHOO_MIN_GAP", "0.30")))
BATCH_CACHE_DURATION = max(5, int(os.environ.get("RAJA_BATCH_CACHE_SECONDS", "30")))
BATCH_SCAN_WORKERS = max(1, min(4, int(os.environ.get("RAJA_BATCH_WORKERS", "4"))))
YAHOO_REQUEST_TIMEOUT_SECONDS = max(3.0, min(15.0, float(os.environ.get("RAJA_YAHOO_REQUEST_TIMEOUT", "7"))))
YAHOO_SYMBOL_LOCK_WAIT_SECONDS = max(2.0, min(20.0, float(os.environ.get("RAJA_YAHOO_SYMBOL_LOCK_WAIT", "8"))))
YAHOO_SEMAPHORE_WAIT_SECONDS = max(2.0, min(25.0, float(os.environ.get("RAJA_YAHOO_SEMAPHORE_WAIT", "12"))))
# Reject market candles that are too old for a live 1-minute trading decision.
# This prevents a freshly-downloaded but old weekend/closed-market candle from being labelled "fresh".
MAX_SOURCE_CANDLE_AGE_SECONDS = max(120, min(3600, int(os.environ.get("RAJA_MAX_SOURCE_CANDLE_AGE", "300"))))
# Browser batch timeout is 90s. Keep the backend deadline comfortably below that
# so slow Yahoo symbols become a safe PARTIAL response instead of a browser failure.
# 58s also leaves headroom for auth, news-safety checks, DB work and network latency.
BATCH_SCAN_DEADLINE_SECONDS = max(25.0, min(75.0, float(os.environ.get("RAJA_BATCH_DEADLINE_SECONDS", "75"))))
FOREX_OTC_FALLBACK_DEADLINE_SECONDS = max(BATCH_SCAN_DEADLINE_SECONDS, min(78.0, float(os.environ.get("RAJA_FOREX_OTC_FALLBACK_DEADLINE_SECONDS", "72"))))

# Twelve Data live backup. The key stays server-side in Railway environment variables.
TWELVE_DATA_API_KEY = (
    os.environ.get("TWELVE_DATA_API_KEY")
    or os.environ.get("RAJA_TWELVE_DATA_API_KEY")
    or ""
).strip()
TWELVE_DATA_ENABLED = bool(TWELVE_DATA_API_KEY)
TWELVE_DATA_BASE_URL = (os.environ.get("RAJA_TWELVE_DATA_BASE_URL") or "https://api.twelvedata.com/time_series").strip()
TWELVE_DATA_OUTPUTSIZE = max(900, min(5000, int(os.environ.get("RAJA_TWELVE_DATA_OUTPUTSIZE", "4000"))))
TWELVE_DATA_CACHE_SECONDS = max(5, int(os.environ.get("RAJA_TWELVE_DATA_CACHE_SECONDS", "6")))
TWELVE_DATA_FAILURE_COOLDOWN = max(30, int(os.environ.get("RAJA_TWELVE_DATA_FAILURE_COOLDOWN", "120")))
TWELVE_DATA_GLOBAL_RATE_LIMIT_COOLDOWN = max(30, int(os.environ.get("RAJA_TWELVE_DATA_RATE_LIMIT_COOLDOWN", "90")))
TWELVE_DATA_REQUEST_TIMEOUT_SECONDS = max(3.0, min(20.0, float(os.environ.get("RAJA_TWELVE_DATA_REQUEST_TIMEOUT", "9"))))
TWELVE_DATA_FETCH_CONCURRENCY = max(1, min(3, int(os.environ.get("RAJA_TWELVE_DATA_CONCURRENCY", "2"))))
TWELVE_DATA_MIN_GAP_SECONDS = max(0.0, float(os.environ.get("RAJA_TWELVE_DATA_MIN_GAP", "0.20")))

market_cache = {}
cache_lock = threading.RLock()

symbol_fetch_locks = {}
symbol_fetch_locks_guard = threading.Lock()
failed_symbol_until = {}
failed_symbol_lock = threading.Lock()

yahoo_fetch_semaphore = threading.BoundedSemaphore(YAHOO_FETCH_CONCURRENCY)
yahoo_pace_lock = threading.Lock()
last_yahoo_fetch_started = 0.0

# Twelve Data backup cache/circuit breaker. It is separate from Yahoo so the
# primary feed and backup feed never overwrite one another.
twelve_data_cache = {}
twelve_data_cache_lock = threading.RLock()
twelve_data_symbol_locks = {}
twelve_data_symbol_locks_guard = threading.Lock()
twelve_data_failed_until = {}
twelve_data_failed_lock = threading.Lock()
twelve_data_global_blocked_until = 0.0
twelve_data_global_lock = threading.Lock()
twelve_data_fetch_semaphore = threading.BoundedSemaphore(TWELVE_DATA_FETCH_CONCURRENCY)
twelve_data_pace_lock = threading.Lock()
last_twelve_data_fetch_started = 0.0

batch_cache = {}
batch_cache_lock = threading.RLock()
batch_key_locks = {}
batch_key_locks_guard = threading.Lock()

# Forex Factory exposes an official weekly JSON export from the calendar page.
# Proxy it through this backend so the browser avoids cross-origin issues and
# can keep a short cache instead of hitting the source on every click.
FOREX_FACTORY_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
MARKET_NEWS_CACHE_SECONDS = max(30, int(os.environ.get("RAJA_NEWS_CACHE_SECONDS", "60")))
market_news_cache = {"timestamp": 0.0, "data": []}
market_news_lock = threading.RLock()

# Duplicate rotation is handled client-side per browser, so one customer's
# scan never suppresses another customer's signal.
recent_signal_lock = threading.Lock()
recent_signals = {}
DUPLICATE_SIGNAL_COOLDOWN = 0


# =========================================================
# LICENSE STORE
# =========================================================

BASE_DIR = Path(__file__).resolve().parent

# One deterministic build ID for the deployed RAJA AI app.
# It changes whenever backend/frontend/PWA source changes, but not on a simple
# Render cold-start. Installed clients use it to detect a real new deployment.
def _compute_app_build_id():
    digest = hashlib.sha256()
    build_files = [
        Path(__file__).resolve(),
        BASE_DIR / "index.html",
        BASE_DIR / "sw.js",
        BASE_DIR / "manifest.json",
        BASE_DIR / "chart_scanner.html",
        BASE_DIR / "raja-ai-icon-192.png",
        BASE_DIR / "raja-ai-icon-512.png",
        BASE_DIR / "raja-ai-icon-192-v2.png",
        BASE_DIR / "raja-ai-icon-512-v2.png",
        BASE_DIR / "raja-ai-icon-192-v3.png",
        BASE_DIR / "raja-ai-icon-512-v3.png",
        BASE_DIR / "raja-splash-logo.png",
    ]
    found = False
    for path in build_files:
        try:
            if path.exists() and path.is_file():
                digest.update(path.name.encode("utf-8", errors="ignore"))
                digest.update(path.read_bytes())
                found = True
        except Exception:
            continue
    return digest.hexdigest()[:16] if found else "unknown"

APP_BUILD_ID = _compute_app_build_id()
APP_BUILD_TOKEN = "__RAJA_APP_BUILD__"

DATA_DIR = Path(os.environ.get("RAJA_DATA_DIR", str(BASE_DIR))).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

LICENSE_FILE = DATA_DIR / "licenses.json"
license_lock = threading.RLock()

ADMIN_PASSWORD = (os.environ.get("RAJA_ADMIN_PASSWORD") or "").strip()

# Quotex Forex OTC fallback + RAJA Quotex Bridge.
# The bridge receives market candles/ticks from the customer's own logged-in
# Quotex browser tab. No Quotex password/cookie is sent to RAJA AI.
RAJA_QUOTEX_OTC_URL = (os.environ.get("RAJA_QUOTEX_OTC_URL") or "https://qxbroker.com/en/").strip()
RAJA_QUOTEX_OTC_COMPANION_URL = (os.environ.get("RAJA_QUOTEX_OTC_COMPANION_URL") or "").strip()
RAJA_REQUIRE_QUOTEX_BRIDGE_FOR_OTC = str(os.environ.get("RAJA_REQUIRE_QUOTEX_BRIDGE_FOR_OTC", "0")).strip().lower() not in {"0", "false", "no", "off"}
# Keep the scanner usable when the browser bridge is connected but not yet streaming.
# Exact Quotex OTC candles are preferred; when they are missing, a clearly-labelled
# Yahoo/Twelve Data reference feed may be used instead of hard-blocking the whole bot.
RAJA_ALLOW_QUOTEX_REFERENCE_FALLBACK = True  # Forced ON for no-bridge OTC mode
RAJA_STRICT_BROKER_OTC = False  # Bridge-free OTC reference mode
QUOTEX_BRIDGE_PAIR_CODE_TTL_SECONDS = max(120, min(1800, int(os.environ.get("RAJA_QUOTEX_BRIDGE_PAIR_CODE_TTL", "600"))))
QUOTEX_BRIDGE_TOKEN_TTL_SECONDS = max(3600, min(31536000, int(os.environ.get("RAJA_QUOTEX_BRIDGE_TOKEN_TTL", str(30 * 24 * 3600)))))
QUOTEX_BRIDGE_MAX_CANDLES = max(1900, min(5000, int(os.environ.get("RAJA_QUOTEX_BRIDGE_MAX_CANDLES", "2500"))))
_QUOTEX_BRIDGE_SECRET_TEXT = (os.environ.get("RAJA_QUOTEX_BRIDGE_SECRET") or ADMIN_PASSWORD or "").strip()
if _QUOTEX_BRIDGE_SECRET_TEXT:
    QUOTEX_BRIDGE_SECRET = hashlib.sha256(_QUOTEX_BRIDGE_SECRET_TEXT.encode("utf-8")).digest()
else:
    # Functional fallback for local/testing. Set RAJA_QUOTEX_BRIDGE_SECRET in Railway
    # so paired extensions remain valid across server restarts/deployments.
    QUOTEX_BRIDGE_SECRET = secrets.token_bytes(32)

quotex_bridge_pair_codes = {}
quotex_bridge_pair_codes_lock = threading.RLock()
quotex_bridge_candles = {}
quotex_bridge_status = {}
quotex_bridge_data_lock = threading.RLock()

# Pocket Option uses the same signed pairing token but keeps a completely
# separate candle/status book so identical OTC pair names can never mix
# between brokers.  The browser extension uploads only parsed OHLC/ticks;
# session cookies/auth frames stay inside the broker tab.
pocket_bridge_candles = {}
pocket_bridge_status = {}
pocket_bridge_data_lock = threading.RLock()

# V29 bridge scan optimizer.  Pair freshness is tracked separately from the
# extension's global heartbeat so a live USD/JPY tab can never make an old
# EUR/JPY cache look current.  Auto Scan also uses this metadata to skip
# unavailable broker pairs immediately instead of spending the shared batch
# deadline on pairs that have no exact candles yet.
BROKER_BRIDGE_PAIR_FRESH_SECONDS = max(5.0, min(60.0, float(os.environ.get("RAJA_BRIDGE_PAIR_FRESH_SECONDS", "20"))))
BROKER_BRIDGE_MARKET_MAX_AGE_SECONDS = max(60.0, min(float(MAX_SOURCE_CANDLE_AGE_SECONDS), float(os.environ.get("RAJA_BRIDGE_MARKET_MAX_AGE_SECONDS", "180"))))
BROKER_BRIDGE_SCAN_MIN_CANDLES = max(30, min(300, int(os.environ.get("RAJA_BRIDGE_SCAN_MIN_CANDLES", "61"))))
quotex_bridge_pair_seen = {}
pocket_bridge_pair_seen = {}


# =========================================================
# RAJA QUOTEX OTC BRIDGE
# =========================================================
def _bridge_b64e(raw):
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _bridge_b64d(text):
    text = str(text or "")
    return base64.urlsafe_b64decode(text + ("=" * (-len(text) % 4)))


def _issue_quotex_bridge_token(user, device):
    payload = {
        "v": 1,
        "u": normalize_user_id(user),
        "d": str(device or "")[:160],
        "iat": int(time.time()),
        "exp": int(time.time()) + QUOTEX_BRIDGE_TOKEN_TTL_SECONDS,
    }
    encoded = _bridge_b64e(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    sig = _bridge_b64e(hmac.new(QUOTEX_BRIDGE_SECRET, encoded.encode("ascii"), hashlib.sha256).digest())
    return encoded + "." + sig


def _validate_quotex_bridge_token(token):
    try:
        encoded, sig = str(token or "").strip().split(".", 1)
        expected = _bridge_b64e(hmac.new(QUOTEX_BRIDGE_SECRET, encoded.encode("ascii"), hashlib.sha256).digest())
        if not hmac.compare_digest(sig, expected):
            return None
        payload = json.loads(_bridge_b64d(encoded).decode("utf-8"))
        if int(payload.get("v") or 0) != 1 or int(payload.get("exp") or 0) <= int(time.time()):
            return None
        user = normalize_user_id(payload.get("u"))
        device = str(payload.get("d") or "").strip()
        if not user or not device:
            return None
        return {"user": user, "device": device, "expires_at": int(payload["exp"])}
    except Exception:
        return None


def _quotex_bridge_pair_key(user, pair):
    return normalize_user_id(user), str(pair or "").strip()


def _normalize_bridge_epoch(value):
    try:
        value = float(value)
    except Exception:
        return None
    if value > 1000000000000:
        value /= 1000.0
    if value < 1000000000 or value > time.time() + 86400:
        return None
    return int(value)


def _bridge_upsert_candle(user, pair, candle):
    if pair not in YAHOO_SYMBOLS or "(OTC)" not in pair:
        return False
    if not isinstance(candle, dict):
        return False
    epoch = _normalize_bridge_epoch(candle.get("t", candle.get("time", candle.get("timestamp"))))
    try:
        o = float(candle.get("o", candle.get("open")))
        h = float(candle.get("h", candle.get("high")))
        l = float(candle.get("l", candle.get("low")))
        c = float(candle.get("c", candle.get("close")))
    except Exception:
        return False
    if epoch is None or min(o, h, l, c) <= 0 or h < max(o, c) or l > min(o, c):
        return False
    minute = int(epoch // 60 * 60)
    key = _quotex_bridge_pair_key(user, pair)
    with quotex_bridge_data_lock:
        book = quotex_bridge_candles.setdefault(key, OrderedDict())
        existing = book.get(minute)
        if existing:
            # Historical updates can refine the same minute; preserve the broadest H/L.
            existing["Open"] = float(existing.get("Open", o))
            existing["High"] = max(float(existing.get("High", h)), h)
            existing["Low"] = min(float(existing.get("Low", l)), l)
            existing["Close"] = c
        else:
            book[minute] = {"Open": o, "High": h, "Low": l, "Close": c, "Volume": 0.0}
        book.move_to_end(minute)
        while len(book) > QUOTEX_BRIDGE_MAX_CANDLES:
            book.popitem(last=False)
    return True


def _bridge_upsert_tick(user, pair, price, epoch=None):
    if pair not in YAHOO_SYMBOLS or "(OTC)" not in pair:
        return False
    try:
        price = float(price)
    except Exception:
        return False
    if price <= 0:
        return False
    epoch = _normalize_bridge_epoch(epoch) or int(time.time())
    minute = int(epoch // 60 * 60)
    key = _quotex_bridge_pair_key(user, pair)
    with quotex_bridge_data_lock:
        book = quotex_bridge_candles.setdefault(key, OrderedDict())
        row = book.get(minute)
        if row:
            row["High"] = max(float(row["High"]), price)
            row["Low"] = min(float(row["Low"]), price)
            row["Close"] = price
        else:
            book[minute] = {"Open": price, "High": price, "Low": price, "Close": price, "Volume": 0.0}
        book.move_to_end(minute)
        while len(book) > QUOTEX_BRIDGE_MAX_CANDLES:
            book.popitem(last=False)
    return True


def _set_quotex_bridge_status(user, device, pair=None, price=None, source_page=None):
    user = normalize_user_id(user)
    now = time.time()
    with quotex_bridge_data_lock:
        current = dict(quotex_bridge_status.get(user) or {})
        current.update({
            "connected": True,
            "last_seen": now,
            "device": str(device or "")[:160],
            "broker": "Quotex",
        })
        if pair:
            clean_pair = str(pair)[:120]
            current["pair"] = clean_pair
            quotex_bridge_pair_seen[_quotex_bridge_pair_key(user, clean_pair)] = now
        if price is not None:
            try: current["price"] = float(price)
            except Exception: pass
        if source_page:
            current["source_page"] = str(source_page)[:300]
        quotex_bridge_status[user] = current


def _bridge_book_snapshot(book, pair_seen_at):
    now = time.time()
    rows = list(book.items()) if book else []
    pair_seen_at = float(pair_seen_at or 0.0)
    upload_age = max(0.0, now - pair_seen_at) if pair_seen_at else None
    latest_epoch = int(rows[-1][0]) if rows else None
    market_age = max(0.0, now - float(latest_epoch)) if latest_epoch else None
    return rows, upload_age, latest_epoch, market_age


def _bridge_analysis_depth(candle_count):
    """Closed-candle depths suitable for the SK25 strategy engine (no indicator warm-up)."""
    count = max(0, int(candle_count or 0))
    thresholds = (("1m", 15), ("2m", 25), ("5m", 56), ("10m", 111), ("15m", 166), ("30m", 331))
    return [name for name, needed in thresholds if count >= needed]


def _sk25_required_base_candles(timeframe):
    """Approximate 1m rows needed to form >=11 closed candles of a requested timeframe."""
    tf = str(timeframe or "1m").strip().lower()
    minutes = TIMEFRAMES.get(tf, 1)
    return max(15, int(minutes) * 11 + 1)


def _bridge_required_base_candles(_min_tf=None):
    # Compatibility helper retained for older bridge status callers.
    return 15


def _get_quotex_bridge_status(user, pair=None):
    user = normalize_user_id(user)
    now = time.time()
    with quotex_bridge_data_lock:
        status = dict(quotex_bridge_status.get(user) or {})
        if pair:
            key = _quotex_bridge_pair_key(user, pair)
            book = quotex_bridge_candles.get(key)
            pair_seen = quotex_bridge_pair_seen.get(key)
            rows, pair_age, latest_epoch, market_age = _bridge_book_snapshot(book, pair_seen)
            status["candle_count"] = len(rows)
            status["pair_age_seconds"] = round(pair_age, 2) if pair_age is not None else None
            status["latest_candle_epoch"] = latest_epoch
            status["market_age_seconds"] = round(market_age, 2) if market_age is not None else None
            status["analysis_timeframes_ready"] = _bridge_analysis_depth(len(rows))
            status["scan_ready"] = bool(
                len(rows) >= BROKER_BRIDGE_SCAN_MIN_CANDLES
                and pair_age is not None and pair_age <= BROKER_BRIDGE_PAIR_FRESH_SECONDS
                and market_age is not None and market_age <= BROKER_BRIDGE_MARKET_MAX_AGE_SECONDS
            )
        else:
            status["pairs_with_data"] = sum(1 for (u, _p), book in quotex_bridge_candles.items() if u == user and book)
    last_seen = float(status.get("last_seen") or 0.0)
    age = max(0.0, now - last_seen) if last_seen else None
    status["age_seconds"] = round(age, 2) if age is not None else None
    status["connected"] = bool(last_seen and age <= BROKER_BRIDGE_PAIR_FRESH_SECONDS)
    status.setdefault("broker", "Quotex")
    return status


def get_quotex_bridge_market_data(user, pair):
    """Return the user's exact Quotex bridge 1m OHLC stream as a pandas DataFrame."""
    user = normalize_user_id(user)
    key = _quotex_bridge_pair_key(user, pair)
    with quotex_bridge_data_lock:
        book = quotex_bridge_candles.get(key)
        pair_seen = quotex_bridge_pair_seen.get(key)
        rows, pair_age, latest_epoch, market_age = _bridge_book_snapshot(book, pair_seen)
    source_info = {
        "source": "Quotex Browser Bridge",
        "source_mode": "broker_otc_exact",
        "provider_symbol": pair,
        "yahoo_symbol": YAHOO_SYMBOLS.get(pair),
        "backup_used": True,
        "exact_broker_feed": True,
        "browser_bridge": True,
        "candle_count": len(rows),
        "latest_candle_epoch": latest_epoch,
        "analysis_timeframes_ready": _bridge_analysis_depth(len(rows)),
    }
    if not rows:
        source_info["unavailable_reason"] = "Quotex Browser Bridge has no exact candles for this OTC pair yet. Open/stream the pair in Quotex first."
        return None, None, pair, source_info
    if pair_age is None or pair_age > BROKER_BRIDGE_PAIR_FRESH_SECONDS:
        source_info["unavailable_reason"] = f"Quotex Browser Bridge is not actively streaming this pair ({int(pair_age or 0)}s since its last upload)."
        return None, market_age, pair, source_info
    if market_age is None or market_age > BROKER_BRIDGE_MARKET_MAX_AGE_SECONDS:
        source_info["unavailable_reason"] = f"Quotex Browser Bridge cached candles for this pair are stale ({int(market_age or 0)}s market age)."
        return None, market_age, pair, source_info
    pd = _get_pandas()
    index = pd.to_datetime([epoch for epoch, _row in rows], unit="s", utc=True)
    frame = pd.DataFrame([row for _epoch, row in rows], index=index).sort_index()
    return frame, market_age, pair, source_info


# =========================================================
# RAJA POCKET OPTION OTC BROWSER BRIDGE
# =========================================================
def _pocket_bridge_pair_key(user, pair):
    return normalize_user_id(user), str(pair or "").strip()


def _pocket_bridge_upsert_candle(user, pair, candle):
    if pair not in YAHOO_SYMBOLS or "(OTC)" not in pair:
        return False
    if not isinstance(candle, dict):
        return False
    epoch = _normalize_bridge_epoch(candle.get("t", candle.get("time", candle.get("timestamp"))))
    try:
        o = float(candle.get("o", candle.get("open")))
        h = float(candle.get("h", candle.get("high")))
        l = float(candle.get("l", candle.get("low")))
        c = float(candle.get("c", candle.get("close")))
    except Exception:
        return False
    if epoch is None or min(o, h, l, c) <= 0 or h < max(o, c) or l > min(o, c):
        return False
    minute = int(epoch // 60 * 60)
    key = _pocket_bridge_pair_key(user, pair)
    with pocket_bridge_data_lock:
        book = pocket_bridge_candles.setdefault(key, OrderedDict())
        existing = book.get(minute)
        if existing:
            existing["Open"] = float(existing.get("Open", o))
            existing["High"] = max(float(existing.get("High", h)), h)
            existing["Low"] = min(float(existing.get("Low", l)), l)
            existing["Close"] = c
        else:
            book[minute] = {"Open": o, "High": h, "Low": l, "Close": c, "Volume": 0.0}
        book.move_to_end(minute)
        while len(book) > QUOTEX_BRIDGE_MAX_CANDLES:
            book.popitem(last=False)
    return True


def _pocket_bridge_upsert_tick(user, pair, price, epoch=None):
    if pair not in YAHOO_SYMBOLS or "(OTC)" not in pair:
        return False
    try:
        price = float(price)
    except Exception:
        return False
    if price <= 0:
        return False
    epoch = _normalize_bridge_epoch(epoch) or int(time.time())
    minute = int(epoch // 60 * 60)
    key = _pocket_bridge_pair_key(user, pair)
    with pocket_bridge_data_lock:
        book = pocket_bridge_candles.setdefault(key, OrderedDict())
        row = book.get(minute)
        if row:
            row["High"] = max(float(row["High"]), price)
            row["Low"] = min(float(row["Low"]), price)
            row["Close"] = price
        else:
            book[minute] = {"Open": price, "High": price, "Low": price, "Close": price, "Volume": 0.0}
        book.move_to_end(minute)
        while len(book) > QUOTEX_BRIDGE_MAX_CANDLES:
            book.popitem(last=False)
    return True


def _set_pocket_bridge_status(user, device, pair=None, price=None, source_page=None):
    user = normalize_user_id(user)
    now = time.time()
    with pocket_bridge_data_lock:
        current = dict(pocket_bridge_status.get(user) or {})
        current.update({
            "connected": True,
            "last_seen": now,
            "device": str(device or "")[:160],
            "broker": "Pocket Option",
        })
        if pair:
            clean_pair = str(pair)[:120]
            current["pair"] = clean_pair
            pocket_bridge_pair_seen[_pocket_bridge_pair_key(user, clean_pair)] = now
        if price is not None:
            try:
                current["price"] = float(price)
            except Exception:
                pass
        if source_page:
            current["source_page"] = str(source_page)[:300]
        pocket_bridge_status[user] = current


def _get_pocket_bridge_status(user, pair=None):
    user = normalize_user_id(user)
    now = time.time()
    with pocket_bridge_data_lock:
        status = dict(pocket_bridge_status.get(user) or {})
        if pair:
            key = _pocket_bridge_pair_key(user, pair)
            book = pocket_bridge_candles.get(key)
            pair_seen = pocket_bridge_pair_seen.get(key)
            rows, pair_age, latest_epoch, market_age = _bridge_book_snapshot(book, pair_seen)
            status["candle_count"] = len(rows)
            status["pair_age_seconds"] = round(pair_age, 2) if pair_age is not None else None
            status["latest_candle_epoch"] = latest_epoch
            status["market_age_seconds"] = round(market_age, 2) if market_age is not None else None
            status["analysis_timeframes_ready"] = _bridge_analysis_depth(len(rows))
            status["scan_ready"] = bool(
                len(rows) >= BROKER_BRIDGE_SCAN_MIN_CANDLES
                and pair_age is not None and pair_age <= BROKER_BRIDGE_PAIR_FRESH_SECONDS
                and market_age is not None and market_age <= BROKER_BRIDGE_MARKET_MAX_AGE_SECONDS
            )
        else:
            status["pairs_with_data"] = sum(1 for (u, _p), book in pocket_bridge_candles.items() if u == user and book)
    last_seen = float(status.get("last_seen") or 0.0)
    age = max(0.0, now - last_seen) if last_seen else None
    status["age_seconds"] = round(age, 2) if age is not None else None
    status["connected"] = bool(last_seen and age <= BROKER_BRIDGE_PAIR_FRESH_SECONDS)
    status.setdefault("broker", "Pocket Option")
    return status


def get_pocket_bridge_market_data(user, pair):
    """Return exact Pocket Option browser-tab 1m OHLC captured by the paired local extension."""
    user = normalize_user_id(user)
    key = _pocket_bridge_pair_key(user, pair)
    with pocket_bridge_data_lock:
        book = pocket_bridge_candles.get(key)
        pair_seen = pocket_bridge_pair_seen.get(key)
        rows, pair_age, latest_epoch, market_age = _bridge_book_snapshot(book, pair_seen)
    source_info = {
        "source": "Pocket Option Browser Bridge",
        "source_mode": "broker_otc_exact",
        "provider_symbol": pair,
        "yahoo_symbol": YAHOO_SYMBOLS.get(pair),
        "backup_used": True,
        "exact_broker_feed": True,
        "browser_bridge": True,
        "candle_count": len(rows),
        "latest_candle_epoch": latest_epoch,
        "analysis_timeframes_ready": _bridge_analysis_depth(len(rows)),
    }
    if not rows:
        source_info["unavailable_reason"] = "Pocket Option Browser Bridge has no exact candles for this OTC pair yet. Open/stream the pair in Pocket Option first."
        return None, None, pair, source_info
    if pair_age is None or pair_age > BROKER_BRIDGE_PAIR_FRESH_SECONDS:
        source_info["unavailable_reason"] = f"Pocket Option Browser Bridge is not actively streaming this pair ({int(pair_age or 0)}s since its last upload)."
        return None, market_age, pair, source_info
    if market_age is None or market_age > BROKER_BRIDGE_MARKET_MAX_AGE_SECONDS:
        source_info["unavailable_reason"] = f"Pocket Option Browser Bridge cached candles for this pair are stale ({int(market_age or 0)}s market age)."
        return None, market_age, pair, source_info
    pd = _get_pandas()
    index = pd.to_datetime([epoch for epoch, _row in rows], unit="s", utc=True)
    frame = pd.DataFrame([row for _epoch, row in rows], index=index).sort_index()
    return frame, market_age, pair, source_info


def _broker_bridge_ready_pairs(user, broker, requested_pairs=None):
    """Return fresh exact bridge pairs with per-pair depth metadata.

    This is intentionally read-only: it never opens/switches a broker asset and
    never falls back to Yahoo for OTC.
    """
    user = normalize_user_id(user)
    broker_key = str(broker or "").strip().casefold().replace(" ", "")
    is_pocket = broker_key in {"pocketoption", "pocket_option", "pocket"}
    if is_pocket:
        lock, books, seen_map, label = pocket_bridge_data_lock, pocket_bridge_candles, pocket_bridge_pair_seen, "Pocket Option"
    else:
        lock, books, seen_map, label = quotex_bridge_data_lock, quotex_bridge_candles, quotex_bridge_pair_seen, "Quotex"
    allow = set(str(p).strip() for p in (requested_pairs or []) if str(p).strip())
    now = time.time()
    rows = []
    with lock:
        for (u, pair), book in books.items():
            if u != user or not book or (allow and pair not in allow):
                continue
            count = len(book)
            pair_seen = float(seen_map.get((u, pair)) or 0.0)
            pair_age = max(0.0, now - pair_seen) if pair_seen else None
            latest_epoch = int(next(reversed(book))) if book else None
            market_age = max(0.0, now - latest_epoch) if latest_epoch else None
            tf_ready = _bridge_analysis_depth(count)
            stream_fresh = bool(pair_age is not None and pair_age <= BROKER_BRIDGE_PAIR_FRESH_SECONDS)
            market_fresh = bool(market_age is not None and market_age <= BROKER_BRIDGE_MARKET_MAX_AGE_SECONDS)
            rows.append({
                "pair": pair,
                "broker": label,
                "candle_count": count,
                "pair_age_seconds": round(pair_age, 2) if pair_age is not None else None,
                "market_age_seconds": round(market_age, 2) if market_age is not None else None,
                "latest_candle_epoch": latest_epoch,
                "analysis_timeframes_ready": tf_ready,
                "analysis_timeframes_ready_count": len(tf_ready),
                "stream_fresh": stream_fresh,
                "market_fresh": market_fresh,
                "scan_ready": bool(stream_fresh and market_fresh and count >= BROKER_BRIDGE_SCAN_MIN_CANDLES),
            })
    rows.sort(key=lambda row: str(row.get("pair") or ""))
    return rows


def _broker_bridge_cache_signature(user, broker, pairs):
    """Small immutable signature for safe batch-cache reuse.

    It changes when a pair gains backfilled candles or rolls into a new 1m
    bucket, so a 30-second cached WARMING result cannot hide newly-arrived exact
    broker data.
    """
    rows = _broker_bridge_ready_pairs(user, broker, pairs)
    meta = {row.get("pair"): row for row in rows}
    return tuple(
        (pair, int((meta.get(pair) or {}).get("candle_count") or 0), int((meta.get(pair) or {}).get("latest_candle_epoch") or 0))
        for pair in pairs
    )


# Permanent license storage:
# - Recommended on Render Free: set DATABASE_URL (for example a Neon/Supabase PostgreSQL URL).
# - If DATABASE_URL is absent, the app falls back to licenses.json. Render Free local files
#   are ephemeral, so the fallback is for local development/testing only.
DATABASE_URL = (os.environ.get("DATABASE_URL") or os.environ.get("RAJA_DATABASE_URL") or "").strip()
LICENSE_STORE_MODE = "postgres" if DATABASE_URL else "file"
DEVICE_SESSION_TTL_SECONDS = max(120, int(os.environ.get("RAJA_DEVICE_SESSION_TTL", "300")))
# User requested a one-time reset of all previously generated keys. This marker makes
# the reset run only once per persistent database. Change/empty the env var to control it.
LICENSE_RESET_VERSION = os.environ.get("RAJA_LICENSE_RESET_VERSION", "").strip()

SIGNALS_FILE = DATA_DIR / "signals.json"
signals_lock = threading.RLock()
SCAN_EVENTS_FILE = DATA_DIR / "scan_events.json"
scan_events_lock = threading.RLock()

# Free-trial anti-abuse store.
# A user/UID can claim a FREE TRIAL once, and after first login the device
# is also permanently recorded as having used a trial.
TRIAL_CLAIMS_FILE = DATA_DIR / "trial_claims.json"
trial_claims_lock = threading.RLock()

# Shared in-app broadcast + emergency scan-control state.
# PostgreSQL mode persists this inside raja_meta; file mode uses app_control.json.
APP_CONTROL_FILE = DATA_DIR / "app_control.json"
app_control_lock = threading.RLock()

DEFAULT_LICENSE_PLAN = "VIP"
AUTO_TRACK_EXPIRIES = {
    "1m": 60,
    "2m": 120,
    "5m": 300,
    "10m": 600,
    "15m": 900,
    "30m": 1800,
}


def normalize_user_id(value):
    """Canonicalize Telegram/user IDs so @Name and name resolve to the same customer."""
    user = str(value or "").strip()
    if user.startswith("@"):
        user = user[1:].strip()
    return user.casefold()


_license_store_ready = threading.Event()
_license_store_init_lock = threading.Lock()

# Small per-process PostgreSQL keep-alive pool. Reusing an already-authenticated
# connection avoids a fresh TCP/TLS/Postgres handshake on every login/heartbeat.
DB_CONNECT_TIMEOUT_SECONDS = max(3, min(10, int(os.environ.get("RAJA_DB_CONNECT_TIMEOUT", "5"))))
DB_POOL_SIZE = max(1, min(6, int(os.environ.get("RAJA_DB_POOL_SIZE", "3"))))
DB_POOL_IDLE_PING_SECONDS = max(20, min(300, int(os.environ.get("RAJA_DB_POOL_IDLE_PING", "60"))))
_db_pool = queue.LifoQueue(maxsize=DB_POOL_SIZE)


def _raw_db_connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    if psycopg is None:
        raise RuntimeError("psycopg is not installed; install requirements.txt")
    return psycopg.connect(
        DATABASE_URL,
        connect_timeout=DB_CONNECT_TIMEOUT_SECONDS,
        application_name="raja_ai_premium",
    )


def _discard_db_connection(conn):
    try:
        conn.close()
    except Exception:
        pass


def _acquire_db_connection():
    now = time.monotonic()
    while True:
        try:
            conn, last_used = _db_pool.get_nowait()
        except queue.Empty:
            return _raw_db_connect()

        if getattr(conn, "closed", False):
            _discard_db_connection(conn)
            continue

        # Only ping connections that have been idle for a while. This keeps the
        # normal login path to one real license query while safely replacing dead
        # connections after provider/network idle timeouts.
        if now - float(last_used or 0.0) >= DB_POOL_IDLE_PING_SECONDS:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
                conn.rollback()
            except Exception:
                _discard_db_connection(conn)
                continue
        return conn


def _release_db_connection(conn, broken=False):
    if conn is None:
        return
    if broken or getattr(conn, "closed", False):
        _discard_db_connection(conn)
        return
    try:
        _db_pool.put_nowait((conn, time.monotonic()))
    except queue.Full:
        _discard_db_connection(conn)


class _DbLease:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        broken = False
        try:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
        except Exception:
            broken = True
        _release_db_connection(self.conn, broken=broken)
        return False


def _prime_db_pool():
    if not DATABASE_URL or _db_pool.qsize() > 0:
        return
    conn = None
    try:
        conn = _raw_db_connect()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        conn.rollback()
        _release_db_connection(conn)
        conn = None
    finally:
        if conn is not None:
            _discard_db_connection(conn)


def _db_connect():
    # Database schema initialization is warmed in a background thread at startup.
    # If a DB-backed request arrives before warmup finishes, perform the same
    # one-time initialization safely, then borrow a warm pooled connection.
    if DATABASE_URL and not _license_store_ready.is_set():
        _ensure_license_store_initialized()
    return _DbLease(_acquire_db_connection())


def initialize_license_store():
    if DATABASE_URL:
        # Persistent license + scan analytics storage.
        with _raw_db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS raja_licenses (
                        license_key TEXT PRIMARY KEY,
                        active BOOLEAN NOT NULL DEFAULT TRUE,
                        user_id TEXT,
                        device_id TEXT,
                        device_label TEXT,
                        created_at BIGINT,
                        last_verified_at BIGINT,
                        session_token TEXT,
                        plan TEXT,
                        expires_at BIGINT,
                        last_login_at BIGINT
                    )
                """)
                for statement in [
                    "ALTER TABLE raja_licenses ADD COLUMN IF NOT EXISTS device_label TEXT",
                    "ALTER TABLE raja_licenses ADD COLUMN IF NOT EXISTS session_token TEXT",
                    "ALTER TABLE raja_licenses ADD COLUMN IF NOT EXISTS plan TEXT",
                    "ALTER TABLE raja_licenses ADD COLUMN IF NOT EXISTS expires_at BIGINT",
                    "ALTER TABLE raja_licenses ADD COLUMN IF NOT EXISTS last_login_at BIGINT",
                ]:
                    cur.execute(statement)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS raja_meta (
                        meta_key TEXT PRIMARY KEY,
                        meta_value TEXT
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS raja_scan_events (
                        event_id BIGSERIAL PRIMARY KEY,
                        created_at BIGINT NOT NULL,
                        user_id TEXT,
                        market TEXT,
                        pair TEXT,
                        mode TEXT,
                        signal_found BOOLEAN NOT NULL DEFAULT FALSE
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS raja_signals (
                        signal_id TEXT PRIMARY KEY,
                        user_id TEXT,
                        created_at BIGINT NOT NULL,
                        payload TEXT NOT NULL
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_raja_signals_user_created
                    ON raja_signals(user_id, created_at DESC)
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS raja_trial_claims (
                        claim_type TEXT NOT NULL,
                        claim_value TEXT NOT NULL,
                        license_key TEXT,
                        created_at BIGINT NOT NULL,
                        PRIMARY KEY (claim_type, claim_value)
                    )
                """)

                # Preserve the existing one-time reset marker behavior; do not change the version
                # during ordinary feature upgrades or existing customer keys would be deleted.
                if LICENSE_RESET_VERSION:
                    cur.execute("SELECT meta_value FROM raja_meta WHERE meta_key = %s", ("license_reset_version",))
                    row = cur.fetchone()
                    current_version = str(row[0]) if row and row[0] is not None else ""
                    if current_version != LICENSE_RESET_VERSION:
                        cur.execute("DELETE FROM raja_licenses")
                        cur.execute("""
                            INSERT INTO raja_meta (meta_key, meta_value)
                            VALUES (%s, %s)
                            ON CONFLICT (meta_key) DO UPDATE SET meta_value = EXCLUDED.meta_value
                        """, ("license_reset_version", LICENSE_RESET_VERSION))
                        print(f"RAJA license reset applied once: {LICENSE_RESET_VERSION}")
        print("RAJA license store: PostgreSQL (persistent)")
        return

    with license_lock:
        if not LICENSE_FILE.exists():
            LICENSE_FILE.write_text("{}", encoding="utf-8")
        if not SCAN_EVENTS_FILE.exists():
            SCAN_EVENTS_FILE.write_text("[]", encoding="utf-8")
        if not TRIAL_CLAIMS_FILE.exists():
            TRIAL_CLAIMS_FILE.write_text("{}", encoding="utf-8")
        if LICENSE_RESET_VERSION:
            marker_file = DATA_DIR / ".raja_license_reset_version"
            current_version = marker_file.read_text(encoding="utf-8").strip() if marker_file.exists() else ""
            if current_version != LICENSE_RESET_VERSION:
                LICENSE_FILE.write_text("{}", encoding="utf-8")
                marker_file.write_text(LICENSE_RESET_VERSION, encoding="utf-8")
                print(f"RAJA local license reset applied once: {LICENSE_RESET_VERSION}")
    print("WARNING: RAJA license store is using local file storage (not persistent on Render Free).")


def _ensure_license_store_initialized():
    if _license_store_ready.is_set():
        return
    with _license_store_init_lock:
        if _license_store_ready.is_set():
            return
        initialize_license_store()
        _license_store_ready.set()


def _background_license_store_warmup():
    # Railway/remote PostgreSQL can become reachable a few seconds after the web
    # process itself starts. Retry quietly in the background and keep one DB
    # connection warm so the customer's first login does not pay the handshake cost.
    last_error = None
    for delay in (0, 2, 5):
        if delay:
            time.sleep(delay)
        try:
            _ensure_license_store_initialized()
            _prime_db_pool()
            return
        except Exception as exc:
            last_error = exc
    # Do not prevent Flask/Gunicorn from serving /health and the cached web shell.
    # The first DB-backed request will still retry initialization synchronously.
    print(f"RAJA license store background warmup warning: {last_error}")


def _start_license_store_warmup():
    if DATABASE_URL:
        threading.Thread(
            target=_background_license_store_warmup,
            name="raja-license-store-warmup",
            daemon=True,
        ).start()
    else:
        _ensure_license_store_initialized()


def load_licenses():
    with license_lock:
        if DATABASE_URL:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT license_key, active, user_id, device_id, device_label, created_at,
                               last_verified_at, session_token, plan, expires_at, last_login_at
                        FROM raja_licenses
                    """)
                    rows = cur.fetchall()

            data = {}
            for (key, active, user, device, device_label, created_at, last_verified_at,
                 session_token, plan, expires_at, last_login_at) in rows:
                data[str(key)] = {
                    "active": bool(active),
                    "user": user,
                    "device": device,
                    "device_label": device_label,
                    "created_at": created_at,
                    "last_verified_at": last_verified_at,
                    "session_token": session_token,
                    "plan": plan or DEFAULT_LICENSE_PLAN,
                    "expires_at": expires_at,
                    "last_login_at": last_login_at,
                }
            return data

        if not LICENSE_FILE.exists():
            LICENSE_FILE.write_text("{}", encoding="utf-8")
            return {}
        try:
            data = json.loads(LICENSE_FILE.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                raise ValueError("Invalid license database")
            return data
        except Exception:
            try:
                backup = LICENSE_FILE.with_name(f"licenses.corrupt.{int(time.time())}.json")
                LICENSE_FILE.replace(backup)
            except Exception:
                pass
            LICENSE_FILE.write_text("{}", encoding="utf-8")
            return {}


def save_licenses(data):
    with license_lock:
        if DATABASE_URL:
            rows = []
            for key, record in (data or {}).items():
                record = record if isinstance(record, dict) else {}
                rows.append((
                    str(key), bool(record.get("active", False)), record.get("user"),
                    record.get("device"), record.get("device_label"), record.get("created_at"),
                    record.get("last_verified_at"), record.get("session_token"),
                    record.get("plan") or DEFAULT_LICENSE_PLAN, record.get("expires_at"),
                    record.get("last_login_at"),
                ))
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM raja_licenses")
                    if rows:
                        cur.executemany("""
                            INSERT INTO raja_licenses
                                (license_key, active, user_id, device_id, device_label, created_at,
                                 last_verified_at, session_token, plan, expires_at, last_login_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, rows)
            return
        temp = LICENSE_FILE.with_suffix(".tmp")
        temp.write_text(json.dumps(data, indent=2), encoding="utf-8")
        temp.replace(LICENSE_FILE)


def load_license_record(key):
    """Load one license without scanning the full PostgreSQL table."""
    key = str(key or "").strip()
    if not key:
        return None

    if DATABASE_URL:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT active, user_id, device_id, device_label, created_at,
                           last_verified_at, session_token, plan, expires_at, last_login_at
                    FROM raja_licenses
                    WHERE license_key = %s
                    LIMIT 1
                """, (key,))
                row = cur.fetchone()
        if not row:
            return None
        (active, user, device, device_label, created_at, last_verified_at,
         session_token, plan, expires_at, last_login_at) = row
        return {
            "active": bool(active),
            "user": user,
            "device": device,
            "device_label": device_label,
            "created_at": created_at,
            "last_verified_at": last_verified_at,
            "session_token": session_token,
            "plan": plan or DEFAULT_LICENSE_PLAN,
            "expires_at": expires_at,
            "last_login_at": last_login_at,
        }

    return load_licenses().get(key)


def save_license_record(key, record):
    """Upsert one license record; never DELETE + rebuild the whole license table."""
    key = str(key or "").strip()
    if not key:
        raise ValueError("License key is required")
    record = record if isinstance(record, dict) else {}

    if DATABASE_URL:
        values = (
            key, bool(record.get("active", False)), record.get("user"),
            record.get("device"), record.get("device_label"), record.get("created_at"),
            record.get("last_verified_at"), record.get("session_token"),
            record.get("plan") or DEFAULT_LICENSE_PLAN, record.get("expires_at"),
            record.get("last_login_at"),
        )
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO raja_licenses
                        (license_key, active, user_id, device_id, device_label, created_at,
                         last_verified_at, session_token, plan, expires_at, last_login_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (license_key) DO UPDATE SET
                        active = EXCLUDED.active,
                        user_id = EXCLUDED.user_id,
                        device_id = EXCLUDED.device_id,
                        device_label = EXCLUDED.device_label,
                        created_at = EXCLUDED.created_at,
                        last_verified_at = EXCLUDED.last_verified_at,
                        session_token = EXCLUDED.session_token,
                        plan = EXCLUDED.plan,
                        expires_at = EXCLUDED.expires_at,
                        last_login_at = EXCLUDED.last_login_at
                """, values)
        return

    with license_lock:
        licenses = load_licenses()
        licenses[key] = record
        save_licenses(licenses)



def find_active_license_for_user(user_ref):
    """Find one active, non-expired license for a user without rebuilding the license table."""
    user = normalize_user_id(user_ref)
    if not user:
        return None, None
    now = int(time.time())

    if DATABASE_URL:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT license_key, active, user_id, device_id, device_label, created_at,
                           last_verified_at, session_token, plan, expires_at, last_login_at
                    FROM raja_licenses
                    WHERE lower(user_id) = %s AND active = TRUE
                    ORDER BY created_at DESC NULLS LAST
                    LIMIT 20
                """, (user,))
                rows = cur.fetchall()
        for row in rows:
            key, active, bound_user, device, device_label, created_at, last_verified_at, session_token, plan, expires_at, last_login_at = row
            record = {
                "active": bool(active), "user": bound_user, "device": device,
                "device_label": device_label, "created_at": created_at,
                "last_verified_at": last_verified_at, "session_token": session_token,
                "plan": plan or DEFAULT_LICENSE_PLAN, "expires_at": expires_at,
                "last_login_at": last_login_at,
            }
            if not license_is_expired(record, now):
                return str(key), record
        return None, None

    for key, record in load_licenses().items():
        if (record.get("active", False)
                and normalize_user_id(record.get("user", "")) == user
                and not license_is_expired(record, now)):
            return key, record
    return None, None


def delete_license_record(key):
    key = str(key or "").strip()
    if not key:
        return False
    if DATABASE_URL:
        with license_lock:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM raja_licenses WHERE license_key=%s", (key,))
                    return cur.rowcount > 0
    with license_lock:
        licenses = load_licenses()
        existed = key in licenses
        licenses.pop(key, None)
        save_licenses(licenses)
        return existed


def clear_all_license_records():
    if DATABASE_URL:
        with license_lock:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM raja_licenses")
                    row = cur.fetchone()
                    removed = int(row[0] or 0) if row else 0
                    cur.execute("DELETE FROM raja_licenses")
                    return removed
    with license_lock:
        licenses = load_licenses()
        removed = len(licenses)
        save_licenses({})
        return removed


def reset_all_license_devices():
    """Clear device sessions atomically; return number of rows that were bound."""
    if DATABASE_URL:
        with license_lock:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT COUNT(*) FROM raja_licenses
                        WHERE device_id IS NOT NULL OR session_token IS NOT NULL
                    """)
                    row = cur.fetchone()
                    updated = int(row[0] or 0) if row else 0
                    cur.execute("""
                        UPDATE raja_licenses
                        SET device_id=NULL, device_label=NULL, last_verified_at=NULL, session_token=NULL
                    """)
                    cur.execute("SELECT COUNT(*) FROM raja_licenses")
                    total_row = cur.fetchone()
                    total = int(total_row[0] or 0) if total_row else 0
                    return updated, total
    with license_lock:
        licenses = load_licenses()
        updated = 0
        for key, record in list(licenses.items()):
            record = record if isinstance(record, dict) else {}
            if record.get("device") or record.get("session_token"):
                updated += 1
            record["device"] = None
            record["device_label"] = None
            record["last_verified_at"] = None
            record["session_token"] = None
            licenses[key] = record
        save_licenses(licenses)
        return updated, len(licenses)


def _trial_claim_key(claim_type, claim_value):
    claim_type = str(claim_type or "").strip().lower()
    claim_value = str(claim_value or "").strip()
    if claim_type == "user":
        claim_value = normalize_user_id(claim_value)
    return claim_type, claim_value


def get_trial_claim(claim_type, claim_value):
    """Return the previous FREE TRIAL claim, if any."""
    claim_type, claim_value = _trial_claim_key(claim_type, claim_value)
    if not claim_type or not claim_value:
        return None

    if DATABASE_URL:
        try:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT license_key, created_at FROM raja_trial_claims "
                        "WHERE claim_type=%s AND claim_value=%s",
                        (claim_type, claim_value),
                    )
                    row = cur.fetchone()
            if row:
                return {"license_key": row[0], "created_at": int(row[1] or 0)}
            return None
        except Exception as exc:
            print(f"Trial claim DB read warning: {exc}")

    with trial_claims_lock:
        if not TRIAL_CLAIMS_FILE.exists():
            TRIAL_CLAIMS_FILE.write_text("{}", encoding="utf-8")
        try:
            data = json.loads(TRIAL_CLAIMS_FILE.read_text(encoding="utf-8"))
        except Exception:
            data = {}
        item = data.get(f"{claim_type}:{claim_value}")
        return item if isinstance(item, dict) else None


def record_trial_claim(claim_type, claim_value, license_key):
    """Permanently record that a user/UID or device has consumed a FREE TRIAL."""
    claim_type, claim_value = _trial_claim_key(claim_type, claim_value)
    if not claim_type or not claim_value:
        return False
    license_key = str(license_key or "").strip()
    now = int(time.time())

    if DATABASE_URL:
        try:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO raja_trial_claims(claim_type, claim_value, license_key, created_at)
                        VALUES(%s,%s,%s,%s)
                        ON CONFLICT(claim_type, claim_value) DO NOTHING
                        """,
                        (claim_type, claim_value, license_key, now),
                    )
            return True
        except Exception as exc:
            print(f"Trial claim DB write warning: {exc}")

    with trial_claims_lock:
        if not TRIAL_CLAIMS_FILE.exists():
            TRIAL_CLAIMS_FILE.write_text("{}", encoding="utf-8")
        try:
            data = json.loads(TRIAL_CLAIMS_FILE.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                data = {}
        except Exception:
            data = {}
        key = f"{claim_type}:{claim_value}"
        if key not in data:
            data[key] = {"license_key": license_key, "created_at": now}
            temp = TRIAL_CLAIMS_FILE.with_suffix(".tmp")
            temp.write_text(json.dumps(data, indent=2), encoding="utf-8")
            temp.replace(TRIAL_CLAIMS_FILE)
    return True


def license_is_expired(record, now=None):
    expires_at = int((record or {}).get("expires_at") or 0)
    return bool(expires_at and expires_at <= int(now or time.time()))


def default_app_control_state():
    return {
        "maintenance": False,
        "maintenance_message": "RAJA AI scanning is temporarily paused for maintenance.",
        "broadcast": {
            "active": False,
            "id": "",
            "message": "",
            "level": "info",
            "created_at": 0,
        },
        "updated_at": 0,
    }


def normalize_app_control_state(raw):
    state = default_app_control_state()
    if isinstance(raw, dict):
        state["maintenance"] = bool(raw.get("maintenance", False))
        state["maintenance_message"] = str(
            raw.get("maintenance_message") or state["maintenance_message"]
        )[:300]
        state["updated_at"] = int(raw.get("updated_at") or 0)
        broadcast = raw.get("broadcast") if isinstance(raw.get("broadcast"), dict) else {}
        level = str(broadcast.get("level") or "info").lower()
        if level not in {"info", "warning", "critical"}:
            level = "info"
        state["broadcast"] = {
            "active": bool(broadcast.get("active", False)),
            "id": str(broadcast.get("id") or "")[:80],
            "message": str(broadcast.get("message") or "")[:500],
            "level": level,
            "created_at": int(broadcast.get("created_at") or 0),
        }
    return state


def load_app_control_state():
    if DATABASE_URL:
        try:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT meta_value FROM raja_meta WHERE meta_key=%s",
                        ("app_control_state",),
                    )
                    row = cur.fetchone()
            if row and row[0]:
                return normalize_app_control_state(json.loads(str(row[0])))
        except Exception as exc:
            print(f"App control DB read warning: {exc}")
        return default_app_control_state()

    with app_control_lock:
        if not APP_CONTROL_FILE.exists():
            APP_CONTROL_FILE.write_text(
                json.dumps(default_app_control_state(), indent=2), encoding="utf-8"
            )
        try:
            return normalize_app_control_state(
                json.loads(APP_CONTROL_FILE.read_text(encoding="utf-8"))
            )
        except Exception:
            return default_app_control_state()


def save_app_control_state(state):
    state = normalize_app_control_state(state)
    state["updated_at"] = int(time.time())
    payload = json.dumps(state, separators=(",", ":"))

    if DATABASE_URL:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO raja_meta(meta_key, meta_value)
                    VALUES(%s,%s)
                    ON CONFLICT(meta_key)
                    DO UPDATE SET meta_value=EXCLUDED.meta_value
                    """,
                    ("app_control_state", payload),
                )
        return state

    with app_control_lock:
        temp = APP_CONTROL_FILE.with_suffix(".tmp")
        temp.write_text(json.dumps(state, indent=2), encoding="utf-8")
        temp.replace(APP_CONTROL_FILE)
    return state


def scan_maintenance_state():
    state = load_app_control_state()
    return state if state.get("maintenance") else None


def _load_scan_events(limit=5000):
    limit = max(1, min(int(limit), 20000))
    if DATABASE_URL:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT created_at, user_id, market, pair, mode, signal_found
                    FROM raja_scan_events ORDER BY created_at DESC LIMIT %s
                """, (limit,))
                rows = cur.fetchall()
        return [
            {"created_at": int(ts), "user": user, "market": market, "pair": pair,
             "mode": mode, "signal_found": bool(found)}
            for ts, user, market, pair, mode, found in rows
        ]
    with scan_events_lock:
        if not SCAN_EVENTS_FILE.exists():
            SCAN_EVENTS_FILE.write_text("[]", encoding="utf-8")
        try:
            items = json.loads(SCAN_EVENTS_FILE.read_text(encoding="utf-8"))
            return items[:limit] if isinstance(items, list) else []
        except Exception:
            return []


def _append_scan_event(user, market, pair, mode, signal_found=False):
    item = {
        "created_at": int(time.time()), "user": normalize_user_id(user),
        "market": str(market or "Unknown")[:80], "pair": str(pair or "")[:120],
        "mode": str(mode or "BALANCED")[:30], "signal_found": bool(signal_found),
    }
    if DATABASE_URL:
        try:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO raja_scan_events(created_at,user_id,market,pair,mode,signal_found)
                        VALUES(%s,%s,%s,%s,%s,%s)
                    """, (item["created_at"], item["user"], item["market"], item["pair"], item["mode"], item["signal_found"]))
            return
        except Exception as exc:
            print(f"Scan analytics DB warning: {exc}")
    with scan_events_lock:
        items = _load_scan_events(5000)
        items.insert(0, item)
        temp = SCAN_EVENTS_FILE.with_suffix(".tmp")
        temp.write_text(json.dumps(items[:5000], indent=2), encoding="utf-8")
        temp.replace(SCAN_EVENTS_FILE)


def _auth_session(data):
    data = data or {}
    key = str(data.get("key", "")).strip()
    user = normalize_user_id(data.get("user", ""))
    device = str(data.get("device", "")).strip()
    session_token = str(data.get("session_token", "")).strip()
    if not key or not user or not device or not session_token:
        return None, (jsonify({"status": "error", "message": "Active license session required."}), 401)

    # Fast path: one indexed key lookup instead of loading every license row.
    record = load_license_record(key)
    now = int(time.time())
    if not record or not record.get("active", False) or license_is_expired(record, now):
        return None, (jsonify({"status": "error", "message": "Invalid, expired or revoked license key."}), 401)
    if normalize_user_id(record.get("user", "")) != user:
        return None, (jsonify({"status": "error", "message": "License is assigned to a different user."}), 403)
    if str(record.get("device") or "") != device or str(record.get("session_token") or "") != session_token:
        return None, (jsonify({"status": "error", "message": "This session was replaced by another device. Please login again."}), 409)
    return {"key": key, "user": user, "device": device, "record": record}, None


_start_license_store_warmup()


# =========================================================
# SIGNAL TRACKING
# =========================================================

def load_signals():
    """Load tracked signals from PostgreSQL when available, otherwise local JSON."""
    with signals_lock:
        if DATABASE_URL:
            try:
                with _db_connect() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT payload FROM raja_signals "
                            "ORDER BY created_at DESC LIMIT 5000"
                        )
                        rows = cur.fetchall()
                items = []
                for row in rows:
                    try:
                        item = json.loads(str(row[0]))
                        if isinstance(item, dict):
                            items.append(item)
                    except Exception:
                        continue
                return items
            except Exception as exc:
                print(f"Signal DB read warning: {exc}")

        if not SIGNALS_FILE.exists():
            SIGNALS_FILE.write_text("[]", encoding="utf-8")
            return []

        try:
            data = json.loads(SIGNALS_FILE.read_text(encoding="utf-8"))
            return data if isinstance(data, list) else []
        except Exception:
            return []


def save_signals(items):
    """Persist the current tracked-signal set."""
    with signals_lock:
        clean_items = [x for x in (items or []) if isinstance(x, dict)][:5000]

        if DATABASE_URL:
            try:
                rows = []
                for item in clean_items:
                    signal_id = str(item.get("id") or "").strip()
                    if not signal_id:
                        continue
                    rows.append((
                        signal_id,
                        normalize_user_id(item.get("user", "")),
                        int(item.get("created_at") or 0),
                        json.dumps(item, separators=(",", ":"), ensure_ascii=False),
                    ))
                with _db_connect() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM raja_signals")
                        if rows:
                            cur.executemany(
                                "INSERT INTO raja_signals(signal_id,user_id,created_at,payload) "
                                "VALUES(%s,%s,%s,%s) "
                                "ON CONFLICT (signal_id) DO UPDATE SET "
                                "user_id=EXCLUDED.user_id, "
                                "created_at=EXCLUDED.created_at, "
                                "payload=EXCLUDED.payload",
                                rows,
                            )
                return
            except Exception as exc:
                print(f"Signal DB write warning: {exc}")

        temp = SIGNALS_FILE.with_suffix(".tmp")
        temp.write_text(json.dumps(clean_items, indent=2), encoding="utf-8")
        temp.replace(SIGNALS_FILE)


def dataframe_epoch_rows(df):
    rows = []
    if df is None or df.empty:
        return rows

    for index_value, row in df.iterrows():
        try:
            epoch = int(index_value.timestamp())
        except Exception:
            continue
        rows.append((epoch, row))

    return rows


def resolve_tracked_signal(item):
    """Resolve a due signal from the same market source used for the scan."""
    pair = str(item.get("pair") or "")
    is_otc = "(OTC)" in pair
    if pair not in YAHOO_SYMBOLS and not is_otc:
        return False

    data = None
    resolved_source = None
    source_info = {}

    if is_otc:
        broker = str(item.get("broker") or "").strip()
        if not broker:
            src = str(item.get("source") or "").casefold()
            broker = "PocketOption" if "pocket" in src else "Quotex"

        if callable(get_native_broker_market_data):
            try:
                data, _age, _symbol, source_info = get_native_broker_market_data(broker, pair)
            except Exception as exc:
                source_info = {"unavailable_reason": f"Native result feed error: {type(exc).__name__}: {exc}"}
                data = None
        if data is not None and not getattr(data, "empty", True):
            resolved_source = str((source_info or {}).get("source") or "Broker Native WebSocket")
        elif str(broker).casefold().replace(" ", "") == "quotex" and item.get("user"):
            # Exact broker bridge fallback for outcome resolution too.
            bridge_df, _bridge_age, _bridge_symbol, bridge_info = get_quotex_bridge_market_data(item.get("user"), pair)
            if bridge_df is not None and not bridge_df.empty:
                data = bridge_df
                source_info = bridge_info or {}
                resolved_source = "Quotex Bridge"

        # Never calculate OTC WIN/LOSS from Yahoo/Twelve Data in strict mode.
        if data is None or getattr(data, "empty", True):
            item["result_wait_reason"] = (source_info or {}).get("unavailable_reason") or "Exact broker OTC result candles are not available yet."
            return False
    else:
        yahoo_symbol = YAHOO_SYMBOLS.get(pair)
        preferred_source = str(item.get("source") or "Yahoo Finance")
        if preferred_source == "Twelve Data" and TWELVE_DATA_ENABLED:
            data, _ = get_twelve_data_market_data(pair, force=True)
            if data is not None and not data.empty:
                resolved_source = "Twelve Data"
        if data is None or getattr(data, "empty", True):
            update_symbol_cache(yahoo_symbol, force=True)
            with cache_lock:
                cached = market_cache.get(yahoo_symbol)
            if cached:
                data = cached.get("data")
                resolved_source = "Yahoo Finance"
        if (data is None or getattr(data, "empty", True)) and TWELVE_DATA_ENABLED:
            data, _ = get_twelve_data_market_data(pair, force=True)
            if data is not None and not data.empty:
                resolved_source = "Twelve Data"

    rows = dataframe_epoch_rows(data)
    if not rows:
        return False

    entry_epoch = int(item.get("entry_epoch", 0))
    expiry_epoch = int(item.get("expiry_epoch", 0))
    entry_candidates = [(epoch, row) for epoch, row in rows if entry_epoch <= epoch < entry_epoch + 120]
    exit_candidates = [(epoch, row) for epoch, row in rows if max(entry_epoch, expiry_epoch - 120) <= epoch < expiry_epoch]
    if not entry_candidates or not exit_candidates:
        return False

    entry_epoch_actual, entry_row = entry_candidates[0]
    exit_epoch_actual, exit_row = exit_candidates[-1]
    try:
        entry_price = float(entry_row["Open"])
        exit_price = float(exit_row["Close"])
    except Exception:
        return False

    direction = item.get("signal")
    if exit_price == entry_price:
        result = "DRAW"
    elif direction == "CALL":
        result = "WIN" if exit_price > entry_price else "LOSS"
    elif direction == "PUT":
        result = "WIN" if exit_price < entry_price else "LOSS"
    else:
        return False

    item["entry_price"] = round(entry_price, 8)
    item["exit_price"] = round(exit_price, 8)
    item["entry_candle_epoch"] = entry_epoch_actual
    item["exit_candle_epoch"] = exit_epoch_actual
    item["result"] = result
    item["status"] = "COMPLETED"
    item["resolved_at"] = int(time.time())
    item["result_source"] = resolved_source or "same_scan_source"
    item["result_reference_source"] = resolved_source or "same_scan_source"
    item.pop("result_wait_reason", None)
    if not is_otc:
        item["reference_result"] = result
        if resolved_source == "Yahoo Finance":
            item["yahoo_result"] = result
        elif resolved_source == "Twelve Data":
            item["backup_result"] = result
    else:
        item["exact_broker_result"] = True
    return True

def tracked_signal_phase(item, now=None):
    """Return the live lifecycle phase used by the browser Signal Flow UI."""
    item = item or {}
    now = int(now or time.time())
    result = str(item.get("result") or "").upper()
    status = str(item.get("status") or "").upper()

    if result in {"WIN", "LOSS", "DRAW"} or status == "COMPLETED":
        return "COMPLETED"
    if status == "AWAITING_QX":
        return "QX_VERIFY"

    entry_epoch = int(item.get("entry_epoch") or 0)
    expiry_epoch = int(item.get("expiry_epoch") or 0)
    if entry_epoch and now < entry_epoch:
        return "WAITING_ENTRY"
    if entry_epoch and now <= entry_epoch + 4:
        return "ENTRY_OPEN"
    if expiry_epoch and now < expiry_epoch:
        return "TRACKING"
    if expiry_epoch:
        return "RESOLVING"
    return "PENDING"


def resolve_due_signals(items, now=None):
    """Resolve all due outcomes once. Returns True when stored data changed."""
    changed = False
    now = int(now or time.time())

    for item in items:
        item_status = str(item.get("status") or "").upper()
        if item_status not in {"PENDING", "AWAITING_QX"}:
            continue
        if item_status == "AWAITING_QX" and (item.get("reference_result") or item.get("yahoo_result") or item.get("backup_result")):
            continue

        expiry_epoch = int(item.get("expiry_epoch") or 0)
        if not expiry_epoch or now < expiry_epoch + 8:
            continue

        pair = str(item.get("pair", ""))

        # V27 resolves OTC only from the broker-native feed (or exact Quotex
        # bridge backup). If exact data is unavailable, leave it pending and retry.
        if resolve_tracked_signal(item):
            changed = True

    return changed


def signal_outcome_worker():
    while True:
        try:
            with signals_lock:
                items = load_signals()
                if resolve_due_signals(items):
                    save_signals(items)
        except Exception as e:
            print(f"Signal outcome worker error: {e}")

        time.sleep(10)


def signal_stats(items):
    completed = [x for x in items if x.get("status") == "COMPLETED"]
    wins = sum(1 for x in completed if x.get("result") == "WIN")
    losses = sum(1 for x in completed if x.get("result") == "LOSS")
    draws = sum(1 for x in completed if x.get("result") == "DRAW")
    decided = wins + losses

    return {
        "completed": len(completed),
        "wins": wins,
        "losses": losses,
        "draws": draws,
        "observed_win_rate": round((wins / decided) * 100, 2) if decided else None,
    }


# =========================================================
# ACCURACY V24: HISTORICAL PERFORMANCE + CALIBRATION
# =========================================================
# Historical outcomes are used conservatively. Small samples stay in LEARNING
# mode and never hard-block an otherwise valid technical setup.
PERFORMANCE_HISTORY_CACHE_SECONDS = max(5, int(os.environ.get("RAJA_PERFORMANCE_CACHE_SECONDS", "20")))
performance_history_cache = {}
performance_history_cache_lock = threading.RLock()


def _completed_performance_history(user):
    """Return recent decided WIN/LOSS rows for one user, cached briefly for batch scans."""
    user = normalize_user_id(user)
    if not user:
        return []
    now = time.time()
    with performance_history_cache_lock:
        cached = performance_history_cache.get(user)
        if cached and now - float(cached.get("timestamp") or 0.0) <= PERFORMANCE_HISTORY_CACHE_SECONDS:
            return list(cached.get("rows") or [])

    rows = []
    try:
        for item in load_signals():
            if normalize_user_id(item.get("user", "")) != user:
                continue
            if str(item.get("status") or "").upper() != "COMPLETED":
                continue
            if str(item.get("result") or "").upper() not in {"WIN", "LOSS"}:
                continue
            if item.get("exclude_from_performance"):
                continue
            rows.append(item)
            if len(rows) >= 1000:
                break
    except Exception as exc:
        print(f"Performance history warning: {exc}")
        rows = []

    with performance_history_cache_lock:
        performance_history_cache[user] = {"timestamp": time.time(), "rows": list(rows)}
    return rows


def pair_timeframe_performance(user, pair, expiry):
    """Conservative pair+expiry quality profile. Hard gates require a meaningful sample."""
    expiry = str(expiry or "").strip()
    rows = [
        x for x in _completed_performance_history(user)
        if str(x.get("pair") or "") == str(pair or "")
        and (not expiry or str(x.get("expiry") or "") == expiry)
    ][:240]
    wins = sum(1 for x in rows if str(x.get("result") or "").upper() == "WIN")
    losses = sum(1 for x in rows if str(x.get("result") or "").upper() == "LOSS")
    n = wins + losses
    raw_rate = (wins / n * 100.0) if n else None
    # Beta(2,2) smoothing prevents small streaks from producing extreme estimates.
    smoothed = ((wins + 2.0) / (n + 4.0) * 100.0) if n else 50.0

    if n < 12:
        status = "LEARNING"
        threshold_raise = 0.0
        score_adjustment = 0.0
    elif smoothed >= 65.0:
        status = "STRONG"
        threshold_raise = 0.0
        score_adjustment = 1.5
    elif smoothed >= 52.0:
        status = "NORMAL"
        threshold_raise = 0.0
        score_adjustment = 0.0
    elif smoothed >= 45.0:
        status = "CAUTION"
        threshold_raise = 2.0
        score_adjustment = -1.5
    else:
        status = "WEAK"
        threshold_raise = 4.0
        score_adjustment = -3.0

    # Never let a short bad streak shut a pair down. Require 25+ decided trades.
    hard_block = bool(n >= 25 and smoothed < 43.0)
    return {
        "sample_size": n,
        "wins": wins,
        "losses": losses,
        "raw_win_rate": round(raw_rate, 2) if raw_rate is not None else None,
        "smoothed_win_rate": round(smoothed, 2),
        "status": status,
        "threshold_raise": threshold_raise,
        "score_adjustment": score_adjustment,
        "hard_block": hard_block,
        "minimum_gate_sample": 12,
        "hard_block_sample": 25,
    }



# =========================================================
# STRATEGY LEARNING V1: PER-TYPE PERFORMANCE ADAPTATION
# =========================================================
STRATEGY_LEARNING_MIN_SAMPLE = max(20, int(os.environ.get("RAJA_STRATEGY_LEARNING_MIN_SAMPLE", "30")))
STRATEGY_LEARNING_STRONG_SAMPLE = max(STRATEGY_LEARNING_MIN_SAMPLE, int(os.environ.get("RAJA_STRATEGY_LEARNING_STRONG_SAMPLE", "60")))

def _strategy_recent_streak(rows):
    streak = 0
    worst = 0
    for item in rows:
        r = str(item.get("result") or "").upper()
        if r == "LOSS":
            streak += 1
            worst = max(worst, streak)
        elif r == "WIN":
            streak = 0
    return worst

def strategy_performance_profile(user, pattern_type):
    """Return a conservative user-specific profile for one RAJA strategy type.

    No strategy is judged before a meaningful sample exists. A hard disable requires
    60+ decided trades, poor smoothed performance, weak recent form and a meaningful
    losing streak, so one short bad run cannot switch a strategy off.
    """
    try:
        ptype = int(pattern_type or 0)
    except Exception:
        ptype = 0
    rows = [x for x in _completed_performance_history(user)
            if int(x.get("pattern_type") or 0) == ptype]
    rows.sort(key=lambda x: int(x.get("resolved_at") or x.get("created_at") or 0), reverse=True)
    rows = rows[:400]
    wins = sum(1 for x in rows if str(x.get("result") or "").upper() == "WIN")
    losses = sum(1 for x in rows if str(x.get("result") or "").upper() == "LOSS")
    n = wins + losses
    raw = (wins / n * 100.0) if n else None
    smoothed = ((wins + 3.0) / (n + 6.0) * 100.0) if n else 50.0
    recent = rows[:20]
    rw = sum(1 for x in recent if str(x.get("result") or "").upper() == "WIN")
    rl = sum(1 for x in recent if str(x.get("result") or "").upper() == "LOSS")
    rn = rw + rl
    recent_rate = (rw / rn * 100.0) if rn else None
    worst_streak = _strategy_recent_streak(list(reversed(rows[:80])))

    status = "LEARNING"
    adjustment = 0
    hard_disable = False
    if n >= STRATEGY_LEARNING_MIN_SAMPLE:
        if smoothed >= 62.0:
            status = "STRONG"; adjustment = 5 if n < STRATEGY_LEARNING_STRONG_SAMPLE else 8
        elif smoothed >= 52.0:
            status = "NORMAL"; adjustment = 0
        elif smoothed >= 47.0:
            status = "CAUTION"; adjustment = -4 if n < STRATEGY_LEARNING_STRONG_SAMPLE else -7
        else:
            status = "WEAK"; adjustment = -8 if n < STRATEGY_LEARNING_STRONG_SAMPLE else -12

    if n >= STRATEGY_LEARNING_STRONG_SAMPLE:
        recent_weak = recent_rate is not None and recent_rate < 50.0
        hard_disable = bool(smoothed < 45.0 and recent_weak and worst_streak >= 5)
        if hard_disable:
            status = "DISABLED_PENDING_REVIEW"

    return {
        "pattern_type": ptype,
        "name": RAJA_STRATEGY_NAMES.get(ptype, f"RAJA Strategy {ptype}"),
        "sample_size": n, "wins": wins, "losses": losses,
        "raw_win_rate": round(raw, 2) if raw is not None else None,
        "smoothed_win_rate": round(smoothed, 2),
        "recent_20_sample": rn,
        "recent_20_win_rate": round(recent_rate, 2) if recent_rate is not None else None,
        "max_losing_streak": worst_streak,
        "status": status,
        "priority_adjustment": adjustment,
        "hard_disable": hard_disable,
        "minimum_sample": STRATEGY_LEARNING_MIN_SAMPLE,
        "strong_sample": STRATEGY_LEARNING_STRONG_SAMPLE,
    }

def strategy_learning_summary(user):
    return [strategy_performance_profile(user, i) for i in range(1, 26)]

def _apply_strategy_learning(strategy, user):
    """Re-rank same-direction exact setups and conservatively block proven weak types."""
    strategy = dict(strategy or {})
    if not user or strategy.get("signal") not in {"CALL", "PUT"}:
        return strategy

    selected_type = int(strategy.get("pattern_type") or 0)
    candidates = [x for x in (strategy.get("pattern_signals") or []) if isinstance(x, dict)]
    direction = str(strategy.get("pattern_direction") or "").upper()
    exact_same = [x for x in candidates if str(x.get("direction") or "").upper() == direction and float(x.get("score") or 0) >= 99.9]

    ranked = []
    for c in exact_same:
        profile = strategy_performance_profile(user, c.get("pattern_type"))
        if profile.get("hard_disable"):
            continue
        adjusted = int(c.get("priority") or 100) + int(profile.get("priority_adjustment") or 0)
        ranked.append((adjusted, int(c.get("rules_total") or 0), c, profile))

    if ranked:
        ranked.sort(key=lambda z: (z[0], z[1]), reverse=True)
        _adj, _rules, best, profile = ranked[0]
        if int(best.get("pattern_type") or 0) != selected_type:
            strategy.update({
                "signal": best.get("signal") or strategy.get("signal"),
                "pattern_type": int(best.get("pattern_type") or 0),
                "selected_pattern": best.get("name") or strategy.get("selected_pattern"),
                "pattern_direction": best.get("direction") or strategy.get("pattern_direction"),
                "next_candle_color": best.get("next_candle") or strategy.get("next_candle_color"),
                "rules": list(best.get("rules") or []),
                "setup": best.get("setup") or "",
                "family": best.get("family") or "",
                "recovery_trade": bool(best.get("recovery_trade")),
                "timeframe_rule": best.get("timeframe_rule"),
                "pattern_priority": int(best.get("priority") or 100),
                "rules_matched": int(best.get("rules_matched") or 0),
                "rules_total": int(best.get("rules_total") or 0),
                "reason": f"{best.get('name')} exact setup selected after strategy-learning priority adjustment.",
            })
    else:
        profile = strategy_performance_profile(user, selected_type)
        if profile.get("hard_disable"):
            strategy.update({
                "raw_signal_before_strategy_learning": strategy.get("signal"),
                "signal": "NO SIGNAL", "pattern_direction": "NONE",
                "next_candle_color": "NONE", "no_trade": True,
                "strategy_learning_blocked": True,
                "reason": f"NO TRADE · Strategy Type {selected_type} is temporarily disabled pending review after {profile.get('sample_size')} decided trades.",
            })

    final_profile = strategy_performance_profile(user, strategy.get("pattern_type"))
    strategy["strategy_learning"] = final_profile
    strategy["strategy_learning_applied"] = True
    strategy["adaptive_pattern_priority"] = int(strategy.get("pattern_priority") or 100) + int(final_profile.get("priority_adjustment") or 0)
    return strategy



# =========================================================
# ADAPTIVE LEARNING V2: PAIR + TIMEFRAME + REGIME QUALITY
# =========================================================
ADAPTIVE_CONTEXT_MIN_SAMPLE = max(12, int(os.environ.get("RAJA_ADAPTIVE_CONTEXT_MIN_SAMPLE", "20")))
ADAPTIVE_CONTEXT_STRONG_SAMPLE = max(ADAPTIVE_CONTEXT_MIN_SAMPLE, int(os.environ.get("RAJA_ADAPTIVE_CONTEXT_STRONG_SAMPLE", "40")))
ADAPTIVE_COOLDOWN_LOSSES = max(2, int(os.environ.get("RAJA_ADAPTIVE_COOLDOWN_LOSSES", "3")))

def _current_loss_streak(rows):
    streak = 0
    for item in rows:
        r = str(item.get("result") or "").upper()
        if r == "LOSS": streak += 1
        elif r == "WIN": break
    return streak

def classify_market_regime(tf_df):
    """Simple closed-candle regime classifier; no indicators and no forming candle use."""
    candles = _candle_rows(tf_df, 40)
    if len(candles) < 10:
        return {"regime":"UNKNOWN","trend":0.0,"flip_rate":0.0,"range_multiple":1.0}
    recent = candles[-12:]
    trend = _sk25_trend(candles, 10)
    dirs = [x["dir"] for x in recent if x["dir"]]
    flips = sum(1 for a,b in zip(dirs, dirs[1:]) if a != b)
    flip_rate = flips / max(1, len(dirs)-1)
    ranges = [x["range"] for x in candles[-24:]]
    med = max(_median(ranges, 1e-12), 1e-12)
    range_multiple = recent[-1]["range"] / med
    if range_multiple >= 2.8:
        regime = "BREAKOUT"
    elif abs(trend) >= 0.22 and flip_rate <= 0.62:
        regime = "TREND_UP" if trend > 0 else "TREND_DOWN"
    elif flip_rate >= 0.72:
        regime = "CHOPPY"
    else:
        regime = "SIDEWAYS"
    return {"regime":regime,"trend":round(trend,4),"flip_rate":round(flip_rate,4),"range_multiple":round(range_multiple,3)}

def _strategy_regime_fit(pattern_type, regime, direction):
    p = int(pattern_type or 0); regime = str(regime or "UNKNOWN").upper(); direction=str(direction or "").upper()
    continuation = {1,9,10,16,17,20,21,22,23}
    sideways = {2,3,4,5,12,18,19,24}
    breakout = {14,25}
    reversal = {13,15}
    if regime == "CHOPPY" and p in continuation:
        return -9, "Continuation setup suppressed in choppy regime."
    if regime == "TREND_UP" and p in continuation and direction == "UP": return 5, "Trend-aligned continuation."
    if regime == "TREND_DOWN" and p in continuation and direction == "DOWN": return 5, "Trend-aligned continuation."
    if regime in {"TREND_UP","TREND_DOWN"} and p in sideways: return -3, "Sideways/rejection setup has lower weight in trend."
    if regime == "SIDEWAYS" and p in sideways: return 4, "Sideways/rejection setup matches regime."
    if regime == "BREAKOUT" and p in breakout: return 5, "Breakout strategy matches expansion regime."
    if regime == "BREAKOUT" and p in reversal: return -4, "Reversal needs extra caution during active breakout."
    return 0, "Neutral regime fit."

def adaptive_context_profile(user, pair, timeframe, pattern_type):
    try: ptype=int(pattern_type or 0)
    except Exception: ptype=0
    rows=[x for x in _completed_performance_history(user)
          if int(x.get("pattern_type") or 0)==ptype
          and str(x.get("pair") or "")==str(pair or "")
          and str(x.get("expiry") or x.get("timeframe") or "")==str(timeframe or "")]
    rows.sort(key=lambda x:int(x.get("resolved_at") or x.get("created_at") or 0), reverse=True)
    rows=rows[:250]
    wins=sum(1 for x in rows if str(x.get("result") or "").upper()=="WIN")
    losses=sum(1 for x in rows if str(x.get("result") or "").upper()=="LOSS")
    n=wins+losses
    smoothed=((wins+3.0)/(n+6.0)*100.0) if n else 50.0
    recent=rows[:20]; rw=sum(1 for x in recent if str(x.get("result") or "").upper()=="WIN"); rl=sum(1 for x in recent if str(x.get("result") or "").upper()=="LOSS")
    rn=rw+rl; recent_rate=(rw/rn*100.0) if rn else None
    loss_streak=_current_loss_streak(rows)
    status="LEARNING"; adjustment=0; hard_block=False
    if n>=ADAPTIVE_CONTEXT_MIN_SAMPLE:
        if smoothed>=64: status="STRONG"; adjustment=7 if n<ADAPTIVE_CONTEXT_STRONG_SAMPLE else 10
        elif smoothed>=54: status="NORMAL"; adjustment=1
        elif smoothed>=48: status="CAUTION"; adjustment=-5
        else: status="WEAK"; adjustment=-10
    cooldown = bool(n>=ADAPTIVE_CONTEXT_MIN_SAMPLE and loss_streak>=ADAPTIVE_COOLDOWN_LOSSES)
    if cooldown: status="COOLDOWN"; adjustment=min(adjustment,-12)
    if n>=ADAPTIVE_CONTEXT_STRONG_SAMPLE and smoothed<44 and (recent_rate is not None and recent_rate<45):
        hard_block=True; status="BLOCKED"
    return {
      "pattern_type":ptype,"pair":pair,"timeframe":timeframe,"sample_size":n,"wins":wins,"losses":losses,
      "smoothed_win_rate":round(smoothed,2),"recent_20_win_rate":round(recent_rate,2) if recent_rate is not None else None,
      "current_loss_streak":loss_streak,"status":status,"priority_adjustment":adjustment,"cooldown":cooldown,"hard_block":hard_block,
      "minimum_sample":ADAPTIVE_CONTEXT_MIN_SAMPLE,"strong_sample":ADAPTIVE_CONTEXT_STRONG_SAMPLE
    }

def _apply_adaptive_context(strategy, user, pair, timeframe, tf_df):
    strategy=dict(strategy or {})
    if not user or strategy.get("signal") not in {"CALL","PUT"}: return strategy
    ptype=int(strategy.get("pattern_type") or 0)
    profile=adaptive_context_profile(user,pair,timeframe,ptype)
    regime=classify_market_regime(tf_df)
    direction=str(strategy.get("pattern_direction") or ("UP" if strategy.get("signal")=="CALL" else "DOWN"))
    regime_adj, regime_reason=_strategy_regime_fit(ptype,regime.get("regime"),direction)
    total_adj=int(profile.get("priority_adjustment") or 0)+int(regime_adj)
    strategy["adaptive_context_learning"]=profile
    strategy["market_regime_profile"]=regime
    strategy["regime_priority_adjustment"]=regime_adj
    strategy["adaptive_context_priority_adjustment"]=total_adj
    strategy["adaptive_pattern_priority"]=int(strategy.get("adaptive_pattern_priority") or strategy.get("pattern_priority") or 100)+total_adj
    strategy["adaptive_context_reason"]=regime_reason
    # V76 BALANCED SIGNAL MODE:
    # A new user no longer needs a large pre-existing win/loss history before a
    # high-quality BALANCED near-match can be released. The market-health gate,
    # opposite-direction conflict gate, cooldown, weak-history hard block and
    # choppy-regime protections below still remain active.
    if strategy.get("balanced_acceptance_applied"):
        strategy["near_match_history_gate_bypassed"] = True
        strategy["adaptive_context_reason"] = (
            str(strategy.get("adaptive_context_reason") or "").rstrip(".")
            + f" · V76 balanced near-match allowed; profile {profile.get('status')} (n={profile.get('sample_size')})."
        ).strip()
    # Safety floor: enough evidence + genuinely weak pair/timeframe combination can block.
    if profile.get("hard_block"):
        strategy.update({"raw_signal_before_adaptive_context":strategy.get("signal"),"signal":"NO SIGNAL","pattern_direction":"NONE",
                         "next_candle_color":"NONE","no_trade":True,"adaptive_context_blocked":True,
                         "reason":f"NO TRADE · Type {ptype} on {pair} {timeframe} is blocked by resolved performance history (n={profile.get('sample_size')})."})
    elif profile.get("cooldown"):
        strategy.update({"raw_signal_before_adaptive_context":strategy.get("signal"),"signal":"NO SIGNAL","pattern_direction":"NONE",
                         "next_candle_color":"NONE","no_trade":True,"adaptive_context_cooldown":True,
                         "reason":f"NO TRADE · Temporary cooldown after {profile.get('current_loss_streak')} consecutive resolved losses for Type {ptype} on {pair} {timeframe}."})
    elif regime.get("regime")=="CHOPPY" and ptype in {1,9,10,16,17,20,21,22,23}:
        strategy.update({"raw_signal_before_regime_gate":strategy.get("signal"),"signal":"NO SIGNAL","pattern_direction":"NONE",
                         "next_candle_color":"NONE","no_trade":True,"regime_gate_blocked":True,
                         "reason":f"NO TRADE · {regime_reason}"})
    return strategy

def adaptive_context_summary(user, limit=120):
    rows=[]
    # Build only from combinations that actually exist in resolved history.
    combos=set()
    for x in _completed_performance_history(user):
        try: p=int(x.get("pattern_type") or 0)
        except Exception: p=0
        pair=str(x.get("pair") or ""); tf=str(x.get("expiry") or x.get("timeframe") or "")
        if p and pair and tf: combos.add((pair,tf,p))
    for pair,tf,p in combos:
        rows.append(adaptive_context_profile(user,pair,tf,p))
    rows.sort(key=lambda x:(x.get("sample_size",0),x.get("smoothed_win_rate",0)), reverse=True)
    return rows[:max(1,int(limit))]


def calibrate_confidence(user, expiry, technical_quality):
    """Calibrate displayed confidence from similar historical score buckets; never gates trades."""
    technical_quality = max(0.0, min(100.0, float(technical_quality or 0.0)))
    expiry = str(expiry or "").strip()
    rows = []
    for item in _completed_performance_history(user):
        if expiry and str(item.get("expiry") or "") != expiry:
            continue
        try:
            historical_score = float(item.get("deep_quality_score") or item.get("score") or 0.0)
        except Exception:
            historical_score = 0.0
        if abs(historical_score - technical_quality) <= 8.0:
            rows.append(item)
        if len(rows) >= 300:
            break

    wins = sum(1 for x in rows if str(x.get("result") or "").upper() == "WIN")
    losses = sum(1 for x in rows if str(x.get("result") or "").upper() == "LOSS")
    n = wins + losses
    if n < 20:
        return {
            "status": "LEARNING",
            "calibrated_confidence": round(technical_quality, 2),
            "sample_size": n,
            "observed_win_rate": None,
            "note": "Calibration is collecting outcomes; technical quality is shown until n>=20.",
        }

    observed = wins / n * 100.0
    smoothed = (wins + 2.0) / (n + 4.0) * 100.0
    # Blend observed reliability with current technical quality so one historical regime
    # cannot completely override the live setup. This value is still not a guarantee.
    calibrated = smoothed * 0.72 + technical_quality * 0.28
    calibrated = max(35.0, min(95.0, calibrated))
    return {
        "status": "CALIBRATED",
        "calibrated_confidence": round(calibrated, 2),
        "sample_size": n,
        "observed_win_rate": round(observed, 2),
        "smoothed_win_rate": round(smoothed, 2),
        "note": "Historical calibration is active; confidence remains a model estimate, not a guaranteed win rate.",
    }


# =========================================================
# YAHOO MARKET DATA
# =========================================================

def fetch_yahoo_1m(symbol):
    """Fetch Yahoo Finance 1-minute OHLCV candles.

    V53 uses a two-path Yahoo fetch because some FX crosses intermittently
    return an empty/short Ticker.history() response even while Yahoo itself
    is showing the pair.  We never synthesize candles: both paths are real
    Yahoo OHLC responses.  Short-but-usable series are accepted and then
    labelled by the feed-quality gate instead of being discarded outright.
    """
    yf = _get_yfinance()
    df = None
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(
            period="5d",
            interval="1m",
            auto_adjust=False,
            actions=False,
            timeout=YAHOO_REQUEST_TIMEOUT_SECONDS,
        )
    except Exception:
        df = None

    # Fallback Yahoo request path for crosses that occasionally fail through
    # Ticker.history().  This is still Yahoo data, not a fabricated series.
    if df is None or getattr(df, "empty", True):
        try:
            df = yf.download(
                tickers=symbol,
                period="5d",
                interval="1m",
                auto_adjust=False,
                actions=False,
                progress=False,
                threads=False,
                timeout=YAHOO_REQUEST_TIMEOUT_SECONDS,
            )
            # yfinance can return MultiIndex columns for download().
            if getattr(df, "columns", None) is not None and getattr(df.columns, "nlevels", 1) > 1:
                try:
                    df.columns = df.columns.get_level_values(0)
                except Exception:
                    pass
        except Exception:
            df = None

    if df is None or getattr(df, "empty", True):
        return None

    required = ["Open", "High", "Low", "Close"]
    if not all(col in df.columns for col in required):
        return None

    df = df.dropna(subset=required).sort_index()
    # V52 rejected every pair below 120 rows.  That made a temporarily-short
    # but perfectly chartable feed look like "no candles".  V53 accepts 35+
    # real rows; indicator/pattern warm-up still decides whether a signal is
    # allowed, and the quality gate warns on weak depth.
    if len(df) < 35:
        return None

    return df


def _get_symbol_fetch_lock(symbol):
    with symbol_fetch_locks_guard:
        lock = symbol_fetch_locks.get(symbol)
        if lock is None:
            lock = threading.Lock()
            symbol_fetch_locks[symbol] = lock
        return lock


def _get_batch_key_lock(key):
    with batch_key_locks_guard:
        lock = batch_key_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            batch_key_locks[key] = lock
        return lock


def _pace_yahoo_request():
    global last_yahoo_fetch_started
    with yahoo_pace_lock:
        now = time.time()
        wait_for = YAHOO_MIN_GAP_SECONDS - (now - last_yahoo_fetch_started)
        if wait_for > 0:
            time.sleep(wait_for)
        last_yahoo_fetch_started = time.time()


def _cached_symbol_is_usable(symbol, max_cache_age):
    now = time.time()
    with cache_lock:
        cached = market_cache.get(symbol)
        if cached and (now - float(cached.get("timestamp") or 0)) <= max_cache_age:
            return True
    return False


def _source_candle_age_seconds(df):
    """Return age of the newest Yahoo 1m candle, not age of our local cache fetch."""
    if df is None or getattr(df, "empty", True):
        return None
    try:
        latest = df.index[-1]
        latest_epoch = float(latest.timestamp())
        # Yahoo timestamps are the candle open time. Give the 1m candle its full minute
        # before counting it as stale.
        return max(0.0, time.time() - latest_epoch - 60.0)
    except Exception:
        return None


def update_symbol_cache(symbol, force=False):
    """Refresh one Yahoo symbol with bounded single-flight waits and failure cooldown."""
    now = time.time()

    with cache_lock:
        cached = market_cache.get(symbol)
        if cached and not force and (now - cached["timestamp"]) <= CACHE_DURATION:
            return True

    with failed_symbol_lock:
        blocked_until = failed_symbol_until.get(symbol, 0)

    if not force and blocked_until > now:
        return _cached_symbol_is_usable(symbol, STALE_CACHE_MAX_AGE)

    symbol_lock = _get_symbol_fetch_lock(symbol)
    acquired_symbol = symbol_lock.acquire(timeout=YAHOO_SYMBOL_LOCK_WAIT_SECONDS)
    if not acquired_symbol:
        # Another request is already fetching this exact symbol. Never let a manual
        # single-pair scan hang indefinitely behind it.
        return _cached_symbol_is_usable(symbol, STALE_CACHE_MAX_AGE)

    try:
        now = time.time()

        # Another request may have refreshed this symbol while this caller waited.
        with cache_lock:
            cached = market_cache.get(symbol)
            if cached and not force and (now - cached["timestamp"]) <= CACHE_DURATION:
                return True

        with failed_symbol_lock:
            blocked_until = failed_symbol_until.get(symbol, 0)

        if not force and blocked_until > now:
            return _cached_symbol_is_usable(symbol, STALE_CACHE_MAX_AGE)

        acquired_slot = yahoo_fetch_semaphore.acquire(timeout=YAHOO_SEMAPHORE_WAIT_SECONDS)
        if not acquired_slot:
            return _cached_symbol_is_usable(symbol, STALE_CACHE_MAX_AGE)

        try:
            _pace_yahoo_request()
            df = fetch_yahoo_1m(symbol)
        finally:
            yahoo_fetch_semaphore.release()

        if df is None:
            with failed_symbol_lock:
                failed_symbol_until[symbol] = time.time() + YAHOO_FAILURE_COOLDOWN
            return False

        with cache_lock:
            market_cache[symbol] = {
                "data": df.copy(),
                "timestamp": time.time(),
            }

        with failed_symbol_lock:
            failed_symbol_until.pop(symbol, None)

        return True

    except Exception as e:
        with failed_symbol_lock:
            failed_symbol_until[symbol] = time.time() + YAHOO_FAILURE_COOLDOWN
        print(f"Yahoo fetch error for {symbol}: {e}")
        return False
    finally:
        symbol_lock.release()



def _get_twelve_data_symbol_lock(provider_symbol):
    with twelve_data_symbol_locks_guard:
        lock = twelve_data_symbol_locks.get(provider_symbol)
        if lock is None:
            lock = threading.Lock()
            twelve_data_symbol_locks[provider_symbol] = lock
        return lock


def _pace_twelve_data_request():
    global last_twelve_data_fetch_started
    with twelve_data_pace_lock:
        now = time.time()
        wait_for = TWELVE_DATA_MIN_GAP_SECONDS - (now - last_twelve_data_fetch_started)
        if wait_for > 0:
            time.sleep(wait_for)
        last_twelve_data_fetch_started = time.time()


def _twelve_data_cache_get(provider_symbol, allow_stale=False):
    now = time.time()
    with twelve_data_cache_lock:
        cached = twelve_data_cache.get(provider_symbol)
        if not cached:
            return None
        cache_age = now - float(cached.get("timestamp") or 0)
        if allow_stale or cache_age <= TWELVE_DATA_CACHE_SECONDS:
            return cached.get("data").copy()
    return None


def _twelve_data_mark_failure(provider_symbol, code=None):
    global twelve_data_global_blocked_until
    now = time.time()
    with twelve_data_failed_lock:
        twelve_data_failed_until[provider_symbol] = now + TWELVE_DATA_FAILURE_COOLDOWN

    try:
        code = int(code or 0)
    except Exception:
        code = 0
    if code in {401, 403, 429}:
        with twelve_data_global_lock:
            twelve_data_global_blocked_until = max(
                twelve_data_global_blocked_until,
                now + TWELVE_DATA_GLOBAL_RATE_LIMIT_COOLDOWN,
            )


def _twelve_data_global_allowed():
    with twelve_data_global_lock:
        return time.time() >= float(twelve_data_global_blocked_until or 0.0)



def _raja_ensure_datetime_index(df):
    """Normalize market frames to sorted UTC minute timestamps."""
    if df is None or getattr(df, "empty", True):
        return df
    pd = _get_pandas()
    out = df.copy()
    try:
        idx = pd.to_datetime(out.index, utc=True, errors="coerce")
        out.index = idx
        out = out[~out.index.isna()]
        out = out.sort_index()
        out = out[~out.index.duplicated(keep="last")]
    except Exception:
        return out
    return out


def _raja_align_1m_boundaries(df):
    """Force Twelve Data 1m rows onto exact xx:00 minute buckets."""
    if df is None or getattr(df, "empty", True):
        return df
    pd = _get_pandas()
    out = _raja_ensure_datetime_index(df)
    if out is None or out.empty:
        return out
    try:
        out.index = out.index.floor("min")
        # If multiple records land in the same minute, rebuild a true OHLC bucket.
        agg = {"Open":"first", "High":"max", "Low":"min", "Close":"last"}
        if "Volume" in out.columns:
            agg["Volume"] = "sum"
        out = out.groupby(level=0).agg(agg).sort_index()
    except Exception:
        pass
    return out

def _parse_twelve_data_frame(payload):
    values = payload.get("values") if isinstance(payload, dict) else None
    if not isinstance(values, list) or not values:
        return None

    pd = _get_pandas()
    df = pd.DataFrame(values)
    required = {"datetime", "open", "high", "low", "close"}
    if not required.issubset(df.columns):
        return None

    df["Datetime"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
    for src, dst in (("open", "Open"), ("high", "High"), ("low", "Low"), ("close", "Close")):
        df[dst] = pd.to_numeric(df[src], errors="coerce")

    if "volume" in df.columns:
        df["Volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
    else:
        df["Volume"] = 0.0

    df = (
        df.dropna(subset=["Datetime", "Open", "High", "Low", "Close"])
          .set_index("Datetime")
          .sort_index()
    )
    df = df[~df.index.duplicated(keep="last")]
    df = _raja_align_1m_boundaries(df)
    if df is None or len(df) < 20:
        return None
    return df[["Open", "High", "Low", "Close", "Volume"]]


def fetch_twelve_data_1m(pair):
    """Fetch Twelve Data 1-minute OHLCV backup candles for one RAJA pair."""
    if not TWELVE_DATA_ENABLED:
        return None, None

    provider_symbol = TWELVE_DATA_SYMBOLS.get(pair)
    if not provider_symbol:
        return None, None

    params = {
        "symbol": provider_symbol,
        "interval": "1min",
        "outputsize": TWELVE_DATA_OUTPUTSIZE,
        "timezone": "UTC",
        "apikey": TWELVE_DATA_API_KEY,
    }
    url = TWELVE_DATA_BASE_URL + "?" + urlencode(params)
    req = UrlRequest(url, headers={"User-Agent": "RAJA-AI/1.0", "Accept": "application/json"})

    try:
        with urlopen(req, timeout=TWELVE_DATA_REQUEST_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read().decode("utf-8", errors="replace"))
    except HTTPError as exc:
        _twelve_data_mark_failure(provider_symbol, getattr(exc, "code", None))
        print(f"Twelve Data HTTP error for {provider_symbol}: {getattr(exc, 'code', 'unknown')}")
        return None, provider_symbol
    except URLError as exc:
        _twelve_data_mark_failure(provider_symbol)
        print(f"Twelve Data network error for {provider_symbol}: {exc.reason}")
        return None, provider_symbol
    except Exception as exc:
        _twelve_data_mark_failure(provider_symbol)
        print(f"Twelve Data fetch error for {provider_symbol}: {exc}")
        return None, provider_symbol

    if not isinstance(payload, dict) or str(payload.get("status") or "").lower() == "error":
        code = payload.get("code") if isinstance(payload, dict) else None
        _twelve_data_mark_failure(provider_symbol, code)
        message = str(payload.get("message") or "API returned an error") if isinstance(payload, dict) else "Invalid response"
        print(f"Twelve Data API error for {provider_symbol}: {message}")
        return None, provider_symbol

    df = _parse_twelve_data_frame(payload)
    if df is None or df.empty:
        _twelve_data_mark_failure(provider_symbol)
        return None, provider_symbol

    return df, provider_symbol


def get_twelve_data_market_data(pair, force=False):
    """Return cached/fresh Twelve Data candles without exposing the API key."""
    if not TWELVE_DATA_ENABLED:
        return None, None

    provider_symbol = TWELVE_DATA_SYMBOLS.get(pair)
    if not provider_symbol:
        return None, None

    cached = _twelve_data_cache_get(provider_symbol, allow_stale=False)
    if cached is not None and not force:
        return cached, provider_symbol

    if not _twelve_data_global_allowed():
        return _twelve_data_cache_get(provider_symbol, allow_stale=True), provider_symbol

    now = time.time()
    with twelve_data_failed_lock:
        blocked_until = float(twelve_data_failed_until.get(provider_symbol, 0.0) or 0.0)
    if blocked_until > now:
        return _twelve_data_cache_get(provider_symbol, allow_stale=True), provider_symbol

    symbol_lock = _get_twelve_data_symbol_lock(provider_symbol)
    acquired = symbol_lock.acquire(timeout=min(12.0, TWELVE_DATA_REQUEST_TIMEOUT_SECONDS + 2.0))
    if not acquired:
        return _twelve_data_cache_get(provider_symbol, allow_stale=True), provider_symbol

    try:
        cached = _twelve_data_cache_get(provider_symbol, allow_stale=False)
        if cached is not None and not force:
            return cached, provider_symbol

        acquired_slot = twelve_data_fetch_semaphore.acquire(timeout=TWELVE_DATA_REQUEST_TIMEOUT_SECONDS + 2.0)
        if not acquired_slot:
            return _twelve_data_cache_get(provider_symbol, allow_stale=True), provider_symbol

        try:
            _pace_twelve_data_request()
            df, provider_symbol = fetch_twelve_data_1m(pair)
        finally:
            twelve_data_fetch_semaphore.release()

        if df is None or df.empty:
            return _twelve_data_cache_get(provider_symbol, allow_stale=True), provider_symbol

        with twelve_data_cache_lock:
            twelve_data_cache[provider_symbol] = {"data": df.copy(), "timestamp": time.time()}
        with twelve_data_failed_lock:
            twelve_data_failed_until.pop(provider_symbol, None)

        return df.copy(), provider_symbol
    finally:
        symbol_lock.release()



def _market_candle_quality(df, lookback=80):
    """Conservative OHLC feed-quality check for chart/signal use.

    This does not invent or reshape candles. It only detects sparse/flat 1m feeds so
    the source router can prefer a configured backup provider when one exists.
    """
    out = {"score": 0.0, "grade": "BAD", "flat_ratio": 1.0, "tiny_ratio": 1.0, "rows": 0}
    if df is None or getattr(df, "empty", True):
        return out
    try:
        part = df.tail(max(20, min(int(lookback), len(df)))).copy()
        rows = []
        for _, r in part.iterrows():
            o = float(r.get("Open")); h = float(r.get("High")); l = float(r.get("Low")); c = float(r.get("Close"))
            if not all(map(lambda x: x == x and abs(x) < 1e12, (o,h,l,c))):
                continue
            if h < max(o,c) or l > min(o,c) or h < l:
                continue
            rng = max(0.0, h-l)
            body = abs(c-o)
            rows.append((o,h,l,c,rng,body))
        n=len(rows)
        if n < 20:
            out.update({"rows": n})
            return out
        ranges=[x[4] for x in rows]
        closes=[abs(x[3]) for x in rows if abs(x[3])>1e-12]
        med_close=sorted(closes)[len(closes)//2] if closes else 1.0
        eps=max(med_close*1e-7,1e-10)
        flat=sum(1 for x in ranges if x <= eps)/n
        positive=sorted([x for x in ranges if x>eps])
        med_range=positive[len(positive)//2] if positive else 0.0
        tiny_cut=max(eps, med_range*0.12)
        tiny=sum(1 for x in ranges if x <= tiny_cut)/n
        # Large share of zero/tiny OHLC bars is characteristic of sparse quote sampling.
        score=100.0 - flat*70.0 - max(0.0,tiny-0.35)*55.0
        score=max(0.0,min(100.0,score))
        grade='GOOD' if score>=75 else ('CAUTION' if score>=55 else 'POOR')
        out.update({"score":round(score,1),"grade":grade,"flat_ratio":round(flat,3),"tiny_ratio":round(tiny,3),"rows":n,"median_range":med_range})
        return out
    except Exception:
        return out

def _market_source_info(pair, yahoo_symbol, source="Yahoo Finance", provider_symbol=None):
    source = str(source or "Yahoo Finance")
    is_backup = source == "Twelve Data"
    if is_backup:
        mode = "underlying_proxy_backup" if "(OTC)" in pair else "live_backup_reference"
    else:
        mode = "underlying_proxy" if "(OTC)" in pair else "live_reference"
    return {
        "source": source,
        "source_mode": mode,
        "provider_symbol": provider_symbol or yahoo_symbol,
        "yahoo_symbol": yahoo_symbol,
        "backup_used": bool(is_backup),
    }


def get_market_data(pair, bridge_user=None, broker=None):
    """
    Source policy:
      * Broker OTC: native broker feed if available, otherwise clearly-labelled
        Yahoo/Twelve Data underlying/reference proxy (NO browser bridge required).
      * Non-OTC/live markets: Yahoo -> Twelve Data reference chain.

    Important: reference OTC data is NOT the broker's exact synthetic OTC candle.
    """
    yahoo_symbol = YAHOO_SYMBOLS.get(pair)

    broker_name = str(broker or "").strip().casefold().replace(" ", "")
    is_otc = "(otc)" in str(pair).casefold()
    is_quotex = broker_name == "quotex"
    is_pocket = broker_name in {"pocketoption", "pocket_option", "pocket"}
    otc_reference_reason = None

    # Prefer exact native broker OTC if the server connector happens to be available.
    # Browser bridge is intentionally not required in this build.
    if is_otc and (is_quotex or is_pocket):
        native_info = {}
        native_symbol = None
        if callable(get_native_broker_market_data):
            try:
                native_df, native_age, native_symbol, native_info = get_native_broker_market_data(broker, pair)
            except Exception as exc:
                native_df, native_age, native_symbol = None, None, None
                native_info = {
                    "source": "Broker Native WebSocket",
                    "source_mode": "broker_native_websocket",
                    "exact_broker_feed": False,
                    "unavailable_reason": f"Native broker feed error: {type(exc).__name__}: {exc}",
                }
            if native_df is not None and not native_df.empty:
                return native_df, native_age, native_symbol or pair, native_info

        otc_reference_reason = (
            (native_info or {}).get("unavailable_reason")
            or "Exact broker OTC feed is unavailable; using underlying reference data."
        )

        if RAJA_STRICT_BROKER_OTC or not yahoo_symbol:
            blocked_info = dict(native_info or {})
            blocked_info.update({
                "source": blocked_info.get("source") or ("Quotex Native WebSocket" if is_quotex else "Pocket Option Native WebSocket"),
                "source_mode": "broker_native_required",
                "exact_broker_feed": False,
                "reference_fallback_blocked": True,
                "unavailable_reason": otc_reference_reason,
            })
            return None, None, native_symbol or yahoo_symbol or pair, blocked_info

    if not yahoo_symbol:
        return None, None, None, _market_source_info(pair, None)

    def _with_reference_notice(info):
        info = dict(info or {})
        if otc_reference_reason:
            info.update({
                "exact_broker_feed": False,
                "bridge_required": False,
                "reference_fallback_used": True,
                "source_mode": (
                    "underlying_proxy_backup"
                    if str(info.get("source") or "") == "Twelve Data"
                    else "underlying_proxy"
                ),
                "reference_notice": "OTC reference mode: underlying market data, not exact broker synthetic OTC candles.",
                "exact_feed_unavailable_reason": otc_reference_reason,
            })
        return info

    yahoo_data = None
    yahoo_age = None
    now = time.time()
    with cache_lock:
        cached = market_cache.get(yahoo_symbol)

    if cached:
        cache_age = now - float(cached.get("timestamp") or 0.0)
        if cache_age <= CACHE_DURATION:
            yahoo_data = cached["data"].copy()
            yahoo_age = _source_candle_age_seconds(yahoo_data)

    if yahoo_data is None:
        refreshed = update_symbol_cache(yahoo_symbol)
        with cache_lock:
            cached = market_cache.get(yahoo_symbol)
        if cached:
            cache_age = time.time() - float(cached.get("timestamp") or 0.0)
            if refreshed or cache_age <= STALE_CACHE_MAX_AGE:
                yahoo_data = cached["data"].copy()
                yahoo_age = _source_candle_age_seconds(yahoo_data)

    yahoo_quality = _market_candle_quality(yahoo_data) if yahoo_data is not None and not yahoo_data.empty else {"score":0.0,"grade":"BAD"}

    # If Yahoo is fresh but its 1m OHLC is visibly sparse/flat, prefer Twelve Data
    # when the user has configured it. Never synthesize fake candles to make a chart look better.
    yahoo_fresh = bool(yahoo_data is not None and not yahoo_data.empty and (yahoo_age is None or yahoo_age <= MAX_SOURCE_CANDLE_AGE_SECONDS))
    yahoo_quality_poor = bool(float(yahoo_quality.get("score") or 0.0) < 55.0)

    td_data = None
    td_age = None
    td_symbol = TWELVE_DATA_SYMBOLS.get(pair)
    if TWELVE_DATA_ENABLED and td_symbol:
        td_data, td_symbol = get_twelve_data_market_data(pair)
        if td_data is not None and not td_data.empty:
            td_age = _source_candle_age_seconds(td_data)
            td_quality = _market_candle_quality(td_data)
            if td_age is None or td_age <= MAX_SOURCE_CANDLE_AGE_SECONDS:
                # Prefer the backup when Yahoo is structurally sparse, or when the
                # backup quality is materially better. This keeps chart and signal on
                # one real OHLC source instead of cosmetically altering Yahoo candles.
                if yahoo_quality_poor or float(td_quality.get("score") or 0.0) >= float(yahoo_quality.get("score") or 0.0) + 12.0:
                    info=_with_reference_notice(_market_source_info(pair, yahoo_symbol, "Twelve Data", td_symbol))
                    info["feed_quality"]=td_quality
                    info["source_switch_reason"]="Yahoo 1m OHLC was sparse/flat; using higher-quality configured backup feed."
                    return td_data, td_age, yahoo_symbol, info

    if yahoo_fresh:
        info=_with_reference_notice(_market_source_info(pair, yahoo_symbol, "Yahoo Finance", yahoo_symbol))
        info["feed_quality"]=yahoo_quality
        if yahoo_quality_poor:
            info["quality_warning"]="Yahoo 1m OHLC is sparse/flat for this pair right now; exact broker candles may look different."
        return yahoo_data, yahoo_age, yahoo_symbol, info

    # If Yahoo is stale/unavailable and a fresh backup exists, use it.
    if td_data is not None and not td_data.empty and (td_age is None or td_age <= MAX_SOURCE_CANDLE_AGE_SECONDS):
        info=_with_reference_notice(_market_source_info(pair, yahoo_symbol, "Twelve Data", td_symbol))
        info["feed_quality"]=_market_candle_quality(td_data)
        return td_data, td_age, yahoo_symbol, info

    candidates = []
    if yahoo_data is not None and not yahoo_data.empty:
        candidates.append((
            float(yahoo_age) if yahoo_age is not None else float("inf"),
            yahoo_data, yahoo_age,
            _with_reference_notice(_market_source_info(pair, yahoo_symbol, "Yahoo Finance", yahoo_symbol)),
        ))
    if td_data is not None and not td_data.empty:
        candidates.append((
            float(td_age) if td_age is not None else float("inf"),
            td_data, td_age,
            _with_reference_notice(_market_source_info(pair, yahoo_symbol, "Twelve Data", td_symbol)),
        ))

    if candidates:
        _, data, age, source_info = min(candidates, key=lambda item: item[0])
        return data.copy(), age, yahoo_symbol, source_info

    missing_info = _with_reference_notice(_market_source_info(pair, yahoo_symbol))
    if otc_reference_reason and not missing_info.get("unavailable_reason"):
        missing_info["unavailable_reason"] = (
            "No fresh Yahoo/Twelve reference data is available for this OTC proxy right now."
        )
    return None, None, yahoo_symbol, missing_info



# =========================================================
# V57 SOURCE POLICY OVERRIDE
# Normal Forex = token-free Yahoo Finance 1-minute OHLC.
# No Quotex/Pocket session, OANDA token, or TradingView token is required.
# Chart and signal engine use the same OHLC dataframe.
# OTC remains broker-specific and is not represented as normal Forex data.
# =========================================================
def get_market_data(pair, bridge_user=None, broker=None):
    pair = str(pair or "").strip()
    broker_name = str(broker or "").strip().casefold().replace(" ", "")
    is_otc = "(otc)" in pair.casefold()
    is_quotex = broker_name == "quotex"
    is_pocket = broker_name in {"pocketoption", "pocket_option", "pocket"}

    if is_otc:
        # Preserve the project's existing OTC native/live-sharing path.
        if is_quotex or is_pocket:
            if callable(get_native_broker_market_data):
                try:
                    ndf, nage, nsym, ninfo = get_native_broker_market_data(broker, pair)
                    if ndf is not None and not ndf.empty:
                        info = dict(ninfo or {})
                        info["feed_quality"] = _market_candle_quality(ndf)
                        return ndf, nage, nsym or pair, info
                except Exception:
                    pass
            try:
                if is_pocket:
                    bdf, bage, bsym, binfo = get_pocket_bridge_market_data(bridge_user, pair)
                else:
                    bdf, bage, bsym, binfo = get_quotex_bridge_market_data(bridge_user, pair)
                if bdf is not None and not bdf.empty:
                    info = dict(binfo or {})
                    info["feed_quality"] = _market_candle_quality(bdf)
                    return bdf, bage, bsym or pair, info
            except Exception:
                pass
        return None, None, pair, {
            "source": "OTC Live Sharing",
            "source_mode": "otc_requires_broker_feed",
            "unavailable_reason": "OTC requires broker-native/live-sharing data.",
        }

    if pair not in YAHOO_SYMBOLS:
        return None, None, pair, {
            "source": "Yahoo Finance",
            "source_mode": "unsupported_symbol",
            "unavailable_reason": f"{pair} is not configured in the token-free FX feed.",
        }

    df, age, symbol, fetch_info = _fetch_yahoo_1m(pair, count=1600)
    if df is None or df.empty:
        info = dict(fetch_info or {})
        info.update({
            "source": "Yahoo Finance",
            "source_mode": info.get("source_mode") or "yahoo_unavailable",
            "provider_symbol": symbol or YAHOO_SYMBOLS.get(pair),
            "unavailable_reason": info.get("unavailable_reason") or
                "Yahoo Finance returned no usable 1-minute candles for this pair.",
        })
        return None, age, symbol or YAHOO_SYMBOLS.get(pair), info

    info = dict(fetch_info or {})
    info.update({
        "source": "Yahoo Finance",
        "source_mode": "token_free_yahoo_1m",
        "provider_symbol": symbol or YAHOO_SYMBOLS.get(pair),
        "feed_quality": _market_candle_quality(df),
        "backup_used": False,
        "credentials_required": False,
    })
    return df, age, symbol or YAHOO_SYMBOLS.get(pair), info

def background_market_poller():
    """Disabled intentionally: full-market polling caused Yahoo rate limits."""
    return


def _raja_clean_ohlc_frame(df):
    """Normalize, validate and de-duplicate OHLC rows without inventing candles.

    V80 keeps provider prices untouched. It only removes unusable rows and makes
    timestamps deterministic so chart, resampling and indicators consume the same
    closed-candle dataset.
    """
    if df is None or getattr(df, "empty", True):
        return df
    pd = _get_pandas()
    out = _raja_ensure_datetime_index(df)
    if out is None or getattr(out, "empty", True):
        return out
    out = out.copy()
    required = ("Open", "High", "Low", "Close")
    if not all(col in out.columns for col in required):
        return out.iloc[0:0].copy()
    for col in required:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    if "Volume" in out.columns:
        out["Volume"] = pd.to_numeric(out["Volume"], errors="coerce").fillna(0.0)
    out = out.dropna(subset=list(required))
    if out.empty:
        return out
    finite = np.isfinite(out[list(required)].to_numpy(dtype=float)).all(axis=1)
    out = out.loc[finite]
    valid = (
        (out["High"] >= out[["Open", "Close"]].max(axis=1))
        & (out["Low"] <= out[["Open", "Close"]].min(axis=1))
        & (out["High"] >= out["Low"])
        & (out["Open"] > 0) & (out["High"] > 0) & (out["Low"] > 0) & (out["Close"] > 0)
    )
    out = out.loc[valid].sort_index()
    out = out[~out.index.duplicated(keep="last")]
    return out


def _raja_1m_gap_count(df, lookback=300):
    """Count missing UTC minute slots in recent provider candles; never fills them."""
    clean = _raja_clean_ohlc_frame(df)
    if clean is None or getattr(clean, "empty", True) or len(clean) < 2:
        return 0
    idx = clean.tail(max(2, int(lookback))).index
    gaps = 0
    for a, b in zip(idx[:-1], idx[1:]):
        try:
            delta = int(round((b.timestamp() - a.timestamp()) / 60.0))
            if delta > 1:
                gaps += delta - 1
        except Exception:
            pass
    return int(gaps)


def _raja_resample_timeframe(base_df, minutes):
    """Resample from exact UTC minute starts without dropping any bucket."""
    if base_df is None or getattr(base_df, "empty", True):
        return None
    minutes = max(1, int(minutes))
    df = _raja_align_1m_boundaries(_raja_clean_ohlc_frame(base_df))
    if df is None or df.empty:
        return None
    if minutes == 1:
        return df.copy()

    rule = f"{minutes}min"
    agg = {"Open":"first","High":"max","Low":"min","Close":"last"}
    if "Volume" in df.columns:
        agg["Volume"] = "sum"
    try:
        tf = df.resample(
            rule,
            label="left",
            closed="left",
            origin="start_day",
            offset="0min",
        ).agg(agg)
    except TypeError:
        tf = df.resample(rule, label="left", closed="left").agg(agg)
    return tf.dropna(subset=["Open","High","Low","Close"])


def _raja_bucket_is_forming(bucket_start, minutes, now_epoch=None):
    """True only when the bucket's exact end has not arrived yet."""
    if bucket_start is None:
        return False
    try:
        now_epoch = float(time.time() if now_epoch is None else now_epoch)
        start_epoch = float(bucket_start.timestamp())
        end_epoch = start_epoch + max(1, int(minutes)) * 60
        return now_epoch < end_epoch
    except Exception:
        return False


def build_timeframe(base_df, minutes):
    """CLOSED candles only — used by the RAJA signal engine."""
    tf = _raja_resample_timeframe(base_df, minutes)
    if tf is None or tf.empty:
        return tf
    if _raja_bucket_is_forming(tf.index[-1], minutes):
        return tf.iloc[:-1].copy()
    return tf


def build_timeframe_display(base_df, minutes):
    """Chart candles including a provider forming candle when supplied."""
    tf = _raja_resample_timeframe(base_df, minutes)
    if tf is None or tf.empty:
        return tf
    return tf.copy()




# =========================================================
# SK TRADING CLUB PATTERN TYPE 1-25 — STRATEGY ONLY ENGINE
# V46 keeps SK25 as the setup generator, then validates exact setups with
# EMA / RSI / MACD / ATR-volatility and higher-timeframe confirmation.
# Indicators never create a trade by themselves; they can only confirm or block an SK25 setup.
# =========================================================

SK25_ENGINE_VERSION = "RAJA_AI_V80_CANDLE_SYNC_WARMUP_FIX"
SK25_PATTERN_LIBRARY_SIZE = 25
SK25_LIVE_MIN_CANDLES = 10

# V42 scanner policy: Pattern Type 1-25 only. IDs 26-35 remain in the
# historical source for compatibility, but add()/add_setup() ignore them.
RAJA_ACTIVE_STRATEGY_IDS = frozenset(range(1, 26))
RAJA_STRATEGY_NAMES = {
    1: "RAJA Type 1 · OTC Eight-Candle Continuation",
    2: "RAJA Type 2 · Resistance Reversal",
    3: "RAJA Type 3 · Sideways Wick Sweep",
    4: "RAJA Type 4 · Long-Wick Rejection",
    5: "RAJA Type 5 · Double-Tail Reversal",
    6: "RAJA Type 6 · Loss Recovery Sequence",
    7: "RAJA Type 7 · RED + Two GREEN Reversal",
    8: "RAJA Type 8 · GREEN + Two RED Reversal",
    9: "RAJA Type 9 · Bull Continuation",
    10: "RAJA Type 10 · Bear Continuation",
    11: "RAJA Type 11 · True 30s Contained Reversal",
    12: "RAJA Type 12 · 2m Resistance Hold",
    13: "RAJA Type 13 · 2m Retest Breakout Reversal",
    14: "RAJA Type 14 · Horizontal Break",
    15: "RAJA Type 15 · V-Shape Breakout Reversal",
    16: "RAJA Type 16 · Bull Run Continuation",
    17: "RAJA Type 17 · Bear Run Continuation",
    18: "RAJA Type 18 · Sideways Resistance Hold",
    19: "RAJA Type 19 · Sideways Support Hold",
    20: "RAJA Type 20 · Downtrend Hold",
    21: "RAJA Type 21 · Uptrend Hold",
    22: "RAJA Type 22 · Uptrend Contained Pullback",
    23: "RAJA Type 23 · Downtrend Contained Pullback",
    24: "RAJA Type 24 · Live S/R Sequence",
    25: "RAJA Type 25 · S/R Breakout",
    26: "PDF Setup 1 · Trend S/R Breakout",
    27: "PDF Setup 4 · Breakout Engulf Retest",
    28: "PDF Setup 6 · Four-Candle S/R Hold",
    29: "PDF Setup 11 · 50% Wick Sweep",
    30: "PDF Setup 13 · Engulf + Open-Level Hold",
    31: "Premium · S/R Breakout Retest Confirmation",
    32: "Premium · Liquidity Sweep Reversal",
    33: "Premium · Trend Pullback Continuation",
    34: "Premium · Failed Breakout Reversal",
    35: "Premium · Engulfing at Key S/R",
}
RAJA_STRATEGY_PRIORITIES = {
    2:105, 4:115, 9:145, 10:145, 12:165, 14:150,
    18:155, 19:155, 20:145, 21:145, 22:155, 23:155, 24:180, 25:175,
    26:188, 27:202, 28:205, 29:192, 30:198,
    31:210, 32:206, 33:204, 34:207, 35:200,
}


def _f(value, default=0.0):
    try:
        value = float(value)
        return default if value != value else value
    except Exception:
        return default


def _candle_rows(df, limit=80):
    if df is None or getattr(df, "empty", True):
        return []
    rows = []
    for idx, row in df.tail(max(12, min(int(limit), 120))).iterrows():
        try:
            o = float(row["Open"]); h = float(row["High"]); l = float(row["Low"]); c = float(row["Close"])
            if not all(x == x for x in (o,h,l,c)) or h < l:
                continue
            rng = max(h-l, 1e-12)
            body = abs(c-o)
            direction = 1 if c > o else (-1 if c < o else 0)
            rows.append({
                "epoch": int(idx.timestamp()) if hasattr(idx, "timestamp") else 0,
                "open": o, "high": h, "low": l, "close": c,
                "dir": direction, "range": rng, "body": body,
                "body_ratio": body/rng,
                "upper_wick": max(0.0, h-max(o,c)),
                "lower_wick": max(0.0, min(o,c)-l),
                "body_top": max(o,c), "body_bottom": min(o,c),
            })
        except Exception:
            continue
    return rows


def _median(values, default=0.0):
    vals = sorted(float(v) for v in values if v is not None)
    if not vals:
        return float(default)
    m = len(vals)//2
    return vals[m] if len(vals)%2 else (vals[m-1]+vals[m])/2.0


def _sk25_trend(candles, lookback=8):
    seq = candles[-max(3, min(int(lookback), len(candles))):]
    if len(seq) < 3:
        return 0.0
    med_range = max(_median([x["range"] for x in seq], 1e-8), 1e-8)
    move = seq[-1]["close"] - seq[0]["close"]
    scale = med_range * max(2.0, len(seq)-1)
    return max(-1.0, min(1.0, move/scale))


def _sk25_level_clusters(values, tolerance, min_touches=2):
    values = sorted(float(v) for v in values)
    groups = []
    for value in values:
        placed = False
        for group in groups:
            center = sum(group)/len(group)
            if abs(value-center) <= tolerance:
                group.append(value); placed = True; break
        if not placed:
            groups.append([value])
    return [sum(g)/len(g) for g in groups if len(g) >= min_touches]


def analyze_sk25_ohlc(df, timeframe="1m", market="LIVE", last_outcome=""):
    """Evaluate Pattern Type 1-25 using closed-candle structure only."""
    tf = str(timeframe or "1m").strip().lower()
    market_name = str(market or "LIVE").upper()
    previous_outcome = str(last_outcome or "").strip().upper()
    if previous_outcome.startswith("RECOVERY_"):
        previous_outcome = previous_outcome.split("_", 1)[1]

    candles = _candle_rows(df, 90)
    count = len(candles)
    if count < SK25_LIVE_MIN_CANDLES:
        return {
            "signal":"NO SIGNAL", "score":0.0, "pattern_type":0,
            "selected_pattern":"NO ACTIVE STRATEGY SETUP", "pattern_direction":"NONE",
            "next_candle_color":"NONE", "setup_match":0.0,
            "rules":[], "pattern_signals":[], "conflict_gate":False,
            "reason":f"Only {count} closed candles are available; need at least {SK25_LIVE_MIN_CANDLES} for strict strategy scanning.",
            "closed_candle_epoch": candles[-1]["epoch"] if candles else None,
        }

    recent = candles[-min(count,24):]
    med_range = max(_median([x["range"] for x in recent], 1e-8), 1e-8)
    med_body = max(_median([x["body"] for x in recent], med_range*0.45), med_range*0.04)
    tol = max(med_range*0.20, abs(candles[-1]["close"])*1e-7)
    trend = _sk25_trend(candles, 9)
    is_otc = "OTC" in market_name
    is_live = not is_otc

    def seq_is(seq, dirs):
        return len(seq)==len(dirs) and all(int(x["dir"])==d for x,d in zip(seq, dirs))
    def normal(x):
        return x["body_ratio"] >= 0.28 and x["body"] >= med_body*0.45 and x["body"] <= med_body*2.20
    def small(x):
        return x["body_ratio"] <= 0.30 or x["body"] <= med_body*0.52
    def long_body(x):
        return x["body_ratio"] >= 0.66 and x["body"] >= med_body*1.28
    def marubozu(x):
        return long_body(x) and (x["upper_wick"]+x["lower_wick"]) <= max(tol*0.6, x["body"]*0.42)
    def long_lower(x):
        return x["lower_wick"] >= max(tol*0.8, x["body"]*0.68, med_range*0.20)
    def long_upper(x):
        return x["upper_wick"] >= max(tol*0.8, x["body"]*0.68, med_range*0.20)
    def body_inside(inner, outer, extra=0.0):
        return inner["body_top"] <= outer["body_top"]+extra and inner["body_bottom"] >= outer["body_bottom"]-extra

    exact=[]; near=[]
    def add(tno, direction, rules, setup, why, family="Candle Sequence", recovery=False, tf_rule="ANY"):
        if int(tno) not in RAJA_ACTIVE_STRATEGY_IDS:
            return
        matched=sum(1 for _,ok in rules if bool(ok)); total=max(1,len(rules)); pct=round(100.0*matched/total,1)
        item={
            "name":RAJA_STRATEGY_NAMES.get(int(tno), f"RAJA Strategy {tno}"), "pattern_type":tno, "direction":"UP" if direction>0 else "DOWN",
            "signal":"CALL" if direction>0 else "PUT", "next_candle":"GREEN" if direction>0 else "RED",
            "score":pct, "priority":RAJA_STRATEGY_PRIORITIES.get(int(tno),100), "setup":setup, "why":why, "family":family,
            "rules_matched":matched, "rules_total":total,
            "rules":[{"name":name,"ok":bool(ok)} for name,ok in rules],
            "recovery_trade":bool(recovery), "timeframe_rule":tf_rule,
        }
        if matched==total: exact.append(item)
        elif pct>=60: near.append(item)

    # Type 1 — OTC continuation, hardened against late-run exhaustion.
    last8=candles[-8:]
    last8_tail=last8[-1]
    green_exhaustion_ok=(not long_upper(last8_tail)) and normal(last8_tail) and last8_tail["body"] <= med_body*1.80
    red_exhaustion_ok=(not long_lower(last8_tail)) and normal(last8_tail) and last8_tail["body"] <= med_body*1.80
    add(1,1,[("OTC market",is_otc),("8 back-to-back GREEN candles",all(x["dir"]>0 for x in last8)),("8th GREEN is normal, not an exhaustion spike",green_exhaustion_ok)],"8 GREEN candles in OTC + exhaustion guard","Next candle GREEN only when the 8th candle is still structurally healthy.",tf_rule="OTC ONLY")
    add(1,-1,[("OTC market",is_otc),("8 back-to-back RED candles",all(x["dir"]<0 for x in last8)),("8th RED is normal, not an exhaustion spike",red_exhaustion_ok)],"8 RED candles in OTC + exhaustion guard","Next candle RED only when the 8th candle is still structurally healthy.",tf_rule="OTC ONLY")

    # Type 2 — two green at respected resistance + confirmed red rejection -> next red.
    a,b,c=candles[-3:]
    resistance_touch=abs(a["high"]-b["high"]) <= tol*1.55
    rejection=c["dir"]<0 and c["close"] < (b["open"]+b["close"])/2.0 and (long_upper(c) or c["body"]>=med_body*0.55)
    add(2,-1,[("GREEN, GREEN, RED setup",seq_is([a,b,c],[1,1,-1])),("Recent highs respect one resistance area",resistance_touch),("RED closes through prior GREEN midpoint / rejects high",rejection)],"2 GREEN + confirmed RED rejection at resistance","Resistance reversal targets next RED only after a meaningful rejection close.","Resistance")

    # Type 3 — sideways wick sweep must reclaim the swept lows before fading.
    wick_level=min(a["low"],b["low"])
    wick_break=c["dir"]>0 and c["low"] < wick_level-tol*0.15 and long_lower(c)
    wick_reclaim=c["close"] > wick_level+tol*0.05
    add(3,-1,[("GREEN, RED, GREEN setup",seq_is([a,b,c],[1,-1,1])),("3rd GREEN lower wick sweeps prior lows",wick_break),("3rd GREEN closes back above swept lows",wick_reclaim),("Sideways/mixed context",abs(trend)<0.60)],"GREEN-RED-GREEN downside sweep + reclaim","Next candle RED only after a completed sweep-and-reclaim.","Sideways")

    # Type 4 — long-tail rejection must occur near recent support, not mid-range.
    a,b=candles[-2:]
    prior_support=min((x["low"] for x in candles[-10:-2]), default=a["low"])
    support_rejection=a["low"] <= prior_support+tol*1.20 and a["close"] >= prior_support-tol*0.20
    add(4,1,[("RED then GREEN",seq_is([a,b],[-1,1])),("1st RED lower tail is long",long_lower(a)),("RED tail longer than GREEN head",a["lower_wick"]>max(b["upper_wick"]*1.12,med_range*0.18)),("Long tail rejects recent support area",support_rejection)],"RED long-tail support rejection + GREEN","Next candle GREEN only when the wick rejects a meaningful recent support area.")

    # Type 5 — R,R long tails; second red does not break first red high; then G -> next R.
    a,b,c=candles[-3:]
    add(5,-1,[("RED, RED, GREEN setup",seq_is([a,b,c],[-1,-1,1])),("First two RED lower tails are long",long_lower(a) and long_lower(b)),("2nd RED high does not break 1st RED",b["high"]<=a["high"]+tol*0.35),("Sideways/mixed context",abs(trend)<0.68)],"2 long-tail RED + GREEN","Next candle RED.","Sideways")

    # Type 6 — loss-recovery label stays, but a loss alone never creates the edge.
    recovery_bear_context=trend < -0.08
    add(6,-1,[("Previous trade marked LOSS",previous_outcome=="LOSS"),("RED, GREEN, RED setup",seq_is([a,b,c],[-1,1,-1])),("Bearish context still present",recovery_bear_context),("Final RED body is normal",normal(c))],"Recovery RED-GREEN-RED + bearish context","Recovery candidate targets next RED only when the market structure still supports bearish continuation; no stake increase is implied.","Recovery",True,"AFTER LOSS ONLY")

    # Types 7/8 — require a meaningful S/R extreme in addition to the colour sequence.
    prior7=candles[-10:-3] if count>=10 else candles[:-3]
    prior_res=max((x["high"] for x in prior7), default=max(b["high"],c["high"]))
    prior_sup=min((x["low"] for x in prior7), default=min(b["low"],c["low"]))
    at_res=max(b["high"],c["high"]) >= prior_res-tol*1.10
    at_sup=min(b["low"],c["low"]) <= prior_sup+tol*1.10
    add(7,-1,[("RED, GREEN, GREEN setup",seq_is([a,b,c],[-1,1,1])),("Two GREEN candles have normal bodies",normal(b) and normal(c)),("GREEN push reaches recent resistance / upper extreme",at_res)],"RED + 2 normal GREEN at resistance","Next candle RED only when the two-green push reaches a meaningful upper extreme.")
    add(8,1,[("GREEN, RED, RED setup",seq_is([a,b,c],[1,-1,-1])),("Two RED candles have normal bodies",normal(b) and normal(c)),("RED push reaches recent support / lower extreme",at_sup)],"GREEN + 2 normal RED at support","Next candle GREEN only when the two-red push reaches a meaningful lower extreme.")

    # Types 9/10 — continuation now requires trend alignment as well as the rejection wick.
    a,b,c,d=candles[-4:]
    add(9,1,[("3 GREEN + 1 RED setup",seq_is([a,b,c,d],[1,1,1,-1])),("Opposite RED has long lower tail",long_lower(d)),("Broader candle trend is bullish",trend>0.08)],"3 GREEN + long-tail RED in bullish context","Next candle GREEN only with trend alignment.")
    add(10,-1,[("3 RED + 1 GREEN setup",seq_is([a,b,c,d],[-1,-1,-1,1])),("Opposite GREEN has long upper head",long_upper(d)),("Broader candle trend is bearish",trend<-0.08)],"3 RED + long-head GREEN in bearish context","Next candle RED only with trend alignment.")

    # Type 11 — only works when a real 30-second closed-candle feed is supplied.
    prior_high=max(x["high"] for x in (a,b,c))
    add(11,1,[("30-second timeframe",tf=="30s"),("RED, RED, RED, GREEN setup",seq_is([a,b,c,d],[-1,-1,-1,1])),("First 3 RED candles normal",all(normal(x) for x in (a,b,c))),("GREEN does not break previous 3 RED highs",d["high"]<=prior_high+tol*0.35)],"3 normal RED + contained GREEN","Next 30s candle GREEN.",tf_rule="30S ONLY")

    # Type 12 — 2m only: RR + GG contained below first-red resistance.
    resistance=max(a["high"],b["high"])
    greens_contained=c["high"]<=resistance+tol*0.35 and d["high"]<=resistance+tol*0.35
    close_near=abs(d["close"]-resistance)<=max(tol*2.8,med_range*0.65)
    add(12,-1,[("2-minute timeframe",tf=="2m"),("RED, RED, GREEN, GREEN setup",seq_is([a,b,c,d],[-1,-1,1,1])),("Normal body candles",all(normal(x) for x in (a,b,c,d))),("GREEN candles do not break first RED resistance",greens_contained),("Last GREEN stays near horizontal level",close_near)],"2 RED + 2 GREEN below resistance","Next 2m candle RED.","Horizontal Level",False,"2M ONLY")

    # Type 13 — 2m false-break reversal: level must be swept and then reclaimed.
    prior=candles[-10:-1] if count>=10 else candles[:-1]; last=candles[-1]
    high_clusters=_sk25_level_clusters([x["high"] for x in prior],tol*1.25,2)
    low_clusters=_sk25_level_clusters([x["low"] for x in prior],tol*1.25,2)
    resistance=max(high_clusters) if high_clusters else max(x["high"] for x in prior)
    support=min(low_clusters) if low_clusters else min(x["low"] for x in prior)
    res_touches=sum(1 for x in prior if abs(x["high"]-resistance)<=tol*1.25)
    sup_touches=sum(1 for x in prior if abs(x["low"]-support)<=tol*1.25)
    up_sweep_reject=last["high"]>resistance+tol*0.18 and last["close"]<resistance+tol*0.05 and long_upper(last)
    dn_sweep_reject=last["low"]<support-tol*0.18 and last["close"]>support-tol*0.05 and long_lower(last)
    add(13,-1,[("2-minute timeframe",tf=="2m"),("Resistance retested several times",res_touches>=2),("Latest candle sweeps above resistance then closes back below",up_sweep_reject)],"Repeated resistance retest + false breakout rejection","Next 2m candle RED only after a confirmed false breakout.","Breakout Reversal",False,"2M ONLY")
    add(13,1,[("2-minute timeframe",tf=="2m"),("Support retested several times",sup_touches>=2),("Latest candle sweeps below support then closes back above",dn_sweep_reject)],"Repeated support retest + false breakdown rejection","Next 2m candle GREEN only after a confirmed false breakdown.","Breakout Reversal",False,"2M ONLY")

    # Type 14 — R,G,G define support then RED breaks; mirror G,R,R + GREEN breakout.
    a,b,c,d=candles[-4:]
    support_ok=seq_is([a,b,c,d],[-1,1,1,-1]) and abs(b["low"]-c["low"])<=tol*1.25 and d["close"]<((b["low"]+c["low"])/2)-tol*0.20
    resistance_ok=seq_is([a,b,c,d],[1,-1,-1,1]) and abs(b["high"]-c["high"])<=tol*1.25 and d["close"]>((b["high"]+c["high"])/2)+tol*0.20
    add(14,-1,[("RED + 2 GREEN define support",seq_is([a,b,c],[-1,1,1]) and abs(b["low"]-c["low"])<=tol*1.25),("Latest RED closes below support",support_ok)],"Horizontal support breakdown","Next candle RED.","Horizontal Break")
    add(14,1,[("GREEN + 2 RED define resistance",seq_is([a,b,c],[1,-1,-1]) and abs(b["high"]-c["high"])<=tol*1.25),("Latest GREEN closes above resistance",resistance_ok)],"Horizontal resistance breakout","Next candle GREEN.","Horizontal Break")

    # Type 15 — V / inverted-V reversal only after a breakout attempt is rejected.
    shape=candles[-7:-1]; last=candles[-1]
    closes=[x["close"] for x in shape]
    low_i=closes.index(min(closes)); high_i=closes.index(max(closes))
    v_shape=1<=low_i<=len(shape)-2 and (closes[0]-closes[low_i])>=med_range*0.8 and (closes[-1]-closes[low_i])>=med_range*0.7
    iv_shape=1<=high_i<=len(shape)-2 and (closes[high_i]-closes[0])>=med_range*0.8 and (closes[high_i]-closes[-1])>=med_range*0.7
    v_level=max(shape[0]["high"],shape[1]["high"]); iv_level=min(shape[0]["low"],shape[1]["low"])
    v_false_break=last["high"]>v_level+tol*0.15 and last["close"]<v_level+tol*0.03 and long_upper(last)
    iv_false_break=last["low"]<iv_level-tol*0.15 and last["close"]>iv_level-tol*0.03 and long_lower(last)
    add(15,-1,[("V shape formed",v_shape),("Upside breakout attempt is rejected",v_false_break)],"V + failed upside breakout","Opposite-direction target RED only after rejection back inside the level.","V Reversal")
    add(15,1,[("Inverted-V shape formed",iv_shape),("Downside breakout attempt is rejected",iv_false_break)],"Inverted V + failed downside breakout","Opposite-direction target GREEN only after rejection back inside the level.","V Reversal")

    # Types 16/17 — 3-4 continuation run then one opposite candle.
    last=candles[-1]
    if last["dir"]<0:
        run=0; i=count-2
        while i>=0 and candles[i]["dir"]>0 and run<5: run+=1; i-=1
        seq=candles[count-run-1:count-1] if run else []
        add(16,1,[("3 to 4 back-to-back GREEN candles",3<=run<=4),("GREEN bodies normal",bool(seq) and all(normal(x) for x in seq)),("One opposite RED setup candle",True)],"3-4 normal GREEN + 1 RED","Next candle GREEN.")
    if last["dir"]>0:
        run=0; i=count-2
        while i>=0 and candles[i]["dir"]<0 and run<5: run+=1; i-=1
        seq=candles[count-run-1:count-1] if run else []
        add(17,-1,[("3 to 4 back-to-back RED candles",3<=run<=4),("RED bodies normal",bool(seq) and all(normal(x) for x in seq)),("One opposite GREEN setup candle",True)],"3-4 normal RED + 1 GREEN","Next candle RED.")

    # Types 18/19 — no automatic Martingale/recovery.
    a,b,c,d,e=candles[-5:]
    add(18,-1,[("Long RED marubozu first candle",a["dir"]<0 and marubozu(a)),("Then GREEN, GREEN, GREEN, RED",seq_is([b,c,d,e],[1,1,1,-1])),("Three GREEN candles normal",all(normal(x) for x in (b,c,d))),("No wick/body breaks first RED resistance",all(x["high"]<=a["high"]+tol*0.30 for x in (b,c,d,e))),("Sideways/mixed context",abs(trend)<0.72)],"Long RED + 3 GREEN + RED below resistance","Next candle RED.","Sideways Level")
    add(19,1,[("Long GREEN marubozu first candle",a["dir"]>0 and marubozu(a)),("Then RED, RED, RED, GREEN",seq_is([b,c,d,e],[-1,-1,-1,1])),("Three RED candles normal",all(normal(x) for x in (b,c,d))),("No wick/body breaks first GREEN support",all(x["low"]>=a["low"]-tol*0.30 for x in (b,c,d,e))),("Sideways/mixed context",abs(trend)<0.72)],"Long GREEN + 3 RED + GREEN above support","Next candle GREEN.","Sideways Level")

    # Types 20/21.
    a,b,c,d=candles[-4:]
    add(20,-1,[("Downtrend context",trend<-0.10),("RED, RED, GREEN, RED setup",seq_is([a,b,c,d],[-1,-1,1,-1])),("First two RED candles normal",normal(a) and normal(b)),("4th RED does not break previous GREEN low",d["low"]>=c["low"]-tol*0.35)],"Downtrend R-R-G-R hold","Next candle RED.","Downtrend")
    add(21,1,[("Uptrend context",trend>0.10),("GREEN, GREEN, RED, GREEN setup",seq_is([a,b,c,d],[1,1,-1,1])),("First two GREEN candles normal",normal(a) and normal(b)),("4th GREEN does not break previous RED high",d["high"]<=c["high"]+tol*0.35)],"Uptrend G-G-R-G hold","Next candle GREEN.","Uptrend")

    # Types 22/23.
    last=candles[-1]; prev=candles[-2]
    if last["dir"]<0:
        run=0; i=count-2
        while i>=0 and candles[i]["dir"]>0 and run<6: run+=1; i-=1
        greens=candles[count-run-1:count-1] if run else []
        add(22,1,[("Uptrend context",trend>0.08),("3 to 5 back-to-back GREEN candles",3<=run<=5),("GREEN candles normal",bool(greens) and all(normal(x) for x in greens)),("Opposite RED body smaller than previous GREEN",last["body"]<prev["body"]),("RED body does not break previous GREEN body",body_inside(last,prev,tol*0.20))],"3-5 GREEN + smaller contained RED","Next candle GREEN.","Uptrend")
    if last["dir"]>0:
        run=0; i=count-2
        while i>=0 and candles[i]["dir"]<0 and run<6: run+=1; i-=1
        reds=candles[count-run-1:count-1] if run else []
        add(23,-1,[("Downtrend context",trend<-0.08),("3 to 5 back-to-back RED candles",3<=run<=5),("RED candles normal",bool(reds) and all(normal(x) for x in reds)),("Opposite GREEN body smaller than previous RED",last["body"]<prev["body"]),("GREEN body does not break previous RED body",body_inside(last,prev,tol*0.20))],"3-5 RED + smaller contained GREEN","Next candle RED.","Downtrend")

    # Type 24 — LIVE sideways: G,R,small-R,G,smaller-R at S/R -> next green.
    a,b,c,d,e=candles[-5:]
    snr=(a["high"]+b["high"])/2.0
    snr_respected=all(x["high"]<=snr+tol*0.40 for x in (a,b,c,d,e))
    add(24,1,[("LIVE market",is_live),("GREEN, RED, RED, GREEN, RED setup",seq_is([a,b,c,d,e],[1,-1,-1,1,-1])),("First GREEN and second RED maintain S/R",abs(a["high"]-b["high"])<=tol*1.45),("3rd RED is Doji/small",small(c)),("4th GREEN does not break S/R",d["high"]<=snr+tol*0.40),("5th RED body smaller than previous GREEN",e["body"]<d["body"]),("No setup candle breaks S/R",snr_respected),("Sideways/mixed context",abs(trend)<0.72)],"LIVE sideways G-R-smallR-G-smallR at S/R","Next candle GREEN.","Live S/R",False,"LIVE ONLY")

    # Type 25 — small red, normal red, long green breaks first-red resistance -> next green.
    a,b,c=candles[-3:]
    add(25,1,[("RED, RED, GREEN setup",seq_is([a,b,c],[-1,-1,1])),("1st RED small/Doji",small(a)),("2nd RED normal",normal(b)),("3rd GREEN long",long_body(c)),("Long GREEN closes above 1st RED resistance",c["close"]>a["high"]+tol*0.15)],"Small RED + normal RED + long GREEN breakout","Next candle GREEN.","S/R Breakout")

    # =========================================================
    # V39 NEW SELECTED STRATEGIES
    # IDs 26-30 = selected PDF setups. IDs 31-35 = premium price-action rules.
    # All use only CLOSED OHLC candles and exact rule matching.
    # =========================================================
    def trend_before_setup(setup_len, lookback):
        base = candles[:-setup_len] if setup_len and len(candles) > setup_len else candles
        return _sk25_trend(base, lookback)

    def touch_price(c, level, margin=0.35):
        return c["low"] <= level + tol*margin and c["high"] >= level - tol*margin

    def bullish_engulf(curr, prev, extra=0.15):
        return curr["dir"] > 0 and body_inside(prev, curr, tol*extra)

    def bearish_engulf(curr, prev, extra=0.15):
        return curr["dir"] < 0 and body_inside(prev, curr, tol*extra)

    def prior_resistance(setup_len, lookback=14):
        prior = candles[max(0, count-setup_len-lookback):count-setup_len]
        return max((x["high"] for x in prior), default=None)

    def prior_support(setup_len, lookback=14):
        prior = candles[max(0, count-setup_len-lookback):count-setup_len]
        return min((x["low"] for x in prior), default=None)

    # PDF SETUP 1 — trend + S/R touch by opposite candle + breakout close.
    if count >= 8:
        a,b = candles[-2:]
        res = prior_resistance(2); sup = prior_support(2)
        big_t = trend_before_setup(2, 12); small_t = trend_before_setup(2, 5)
        if res is not None:
            add(26,1,[("Big trend UP",big_t>0.04),("Small trend UP",small_t>0.04),("RED opposite candle",a["dir"]<0),("RED touches resistance/SNR",touch_price(a,res,0.75)),("RED closes at/below SNR",a["close"]<=res+tol*0.18),("GREEN breakout candle",b["dir"]>0 and normal(b)),("GREEN closes above SNR",b["close"]>res+tol*0.18)],"PDF S1 bullish S/R breakout","Trend-aligned opposite candle tests S/R, then a closed green candle breaks above it.","PDF · Trend Breakout")
        if sup is not None:
            add(26,-1,[("Big trend DOWN",big_t<-0.04),("Small trend DOWN",small_t<-0.04),("GREEN opposite candle",a["dir"]>0),("GREEN touches support/SNR",touch_price(a,sup,0.75)),("GREEN closes at/above SNR",a["close"]>=sup-tol*0.18),("RED breakout candle",b["dir"]<0 and normal(b)),("RED closes below SNR",b["close"]<sup-tol*0.18)],"PDF S1 bearish S/R breakout","Trend-aligned opposite candle tests S/R, then a closed red candle breaks below it.","PDF · Trend Breakout")

    # PDF SETUP 4 — breakout, continuation, opposite engulf/retest that still holds the broken level.
    if count >= 9:
        a,b,c = candles[-3:]
        res = prior_resistance(3); sup = prior_support(3)
        big_t = trend_before_setup(3, 12); small_t = trend_before_setup(3, 5)
        if res is not None:
            add(27,1,[("Big trend UP",big_t>0.04),("Small trend UP",small_t>0.04),("Candle 1 GREEN breaks S/R",a["dir"]>0 and a["close"]>res+tol*0.15),("Candle 2 GREEN continuation",b["dir"]>0 and normal(b)),("Candle 3 RED engulfs candle 2",bearish_engulf(c,b,0.20)),("Candle 3 retests broken S/R",touch_price(c,res,0.45)),("Candle 3 closes above S/R",c["close"]>res-tol*0.05)],"PDF S4 bullish breakout-engulf-retest","Broken resistance is retested by an opposite engulfing candle but remains support.","PDF · Breakout Retest")
        if sup is not None:
            add(27,-1,[("Big trend DOWN",big_t<-0.04),("Small trend DOWN",small_t<-0.04),("Candle 1 RED breaks S/R",a["dir"]<0 and a["close"]<sup-tol*0.15),("Candle 2 RED continuation",b["dir"]<0 and normal(b)),("Candle 3 GREEN engulfs candle 2",bullish_engulf(c,b,0.20)),("Candle 3 retests broken S/R",touch_price(c,sup,0.45)),("Candle 3 closes below S/R",c["close"]<sup+tol*0.05)],"PDF S4 bearish breakout-engulf-retest","Broken support is retested by an opposite engulfing candle but remains resistance.","PDF · Breakout Retest")

    # PDF SETUP 6 — four-candle S/R hold after engulfing impulse.
    if count >= 10:
        a,b,c,d = candles[-4:]
        big_t = trend_before_setup(4, 12); small_t = trend_before_setup(4, 5)
        wick_both = b["upper_wick"]>=max(tol*0.18,b["body"]*0.16) and b["lower_wick"]>=max(tol*0.18,b["body"]*0.16)
        if a["dir"] < 0:
            level = a["body_top"]
            add(28,1,[("Big trend UP",big_t>0.02),("Small trend UP",small_t>0.02),("Candle 1 normal RED",a["dir"]<0 and normal(a)),("Candle 2 small RED with two wicks",b["dir"]<0 and small(b) and wick_both),("Candle 3 GREEN covers candle 1",bullish_engulf(c,a,0.22) and c["close"]>level+tol*0.08),("Candle 4 small RED",d["dir"]<0 and small(d)),("Candle 4 touches candle-1 S/R",touch_price(d,level,0.40)),("Candle 4 closes above S/R",d["close"]>level-tol*0.05)],"PDF S6 bullish four-candle S/R hold","Engulfing recovery clears candle 1 and a small pullback holds its S/R level.","PDF · Four Candle Hold")
        if a["dir"] > 0:
            level = a["body_bottom"]
            add(28,-1,[("Big trend DOWN",big_t<-0.02),("Small trend DOWN",small_t<-0.02),("Candle 1 normal GREEN",a["dir"]>0 and normal(a)),("Candle 2 small GREEN with two wicks",b["dir"]>0 and small(b) and wick_both),("Candle 3 RED covers candle 1",bearish_engulf(c,a,0.22) and c["close"]<level-tol*0.08),("Candle 4 small GREEN",d["dir"]>0 and small(d)),("Candle 4 touches candle-1 S/R",touch_price(d,level,0.40)),("Candle 4 closes below S/R",d["close"]<level+tol*0.05)],"PDF S6 bearish four-candle S/R hold","Engulfing drop clears candle 1 and a small pullback holds its S/R level.","PDF · Four Candle Hold")

    # PDF SETUP 11 — second candle sweeps the first wick and closes around 50% of candle-1 body.
    if count >= 2:
        a,b = candles[-2:]
        mid = (a["body_top"] + a["body_bottom"]) / 2.0
        mid_ok = abs(b["close"]-mid) <= max(a["body"]*0.28, tol*0.50)
        add(29,1,[("Candle 1 normal GREEN",a["dir"]>0 and normal(a)),("Candle 2 RED",b["dir"]<0),("Candle 2 wick sweeps candle-1 low",b["low"]<a["low"]-tol*0.10 and long_lower(b)),("Candle 2 close near 50% of candle-1 body",mid_ok and b["close"]>a["body_bottom"]-tol*0.10)],"PDF S11 bullish 50% wick sweep","A red candle sweeps the prior green low but rejects back near the 50% body area.","PDF · Wick Sweep")
        add(29,-1,[("Candle 1 normal RED",a["dir"]<0 and normal(a)),("Candle 2 GREEN",b["dir"]>0),("Candle 2 wick sweeps candle-1 high",b["high"]>a["high"]+tol*0.10 and long_upper(b)),("Candle 2 close near 50% of candle-1 body",mid_ok and b["close"]<a["body_top"]+tol*0.10)],"PDF S11 bearish 50% wick sweep","A green candle sweeps the prior red high but rejects back near the 50% body area.","PDF · Wick Sweep")

    # PDF SETUP 13 — candle 2 fully engulfs candle 1; candle 3 holds candle-1 opening level.
    if count >= 3:
        a,b,c = candles[-3:]
        if a["dir"] < 0:
            level = a["open"]
            add(30,1,[("Candle 1 RED",a["dir"]<0),("Candle 2 GREEN totally engulfs candle 1",bullish_engulf(b,a,0.18)),("Candle 3 RED",c["dir"]<0),("Candle 3 does not break candle-1 open",c["low"]>=level-tol*0.22 and c["close"]>=level-tol*0.06)],"PDF S13 bullish engulf + open-level hold","Bullish engulfing impulse is followed by a red candle that holds the first candle opening level.","PDF · Engulf Hold")
        if a["dir"] > 0:
            level = a["open"]
            add(30,-1,[("Candle 1 GREEN",a["dir"]>0),("Candle 2 RED totally engulfs candle 1",bearish_engulf(b,a,0.18)),("Candle 3 GREEN",c["dir"]>0),("Candle 3 does not break candle-1 open",c["high"]<=level+tol*0.22 and c["close"]<=level+tol*0.06)],"PDF S13 bearish engulf + open-level hold","Bearish engulfing impulse is followed by a green candle that holds the first candle opening level.","PDF · Engulf Hold")

    # PREMIUM 1 — S/R breakout + retest + fresh same-direction confirmation.
    if count >= 9:
        a,b,c = candles[-3:]
        res = prior_resistance(3); sup = prior_support(3)
        if res is not None:
            add(31,1,[("GREEN breakout closes above resistance",a["dir"]>0 and a["close"]>res+tol*0.15),("RED retest candle",b["dir"]<0 and touch_price(b,res,0.45)),("Retest closes above broken resistance",b["close"]>res-tol*0.04),("GREEN confirmation",c["dir"]>0 and normal(c)),("Confirmation closes above retest",c["close"]>b["body_top"]+tol*0.04)],"S/R breakout-retest-confirmation CALL","A closed breakout is retested and then confirmed by a new bullish candle.","Premium · Breakout Retest")
        if sup is not None:
            add(31,-1,[("RED breakout closes below support",a["dir"]<0 and a["close"]<sup-tol*0.15),("GREEN retest candle",b["dir"]>0 and touch_price(b,sup,0.45)),("Retest closes below broken support",b["close"]<sup+tol*0.04),("RED confirmation",c["dir"]<0 and normal(c)),("Confirmation closes below retest",c["close"]<b["body_bottom"]-tol*0.04)],"S/R breakout-retest-confirmation PUT","A closed breakdown is retested and then confirmed by a new bearish candle.","Premium · Breakout Retest")

    # PREMIUM 2 — liquidity sweep/wick rejection + confirmation candle.
    if count >= 8:
        a,b = candles[-2:]
        res = prior_resistance(2); sup = prior_support(2)
        if sup is not None:
            add(32,1,[("Sweep candle trades below prior support",a["low"]<sup-tol*0.12),("Sweep closes back above support",a["close"]>sup+tol*0.04),("Long lower rejection wick",long_lower(a)),("GREEN confirmation candle",b["dir"]>0 and normal(b)),("Confirmation closes above sweep body",b["close"]>a["body_top"]+tol*0.04)],"Liquidity sweep bullish reversal","Prior support is swept by wick, reclaimed on close, then confirmed bullish.","Premium · Liquidity Sweep")
        if res is not None:
            add(32,-1,[("Sweep candle trades above prior resistance",a["high"]>res+tol*0.12),("Sweep closes back below resistance",a["close"]<res-tol*0.04),("Long upper rejection wick",long_upper(a)),("RED confirmation candle",b["dir"]<0 and normal(b)),("Confirmation closes below sweep body",b["close"]<a["body_bottom"]-tol*0.04)],"Liquidity sweep bearish reversal","Prior resistance is swept by wick, reclaimed on close, then confirmed bearish.","Premium · Liquidity Sweep")

    # PREMIUM 3 — trend-aligned two-candle pullback followed by continuation impulse.
    if count >= 9:
        a,b,c = candles[-3:]
        big_t = trend_before_setup(3, 12); small_t = trend_before_setup(3, 6)
        add(33,1,[("Big trend UP",big_t>0.05),("Small trend UP",small_t>0.03),("Two RED pullback candles",seq_is([a,b],[-1,-1])),("Pullback candles not oversized",a["body"]<=med_body*1.45 and b["body"]<=med_body*1.45),("GREEN continuation candle",c["dir"]>0 and c["body_ratio"]>=0.45 and c["body"]>=med_body*0.85),("Continuation clears pullback highs",c["close"]>max(a["high"],b["high"])+tol*0.04)],"Trend pullback continuation CALL","Two controlled red pullback candles are followed by a trend-aligned bullish continuation close.","Premium · Trend Pullback")
        add(33,-1,[("Big trend DOWN",big_t<-0.05),("Small trend DOWN",small_t<-0.03),("Two GREEN pullback candles",seq_is([a,b],[1,1])),("Pullback candles not oversized",a["body"]<=med_body*1.45 and b["body"]<=med_body*1.45),("RED continuation candle",c["dir"]<0 and c["body_ratio"]>=0.45 and c["body"]>=med_body*0.85),("Continuation clears pullback lows",c["close"]<min(a["low"],b["low"])-tol*0.04)],"Trend pullback continuation PUT","Two controlled green pullback candles are followed by a trend-aligned bearish continuation close.","Premium · Trend Pullback")

    # PREMIUM 4 — failed breakout: close outside level, immediate reclaim, then reversal confirmation.
    if count >= 9:
        a,b,c = candles[-3:]
        res = prior_resistance(3); sup = prior_support(3)
        if sup is not None:
            add(34,1,[("RED closes below support",a["dir"]<0 and a["close"]<sup-tol*0.12),("GREEN reclaims support",b["dir"]>0 and b["close"]>sup+tol*0.04),("GREEN confirmation candle",c["dir"]>0 and normal(c)),("Confirmation extends above reclaim",c["close"]>b["close"]+max(tol*0.04,med_body*0.08))],"Failed breakdown bullish reversal","A breakdown closes below support, immediately fails, and receives bullish confirmation.","Premium · Failed Breakout")
        if res is not None:
            add(34,-1,[("GREEN closes above resistance",a["dir"]>0 and a["close"]>res+tol*0.12),("RED falls back below resistance",b["dir"]<0 and b["close"]<res-tol*0.04),("RED confirmation candle",c["dir"]<0 and normal(c)),("Confirmation extends below reclaim",c["close"]<b["close"]-max(tol*0.04,med_body*0.08))],"Failed breakout bearish reversal","A breakout closes above resistance, immediately fails, and receives bearish confirmation.","Premium · Failed Breakout")

    # PREMIUM 5 — engulfing only at a real recent support/resistance level.
    if count >= 8:
        a,b = candles[-2:]
        res = prior_resistance(2); sup = prior_support(2)
        if sup is not None:
            add(35,1,[("RED setup candle at support",a["dir"]<0 and touch_price(a,sup,0.55)),("GREEN bullish engulfing",bullish_engulf(b,a,0.18)),("Engulf closes above support",b["close"]>sup+tol*0.04)],"Bullish engulfing at key support","A true body engulf occurs at recent support and closes back above the level.","Premium · Engulfing S/R")
        if res is not None:
            add(35,-1,[("GREEN setup candle at resistance",a["dir"]>0 and touch_price(a,res,0.55)),("RED bearish engulfing",bearish_engulf(b,a,0.18)),("Engulf closes below resistance",b["close"]<res-tol*0.04)],"Bearish engulfing at key resistance","A true body engulf occurs at recent resistance and closes back below the level.","Premium · Engulfing S/R")

    exact.sort(key=lambda x:(int(x["priority"]),int(x["rules_total"]),int(x["pattern_type"])),reverse=True)
    near.sort(key=lambda x:(float(x["score"]),int(x["rules_total"])),reverse=True)
    directions={x["direction"] for x in exact}
    conflict=len(directions)>1
    best=None if conflict or not exact else exact[0]
    closest=near[0] if near else None
    closed_epoch=candles[-1]["epoch"]

    if conflict:
        return {
            "signal":"NO SIGNAL","score":0.0,"pattern_type":0,"selected_pattern":"CONFLICTING ACTIVE STRATEGIES",
            "pattern_direction":"NONE","next_candle_color":"NONE","setup_match":0.0,
            "rules":[],"pattern_signals":exact[:8],"conflict_gate":True,
            "reason":"NO TRADE · CONFLICTING SETUPS: exact UP and DOWN active strategy rules are both present on the same closed-candle snapshot.",
            "closed_candle_epoch":closed_epoch,
        }
    if not best:
        watch=f" Closest: {closest['name']} {closest['rules_matched']}/{closest['rules_total']} rules." if closest else ""
        return {
            "signal":"NO SIGNAL","score":0.0,"pattern_type":0,"selected_pattern":"NO ACTIVE STRATEGY SETUP",
            "pattern_direction":"NONE","next_candle_color":"NONE","setup_match":float(closest["score"]) if closest else 0.0,
            "rules":list(closest.get("rules") or []) if closest else [],"pattern_signals":near[:8],"conflict_gate":False,
            "reason":"No exact active strategy setup is complete on the latest CLOSED candles."+watch,
            "closed_candle_epoch":closed_epoch,
        }

    return {
        "signal":best["signal"],"score":100.0,"pattern_type":best["pattern_type"],"selected_pattern":best["name"],
        "pattern_direction":best["direction"],"next_candle_color":best["next_candle"],"setup_match":100.0,
        "rules":best["rules"],"pattern_signals":exact[:8],"conflict_gate":False,
        "setup":best["setup"],"reason":f"{best['name']} exact setup matched {best['rules_matched']}/{best['rules_total']} rules. NEXT candle {best['next_candle']} ({best['direction']}).",
        "family":best["family"],"recovery_trade":bool(best.get("recovery_trade")),"timeframe_rule":best.get("timeframe_rule"),
        "pattern_priority":best["priority"],"rules_matched":best["rules_matched"],"rules_total":best["rules_total"],
        "closed_candle_epoch":closed_epoch,
    }


def format_market_data_age(seconds):
    if seconds is None: return "--"
    try: seconds=max(0,int(float(seconds)))
    except Exception: return "--"
    if seconds<60: return f"{seconds}s"
    if seconds<3600: return f"{seconds//60}m {seconds%60}s"
    return f"{seconds//3600}h {(seconds%3600)//60}m"


def market_result_is_countable(result):
    result=result or {}
    return not bool(result.get("exclude_from_history") or result.get("source_stale") or result.get("data_delayed"))


def batch_results_are_countable(results):
    return any(market_result_is_countable(x) for x in (results or []) if isinstance(x,dict))


def no_signal_result(pair, reason, symbol=None, data_age=None, timeframes=None, source_info=None):
    source_info=source_info or _market_source_info(pair,symbol)
    return {
        "pair":pair,"score":0.0,"signal":"NO SIGNAL","reason":str(reason or "No exact active strategy setup."),
        "pattern_type":0,"selected_pattern":"NO ACTIVE STRATEGY SETUP","pattern_direction":"NONE","next_candle_color":"NONE",
        "setup_match":0.0,"rules":[],"pattern_signals":[],"conflict_gate":False,"recovery_trade":False,
        "data_age":round(float(data_age),2) if data_age is not None else None,
        "source":source_info.get("source") or "Broker Native WebSocket","source_mode":source_info.get("source_mode") or ("underlying_proxy" if "(OTC)" in pair else "live_reference"),
        "backup_used":bool(source_info.get("backup_used")),"provider_symbol":source_info.get("provider_symbol"),"yahoo_symbol":symbol,
        "timeframe_summary":timeframes or {},"timeframes_scanned":[],"no_trade":True,
        "no_trade_reason":str(reason or "No exact active strategy setup."),"quality_gate":"PATTERN_ONLY",
        "engine":SK25_ENGINE_VERSION,
    }


def serialize_candles(df, limit=28):
    if df is None or getattr(df,"empty",True): return []
    out=[]
    for idx,row in df.tail(max(8,min(int(limit),60))).iterrows():
        try:
            out.append({"t":int(idx.timestamp()),"o":round(float(row["Open"]),8),"h":round(float(row["High"]),8),"l":round(float(row["Low"]),8),"c":round(float(row["Close"]),8)})
        except Exception: continue
    return out


def normalize_scan_options(_raw):
    # Compatibility object for existing UI/routes. Strategy rules stay unchanged.
    # BALANCED affects only the post-strategy acceptance layer for near-complete setups.
    raw = _raw if isinstance(_raw, dict) else {}
    mode = str(raw.get("mode") or "SK25_BALANCED").strip().upper()
    if mode not in {"SK25_BALANCED", "SK25_STRICT"}:
        mode = "SK25_BALANCED"
    return {
        "mode": mode,
        "min_tf": 1,
        "min_agreement": 100.0,
        "min_score": 100.0,
        "vol_min": 0.0,
        "vol_max": 999.0,
        # V77 ACCURACY MODE: near-match scores are retained for diagnostics only.
        # Live trade signals require an exact SK25 setup; these higher thresholds make
        # WATCH/diagnostic near-matches conservative without manufacturing a trade.
        "good_near_match": 75.0,
        "caution_near_match": 80.0,
        "bad_market_block": True,
    }


def _balanced_market_health_from_tf(tf_df, timeframe, market_name):
    """Reuse the existing market-health model without fetching data again."""
    if tf_df is None or getattr(tf_df, "empty", True) or len(tf_df) < 25:
        return {"grade": "BAD", "score": 0.0, "tradeable": False}
    structure = _market_health_structure(tf_df)
    replay = _market_health_sk25_replay(tf_df, timeframe, market_name)
    score = float(structure.get("score") or 0.0)
    sample = int(replay.get("sample_size") or 0)
    wr = replay.get("win_rate")
    if sample >= 4 and wr is not None:
        score += max(-18.0, min(18.0, (float(wr) - 50.0) * 0.45))
    elif sample == 0:
        score -= 7.0
    score = round(max(0.0, min(100.0, score)), 1)
    if score >= 70.0:
        grade = "GOOD"
    elif score >= 50.0:
        grade = "CAUTION"
    else:
        grade = "BAD"
    return {"grade": grade, "score": score, "tradeable": grade != "BAD", "structure": structure, "replay": replay}


def _apply_balanced_acceptance(strategy, tf_df, timeframe, market_name, opts):
    """
    Post-strategy acceptance only. Existing SK25 strategy definitions are not changed.
    Exact matches remain exact. Conflict/BAD market remain blocked.
    On GOOD/CAUTION markets, the highest near-complete coded setup may be promoted.
    """
    strategy = dict(strategy or {})
    health = _balanced_market_health_from_tf(tf_df, timeframe, market_name)
    strategy["market_health_grade"] = health["grade"]
    strategy["market_health_score"] = health["score"]
    strategy["balanced_acceptance_applied"] = False

    if str(opts.get("mode") or "").upper() == "SK25_STRICT":
        strategy["acceptance_mode"] = "STRICT"
        return strategy
    strategy["acceptance_mode"] = "BALANCED"

    # Never alter an exact signal or override a coded conflict gate.
    if strategy.get("signal") in {"CALL", "PUT"} or bool(strategy.get("conflict_gate")):
        return strategy
    if health["grade"] == "BAD":
        strategy["balanced_block_reason"] = "BAD market health blocks near-match promotion."
        return strategy

    threshold = float(opts.get("good_near_match", 75.0) if health["grade"] == "GOOD" else opts.get("caution_near_match", 80.0))
    if V78_BALANCED_ACCURACY and V78_REQUIRE_EXACT_SK25:
        strategy["balanced_threshold"] = threshold
        strategy["accuracy_exact_match_required"] = True
        strategy["balanced_block_reason"] = "V78 Balanced Accuracy exact-only mode is optional; strong near-matches may be released when this flag is off."
        return strategy
    near = [x for x in (strategy.get("pattern_signals") or []) if isinstance(x, dict)]
    eligible = [x for x in near if float(x.get("score") or 0.0) >= threshold]
    if not eligible:
        strategy["balanced_threshold"] = threshold
        return strategy

    # Keep conflict protection: if qualifying near-setups point both ways, release NO SIGNAL.
    dirs = {str(x.get("direction") or "").upper() for x in eligible if str(x.get("direction") or "").upper() in {"UP", "DOWN"}}
    if len(dirs) > 1:
        strategy.update({
            "signal": "NO SIGNAL",
            "pattern_direction": "NONE",
            "next_candle_color": "NONE",
            "conflict_gate": True,
            "reason": f"NO TRADE · BALANCED CONFLICT: qualifying near-complete UP and DOWN setups both exceed the {threshold:.0f}% {health['grade']} threshold.",
            "balanced_threshold": threshold,
            "balanced_block_reason": "opposite near-complete setups conflict",
        })
        return strategy

    best = max(eligible, key=lambda x: (float(x.get("score") or 0.0), int(x.get("priority") or 0), int(x.get("rules_total") or 0)))
    match = float(best.get("score") or 0.0)
    direction = str(best.get("direction") or "").upper()
    signal = str(best.get("signal") or ("CALL" if direction == "UP" else "PUT" if direction == "DOWN" else "NO SIGNAL")).upper()
    if signal not in {"CALL", "PUT"} or direction not in {"UP", "DOWN"}:
        return strategy

    strategy.update({
        "signal": signal,
        "score": round(match, 1),
        "pattern_type": int(best.get("pattern_type") or 0),
        "selected_pattern": best.get("name") or strategy.get("selected_pattern"),
        "pattern_direction": direction,
        "next_candle_color": best.get("next_candle") or ("GREEN" if direction == "UP" else "RED"),
        "setup_match": round(match, 1),
        "rules": list(best.get("rules") or []),
        "setup": best.get("setup") or "",
        "family": best.get("family") or "",
        "recovery_trade": bool(best.get("recovery_trade")),
        "timeframe_rule": best.get("timeframe_rule"),
        "pattern_priority": int(best.get("priority") or 0),
        "rules_matched": int(best.get("rules_matched") or 0),
        "rules_total": int(best.get("rules_total") or 0),
        "conflict_gate": False,
        "balanced_acceptance_applied": True,
        "balanced_threshold": threshold,
        "reason": f"BALANCED {health['grade']} acceptance · existing strategy {best.get('name') or best.get('pattern_type')} matched {int(best.get('rules_matched') or 0)}/{int(best.get('rules_total') or 0)} coded rules ({match:.1f}%), above the {threshold:.0f}% threshold. Strategy rules were not changed.",
    })
    return strategy


def _last_strategy_outcome(user):
    user=normalize_user_id(user)
    if not user: return ""
    try:
        for item in load_signals():
            if normalize_user_id(item.get("user"))!=user: continue
            result=str(item.get("result") or "").upper()
            if result in {"WIN","LOSS"}: return result
    except Exception: pass
    return ""


def _movement_info(df):
    if df is None or getattr(df,"empty",True) or len(df)<2: return {"label":"--","percent":0.0}
    part=df.tail(min(12,len(df)))
    vals=[]
    for _,row in part.iterrows():
        c=abs(_f(row.get("Close"),0.0)); rng=max(0.0,_f(row.get("High"),0.0)-_f(row.get("Low"),0.0))
        if c>0: vals.append(rng/c*100.0)
    pct=_median(vals,0.0)
    label="LOW" if pct<0.03 else ("HIGH" if pct>0.30 else "NORMAL")
    return {"label":label,"percent":round(pct,5)}


# =========================================================
# PRE-SCAN MARKET HEALTH TEST
# Recent closed-candle structure + rolling SK25 replay. This is a market-fitness
# gate, not a guaranteed win-rate predictor and it never places an order.
# =========================================================
MARKET_HEALTH_VERSION = "RAJA_MARKET_HEALTH_V1"
MARKET_HEALTH_LOOKBACK = max(40, min(120, int(os.environ.get("RAJA_MARKET_HEALTH_LOOKBACK", "80"))))
MARKET_HEALTH_REPLAY_STEPS = max(12, min(40, int(os.environ.get("RAJA_MARKET_HEALTH_REPLAY_STEPS", "28"))))


def _safe_pct(num, den):
    try:
        den = abs(float(den))
        return float(num) / den * 100.0 if den > 1e-12 else 0.0
    except Exception:
        return 0.0


def _market_health_structure(tf_df):
    """Return indicator-free structure diagnostics from closed OHLC candles only."""
    frame = tf_df.tail(MARKET_HEALTH_LOOKBACK).copy()
    ranges_pct, body_ratios, closes, dirs = [], [], [], []
    for _, row in frame.iterrows():
        o = _f(row.get("Open"), 0.0); h = _f(row.get("High"), 0.0)
        l = _f(row.get("Low"), 0.0); c = _f(row.get("Close"), 0.0)
        mid = max(1e-12, abs((o + c) / 2.0))
        rng = max(0.0, h - l)
        ranges_pct.append(rng / mid * 100.0)
        body_ratios.append(abs(c - o) / max(rng, 1e-12))
        closes.append(c)
        dirs.append(1 if c > o else (-1 if c < o else 0))

    med_range = _median(ranges_pct, 0.0)
    spike_rate = (sum(1 for x in ranges_pct if med_range > 0 and x > med_range * 3.5) / max(1, len(ranges_pct))) * 100.0
    dead_rate = (sum(1 for x in ranges_pct if med_range > 0 and x < med_range * 0.22) / max(1, len(ranges_pct))) * 100.0
    doji_rate = (sum(1 for x in body_ratios if x < 0.15) / max(1, len(body_ratios))) * 100.0

    active_dirs = [d for d in dirs if d]
    flips = sum(1 for a, b in zip(active_dirs, active_dirs[1:]) if a != b)
    flip_rate = flips / max(1, len(active_dirs) - 1) * 100.0

    path = 0.0
    for a, b in zip(closes, closes[1:]):
        path += abs(b - a)
    efficiency = abs(closes[-1] - closes[0]) / max(path, 1e-12) * 100.0 if len(closes) >= 2 else 0.0

    score = 100.0
    reasons, warnings = [], []
    if med_range <= 0.0015:
        score -= 48; reasons.append("Recent candles are nearly flat / inactive.")
    elif med_range < 0.008:
        score -= 14; warnings.append("Movement is very quiet; entries may have weak follow-through.")

    if flip_rate >= 78:
        score -= 28; reasons.append("Very high candle-direction flip rate: market is choppy.")
    elif flip_rate >= 68:
        score -= 16; warnings.append("Direction changes frequently; use selective entries only.")
    elif flip_rate >= 58:
        score -= 7

    if spike_rate >= 18:
        score -= 24; reasons.append("Too many abnormal range spikes were detected.")
    elif spike_rate >= 10:
        score -= 12; warnings.append("Recent volatility contains repeated spikes.")

    if doji_rate >= 45:
        score -= 18; reasons.append("Too many indecision/small-body candles.")
    elif doji_rate >= 32:
        score -= 8; warnings.append("Indecision candles are elevated.")

    if dead_rate >= 35:
        score -= 14; warnings.append("Many candles have unusually small ranges.")

    if efficiency < 7 and flip_rate > 58:
        score -= 14; reasons.append("Price is moving without clean directional efficiency.")
    elif efficiency < 12 and flip_rate > 52:
        score -= 7

    return {
        "score": max(0.0, min(100.0, score)),
        "median_range_pct": round(med_range, 5),
        "flip_rate_pct": round(flip_rate, 1),
        "spike_rate_pct": round(spike_rate, 1),
        "doji_rate_pct": round(doji_rate, 1),
        "dead_candle_rate_pct": round(dead_rate, 1),
        "directional_efficiency_pct": round(efficiency, 1),
        "reasons": reasons,
        "warnings": warnings,
    }


def _market_health_sk25_replay(tf_df, timeframe, market_name):
    """Replay recent closed candles: each prefix predicts the next already-closed candle."""
    frame = tf_df.tail(max(45, MARKET_HEALTH_REPLAY_STEPS + 24)).copy()
    n = len(frame)
    if n < 22:
        return {"sample_size": 0, "wins": 0, "losses": 0, "draws": 0, "win_rate": None, "status": "LEARNING", "patterns": {}}

    start = max(20, n - MARKET_HEALTH_REPLAY_STEPS - 1)
    wins = losses = draws = 0
    patterns = Counter()
    last_outcome = ""
    for next_pos in range(start, n):
        prefix = frame.iloc[:next_pos]
        if len(prefix) < SK25_LIVE_MIN_CANDLES:
            continue
        try:
            setup = analyze_sk25_ohlc(prefix, timeframe, market_name, last_outcome)
        except Exception:
            continue
        signal = str(setup.get("signal") or "").upper()
        if signal not in {"CALL", "PUT"}:
            continue
        nxt = frame.iloc[next_pos]
        o = _f(nxt.get("Open"), 0.0); c = _f(nxt.get("Close"), 0.0)
        if c == o:
            result = "DRAW"; draws += 1
        elif (signal == "CALL" and c > o) or (signal == "PUT" and c < o):
            result = "WIN"; wins += 1
        else:
            result = "LOSS"; losses += 1
        last_outcome = result if result in {"WIN", "LOSS"} else last_outcome
        ptype = int(setup.get("pattern_type") or 0)
        if ptype:
            patterns[str(ptype)] += 1

    decided = wins + losses
    win_rate = round(wins / decided * 100.0, 1) if decided else None
    if decided >= 8:
        status = "STRONG SAMPLE"
    elif decided >= 4:
        status = "USABLE SAMPLE"
    else:
        status = "LEARNING"
    return {
        "sample_size": decided,
        "wins": wins,
        "losses": losses,
        "draws": draws,
        "win_rate": win_rate,
        "status": status,
        "patterns": dict(patterns.most_common(6)),
    }


def calculate_market_health(pair, selected_expiry=None, bridge_user=None, broker=None):
    """Pre-scan gate for one pair. Uses only closed OHLC structure + SK25 rolling replay."""
    tf = str(selected_expiry or "1m").strip().lower()
    base = {
        "pair": pair, "selected_expiry": tf, "health_version": MARKET_HEALTH_VERSION,
        "tradeable": False, "grade": "BAD", "status": "BAD", "score": 0.0,
        "reason": "Market health could not be verified.", "backtest_is_guarantee": False,
    }
    if tf not in TIMEFRAMES:
        base["reason"] = f"{tf} is not available for live market-health testing."
        return base

    base_df, data_age, symbol, source_info = get_market_data(pair, bridge_user=bridge_user, broker=broker)
    base.update({
        "source": (source_info or {}).get("source") or "Unknown",
        "source_mode": (source_info or {}).get("source_mode") or "",
        "provider_symbol": (source_info or {}).get("provider_symbol") or symbol,
        "data_age_seconds": round(float(data_age), 2) if data_age is not None else None,
    })
    if base_df is None or getattr(base_df, "empty", True):
        base["reason"] = (source_info or {}).get("unavailable_reason") or "No market data is available for the health test."
        return base
    if data_age is not None and float(data_age) > MAX_SOURCE_CANDLE_AGE_SECONDS:
        base.update({"reason": f"Market data is stale ({format_market_data_age(data_age)} old).", "stale": True})
        return base

    tf_df = build_timeframe(base_df, TIMEFRAMES[tf])
    if tf_df is None or getattr(tf_df, "empty", True) or len(tf_df) < 25:
        base["reason"] = f"Only {0 if tf_df is None else len(tf_df)} closed {tf} candles are available; at least 25 are required."
        return base

    structure = _market_health_structure(tf_df)
    market_name = "OTC" if "(OTC)" in str(pair).upper() else "LIVE"
    replay = _market_health_sk25_replay(tf_df, tf, market_name)
    score = float(structure["score"])
    sample = int(replay.get("sample_size") or 0)
    wr = replay.get("win_rate")
    notes = list(structure.get("reasons") or [])
    warnings = list(structure.get("warnings") or [])

    if sample >= 4 and wr is not None:
        score += max(-18.0, min(18.0, (float(wr) - 50.0) * 0.45))
        if wr < 45:
            notes.append(f"Recent SK25 replay is weak: {wr:.0f}% over {sample} decided setups.")
        elif wr >= 62:
            warnings.append(f"Recent SK25 replay is supportive: {wr:.0f}% over {sample} decided setups (not a guarantee).")
    elif sample == 0:
        score -= 7.0
        warnings.append("No exact SK25 setup occurred in the recent replay window; market may be inactive for this strategy set.")
    else:
        warnings.append(f"Only {sample} recent SK25 replay setups were available; backtest sample is still small.")

    score = round(max(0.0, min(100.0, score)), 1)
    if score >= 70:
        grade, tradeable = "GOOD", True
        headline = "GOOD MARKET — TRADEABLE"
    elif score >= 50:
        grade, tradeable = "CAUTION", True
        headline = "CAUTION — SELECTIVE TRADES ONLY"
    else:
        grade, tradeable = "BAD", False
        headline = "BAD MARKET — DO NOT TRADE"

    if grade == "BAD" and not notes:
        notes.append("Combined structure and recent SK25 replay score is below the safe pre-scan threshold.")

    base.update({
        "tradeable": tradeable,
        "grade": grade,
        "status": grade,
        "headline": headline,
        "score": score,
        "closed_candles_tested": min(len(tf_df), MARKET_HEALTH_LOOKBACK),
        "structure": structure,
        "backtest": replay,
        "reason": " ".join(notes[:4]) if notes else "Recent closed-candle structure passed the pre-scan health gate.",
        "warnings": warnings[:5],
        "forming_candle_excluded": True,
        "method": "Closed-candle structure + rolling SK25 next-candle replay",
        "backtest_is_guarantee": False,
    })
    return base



# =========================================================
# RAJA AI V47 — BALANCED TECHNICAL CONFIRMATION GATE
# SK25 remains the primary setup engine. These indicators only FILTER an
# already-valid CALL/PUT setup; they never manufacture a signal on their own.
# =========================================================
# V79 SIGNAL FLOW BALANCED:
# - GOOD market: 75% SK25 near-match can qualify
# - CAUTION market: 80% SK25 near-match can qualify
# - 65% technical confirmation minimum
# - hybrid indicator-only signals remain OFF
# - ATR and HTF are soft confirmations by default, not hard blockers
# - BAD market / conflict / learning / regime safety gates remain active
V78_BALANCED_ACCURACY = str(os.environ.get("RAJA_V78_BALANCED_ACCURACY", "1")).strip().lower() not in {"0","false","off","no"}
V78_REQUIRE_EXACT_SK25 = str(os.environ.get("RAJA_V78_REQUIRE_EXACT_SK25", "0")).strip().lower() not in {"0","false","off","no"}
V78_ALLOW_HYBRID_FALLBACK = str(os.environ.get("RAJA_V78_ALLOW_HYBRID_FALLBACK", "0")).strip().lower() in {"1","true","on","yes"}
V78_REQUIRE_HTF = str(os.environ.get("RAJA_V78_REQUIRE_HTF", "0")).strip().lower() not in {"0","false","off","no"}
V78_REQUIRE_VOLATILITY = str(os.environ.get("RAJA_V78_REQUIRE_VOLATILITY", "0")).strip().lower() not in {"0","false","off","no"}

V46_TECH_FILTER_ENABLED = str(os.environ.get("RAJA_V46_TECH_FILTER", "1")).strip().lower() not in {"0","false","off","no"}
# V76: 65 keeps confirmation meaningful but avoids requiring nearly every
# indicator to agree on each scan. Railway env vars can still override this.
V46_MIN_CONFIDENCE = max(60.0, min(95.0, float(os.environ.get("RAJA_V78_MIN_CONFIDENCE", "65"))))
V46_EMA_FAST = max(3, int(os.environ.get("RAJA_V46_EMA_FAST", "9")))
V46_EMA_SLOW = max(V46_EMA_FAST + 2, int(os.environ.get("RAJA_V46_EMA_SLOW", "21")))
V46_RSI_PERIOD = max(5, int(os.environ.get("RAJA_V46_RSI_PERIOD", "14")))
V46_ATR_PERIOD = max(5, int(os.environ.get("RAJA_V46_ATR_PERIOD", "14")))
V46_HTF_MAP = {"1m":"5m", "2m":"5m", "5m":"15m", "10m":"30m", "15m":"30m", "30m":None}


def _v46_indicator_snapshot(df):
    """Return robust closed-candle indicator state. Pure pandas; no TA dependency.

    V80 fixes the old lazy-pandas bug (``pd`` was referenced without being loaded),
    which could incorrectly report "indicator warm-up incomplete" even with 50+
    visible candles. The function now reports actual usable/required counts.
    """
    required = max(28, V46_EMA_SLOW + 5, V46_RSI_PERIOD + 3, V46_ATR_PERIOD + 3)
    try:
        pd = _get_pandas()
        clean = _raja_clean_ohlc_frame(df)
        usable = 0 if clean is None or getattr(clean, "empty", True) else int(len(clean))
        if usable < required:
            return {
                "ready": False, "usable_candles": usable, "required_candles": required,
                "reason": f"Not enough clean CLOSED candles for indicator warm-up ({usable}/{required})."
            }

        close = pd.to_numeric(clean["Close"], errors="coerce").astype(float)
        high = pd.to_numeric(clean["High"], errors="coerce").astype(float)
        low = pd.to_numeric(clean["Low"], errors="coerce").astype(float)

        ema_fast = close.ewm(span=V46_EMA_FAST, adjust=False).mean()
        ema_slow = close.ewm(span=V46_EMA_SLOW, adjust=False).mean()

        delta = close.diff()
        gain = delta.clip(lower=0.0)
        loss = (-delta.clip(upper=0.0))
        avg_gain = gain.ewm(alpha=1.0/V46_RSI_PERIOD, adjust=False, min_periods=V46_RSI_PERIOD).mean()
        avg_loss = loss.ewm(alpha=1.0/V46_RSI_PERIOD, adjust=False, min_periods=V46_RSI_PERIOD).mean()
        eps = 1e-12
        rs = avg_gain / avg_loss.where(avg_loss > eps, eps)
        rsi = 100.0 - (100.0 / (1.0 + rs))
        both_flat = (avg_gain <= eps) & (avg_loss <= eps)
        rsi = rsi.mask(both_flat, 50.0)
        rsi = rsi.mask((avg_loss <= eps) & (avg_gain > eps), 100.0)
        rsi = rsi.mask((avg_gain <= eps) & (avg_loss > eps), 0.0)

        macd_line = close.ewm(span=12, adjust=False).mean() - close.ewm(span=26, adjust=False).mean()
        macd_signal = macd_line.ewm(span=9, adjust=False).mean()
        macd_hist = macd_line - macd_signal

        prev_close = close.shift(1)
        tr = pd.concat([(high-low).abs(), (high-prev_close).abs(), (low-prev_close).abs()], axis=1).max(axis=1)
        atr = tr.ewm(alpha=1.0/V46_ATR_PERIOD, adjust=False, min_periods=V46_ATR_PERIOD).mean()
        atr_pct = (atr / close.abs().where(close.abs() > eps, eps)) * 100.0
        atr_ref = atr_pct.rolling(50, min_periods=15).median()
        atr_ref = atr_ref.where(atr_ref.notna(), atr_pct)

        frame = pd.DataFrame({
            "close": close, "ema_fast": ema_fast, "ema_slow": ema_slow,
            "ema_fast_prev": ema_fast.shift(1), "rsi": rsi,
            "macd": macd_line, "macd_signal": macd_signal,
            "macd_hist": macd_hist, "macd_hist_prev": macd_hist.shift(1),
            "atr_pct": atr_pct, "atr_pct_median": atr_ref,
        }).replace([np.inf, -np.inf], np.nan).dropna()

        if frame.empty:
            return {
                "ready": False, "usable_candles": usable, "required_candles": required,
                "reason": f"Indicators have {usable} clean candles but no complete calculation row yet."
            }

        row = frame.iloc[-1]
        vals = {
            "ready": True, "usable_candles": usable, "required_candles": required,
            "indicator_epoch": int(frame.index[-1].timestamp()) if hasattr(frame.index[-1], "timestamp") else None,
            "close": float(row["close"]), "ema_fast": float(row["ema_fast"]),
            "ema_slow": float(row["ema_slow"]), "ema_fast_prev": float(row["ema_fast_prev"]),
            "rsi": float(row["rsi"]), "macd": float(row["macd"]),
            "macd_signal": float(row["macd_signal"]), "macd_hist": float(row["macd_hist"]),
            "macd_hist_prev": float(row["macd_hist_prev"]), "atr_pct": float(row["atr_pct"]),
            "atr_pct_median": float(row["atr_pct_median"]),
        }
        return vals
    except Exception as exc:
        return {
            "ready": False, "usable_candles": 0, "required_candles": required,
            "reason": f"Indicator calculation failed: {type(exc).__name__}: {exc}"
        }


def _v46_direction_checks(snapshot, signal):
    """Score direction-specific confirmation without pretending it is win probability."""
    up = str(signal).upper() == "CALL"
    if not snapshot.get("ready"):
        return {"ema":False,"rsi":False,"macd":False,"volatility":False,"opposition":False}

    close=float(snapshot["close"]); ef=float(snapshot["ema_fast"]); es=float(snapshot["ema_slow"])
    ef_prev=float(snapshot["ema_fast_prev"]); rsi=float(snapshot["rsi"])
    macd=float(snapshot["macd"]); ms=float(snapshot["macd_signal"]); hist=float(snapshot["macd_hist"])
    hist_prev=float(snapshot["macd_hist_prev"]); atr=float(snapshot["atr_pct"]); atr_med=max(float(snapshot["atr_pct_median"]),1e-12)

    ema_ok = (close >= ef >= es and ef >= ef_prev) if up else (close <= ef <= es and ef <= ef_prev)
    # Momentum confirmation while avoiding chasing an already stretched move.
    # V76 balanced bands: still directional, but less likely to reject a valid
    # setup just because momentum sits a few points outside the old narrow window.
    rsi_ok = (50.0 <= rsi <= 75.0) if up else (25.0 <= rsi <= 50.0)
    macd_ok = (macd >= ms and hist >= 0.0 and hist >= hist_prev*0.70) if up else (macd <= ms and hist <= 0.0 and hist <= hist_prev*0.70)
    # Relative ATR band adapts to each pair. Very dead and extreme-spike regimes are blocked.
    atr_ratio = atr / atr_med
    volatility_ok = 0.45 <= atr_ratio <= 2.10
    ema_opposes = (ef < es and close < ef) if up else (ef > es and close > ef)
    macd_opposes = (macd < ms and hist < 0.0) if up else (macd > ms and hist > 0.0)
    return {"ema":ema_ok,"rsi":rsi_ok,"macd":macd_ok,"volatility":volatility_ok,
            "opposition":bool(ema_opposes and macd_opposes),"atr_ratio":round(atr_ratio,3)}


def _v76_hybrid_indicator_fallback(strategy, tf_df, timeframe, market_name, opts):
    """
    V76 BALANCED fallback for scans where no SK25 pattern produced CALL/PUT.

    It does NOT force a trade. In BALANCED mode only, it may create a direction
    when market health is tradeable, ATR is normal, and multiple directional
    indicators agree. GOOD market requires 2/3 of EMA, RSI, MACD; CAUTION
    requires all 3. BAD market or ambiguous direction remains NO SIGNAL.
    """
    strategy = dict(strategy or {})
    if V78_BALANCED_ACCURACY and not V78_ALLOW_HYBRID_FALLBACK:
        strategy["v76_indicator_fallback"] = {
            "applied": False,
            "disabled_by": "V78_BALANCED_ACCURACY",
            "reason": "Indicator-only fallback is disabled; an exact SK25 setup is required."
        }
        return strategy
    if strategy.get("signal") in {"CALL", "PUT"}:
        return strategy
    if str((opts or {}).get("mode") or "").upper() == "SK25_STRICT":
        return strategy

    health = _balanced_market_health_from_tf(tf_df, timeframe, market_name)
    grade = str(health.get("grade") or "BAD").upper()
    if grade == "BAD":
        strategy["v76_indicator_fallback"] = {"applied": False, "reason": "BAD market health"}
        return strategy

    snap = _v46_indicator_snapshot(tf_df)
    if not snap.get("ready"):
        strategy["v76_indicator_fallback"] = {"applied": False, "reason": snap.get("reason") or "indicator warm-up"}
        return strategy

    call_checks = _v46_direction_checks(snap, "CALL")
    put_checks = _v46_direction_checks(snap, "PUT")

    def directional_count(checks):
        return sum(1 for key in ("ema", "rsi", "macd") if checks.get(key))

    call_n = directional_count(call_checks)
    put_n = directional_count(put_checks)
    need = 2 if grade == "GOOD" else 3

    call_ok = bool(call_checks.get("volatility") and not call_checks.get("opposition") and call_n >= need)
    put_ok = bool(put_checks.get("volatility") and not put_checks.get("opposition") and put_n >= need)

    # Never manufacture a direction if both sides somehow qualify.
    if call_ok == put_ok:
        strategy["v76_indicator_fallback"] = {
            "applied": False, "market_health": grade, "required_directional_checks": need,
            "call_directional_checks": call_n, "put_directional_checks": put_n,
            "reason": "no unique indicator direction",
        }
        return strategy

    signal = "CALL" if call_ok else "PUT"
    direction = "UP" if signal == "CALL" else "DOWN"
    checks = call_checks if call_ok else put_checks
    strength = directional_count(checks)
    strategy.update({
        "signal": signal,
        "pattern_direction": direction,
        "next_candle_color": "GREEN" if signal == "CALL" else "RED",
        "selected_pattern": "V76 Hybrid Indicator Confirmation",
        "pattern_type": 0,
        "setup_match": round(100.0 * strength / 3.0, 1),
        "no_trade": False,
        "signal_release_mode": "HYBRID_INDICATOR_FALLBACK",
        "v76_indicator_fallback": {
            "applied": True, "market_health": grade, "required_directional_checks": need,
            "directional_checks": strength, "checks": checks,
        },
        "reason": (
            f"V76 HYBRID {signal} · no complete SK25 setup, but {strength}/3 directional "
            f"indicators agree in a {grade} market with acceptable volatility. "
            "Normal technical confirmation/opposition gate still applies."
        ),
    })
    return strategy


def _v46_apply_technical_confirmation(strategy, base_df, tf_df, timeframe):
    strategy=dict(strategy or {})
    if not V46_TECH_FILTER_ENABLED or strategy.get("signal") not in {"CALL","PUT"}:
        strategy["v46_technical_filter"]={"enabled":V46_TECH_FILTER_ENABLED,"applied":False}
        return strategy

    signal=str(strategy.get("signal")).upper()
    snap=_v46_indicator_snapshot(tf_df)
    checks=_v46_direction_checks(snap,signal)
    htf=V46_HTF_MAP.get(str(timeframe).lower())
    htf_snap={"ready":False,"reason":"No higher timeframe required for 30m."}
    htf_checks={"ema":True,"macd":True,"opposition":False}
    if htf and htf in TIMEFRAMES:
        htf_df=build_timeframe(base_df,TIMEFRAMES[htf])
        htf_snap=_v46_indicator_snapshot(htf_df)
        htf_checks=_v46_direction_checks(htf_snap,signal)

    # Exact SK25 is mandatory before this function runs and contributes 40 points.
    # The remaining 60 points measure confirmation quality, not expected win rate.
    score=40.0
    score += 15.0 if checks.get("ema") else 0.0
    score += 10.0 if checks.get("rsi") else 0.0
    score += 15.0 if checks.get("macd") else 0.0
    score += 10.0 if checks.get("volatility") else 0.0
    if htf:
        htf_ready=bool(htf_snap.get("ready"))
        htf_ok=bool(htf_ready and htf_checks.get("ema") and htf_checks.get("macd"))
        # V47: higher-timeframe confirmation is valuable, but a short broker
        # history should not permanently suppress otherwise valid 1m setups.
        # Give no HTF bonus until it is ready; do not hard-block on HTF warm-up.
        score += 10.0 if htf_ok else 0.0
    else:
        htf_ready=True
        htf_ok=True
        score += 10.0

    tech={
        "enabled":True,"applied":True,"threshold":V46_MIN_CONFIDENCE,
        "model_confidence":round(score,1),"confidence_is_win_probability":False,
        "timeframe":timeframe,"higher_timeframe":htf,"higher_timeframe_ready":htf_ready,"higher_timeframe_ok":htf_ok,
        "checks":checks,"snapshot":snap,"higher_timeframe_checks":htf_checks,"higher_timeframe_snapshot":htf_snap,
    }
    strategy["v46_technical_filter"]=tech
    strategy["model_confidence"]=round(score,1)
    strategy["calibrated_confidence"]=round(score,1)
    strategy["calibration_status"]="V47 BALANCED TECHNICAL CONFIRMATION"

    hard_opposition=bool(checks.get("opposition") or (htf and htf_ready and htf_checks.get("opposition")))
    warmup_failed=not bool(snap.get("ready"))
    htf_failed=bool(V78_BALANCED_ACCURACY and V78_REQUIRE_HTF and htf and (not htf_ready or not htf_ok))
    volatility_failed=bool(V78_BALANCED_ACCURACY and V78_REQUIRE_VOLATILITY and not checks.get("volatility"))

    # V77 ACCURACY MODE: never release a pattern-only signal while indicators
    # are warming up. Quality takes priority over signal frequency.
    if warmup_failed:
        original=signal
        tech["warmup_bypass"] = False
        tech["confirmation_pending"] = True
        strategy.update({
            "raw_signal_before_v46_filter":original,"signal":"NO SIGNAL","pattern_direction":"NONE","next_candle_color":"NONE",
            "no_trade":True,"v46_technical_blocked":True,"technical_warmup_pending":True,
            "reason":"NO TRADE · V79 signal-flow gate: indicator warm-up incomplete; wait for enough CLOSED candles before releasing a trade signal.",
        })
        return strategy

    if hard_opposition or htf_failed or volatility_failed or score < V46_MIN_CONFIDENCE:
        original=signal
        reasons=[]
        if hard_opposition: reasons.append("EMA + MACD direction conflict")
        if htf_failed:
            reasons.append(f"higher-timeframe {htf} confirmation not ready/aligned")
        if volatility_failed: reasons.append("ATR volatility outside the accepted range")
        if score < V46_MIN_CONFIDENCE: reasons.append(f"confirmation {score:.0f}% < {V46_MIN_CONFIDENCE:.0f}%")
        strategy.update({
            "raw_signal_before_v46_filter":original,"signal":"NO SIGNAL","pattern_direction":"NONE","next_candle_color":"NONE",
            "no_trade":True,"v46_technical_blocked":True,
            "reason":"NO TRADE · V79 signal-flow gate: " + "; ".join(reasons) + ".",
        })
    else:
        strategy["v46_technical_blocked"]=False
        strategy["reason"]=(str(strategy.get("reason") or "").rstrip(".") +
                            f" · V80 confirmed {score:.0f}% (SK25 + EMA/RSI/MACD with soft ATR/HTF defaults).")
    return strategy


def _raja_dukascopy_forming_snapshot(pair, source_info=None):
    """Fetch the bridge's authoritative live BID forming 1m candle for chart display."""
    info = source_info or {}
    if str(info.get("source_mode") or "") != "dukascopy_bridge" or not DUKASCOPY_BRIDGE_URL:
        return None
    bridge_pair = str(info.get("bridge_pair") or pair or "").strip().upper()
    if not bridge_pair:
        return None
    try:
        payload = _dukascopy_bridge_json("/tick", {"pair": bridge_pair})
        f = payload.get("forming_candle") or {}
        vals = {
            "t": int(f.get("t")), "o": float(f.get("o")), "h": float(f.get("h")),
            "l": float(f.get("l")), "c": float(f.get("c")),
            "bid": float(payload.get("bid")), "age_ms": int(payload.get("age_ms") or 0),
            "source": str(payload.get("source") or "Dukascopy JForex BID TICK"),
        }
        if vals["t"] <= 0 or min(vals["o"], vals["h"], vals["l"], vals["c"], vals["bid"]) <= 0:
            return None
        if vals["h"] < max(vals["o"], vals["c"]) or vals["l"] > min(vals["o"], vals["c"]) or vals["h"] < vals["l"]:
            return None
        # Only treat the current UTC minute as forming; never overwrite closed history.
        pd = _get_pandas()
        ts = pd.to_datetime(vals["t"], unit="s", utc=True).floor("min")
        if not _raja_bucket_is_forming(ts, 1):
            return None
        vals["timestamp"] = ts
        return vals
    except Exception:
        return None


def _raja_display_base_with_forming(pair, closed_base_df, source_info=None):
    """Return display-only base data = clean CLOSED rows + authoritative forming tick candle."""
    clean = _raja_clean_ohlc_frame(closed_base_df)
    if clean is None or getattr(clean, "empty", True):
        return clean, None
    snap = _raja_dukascopy_forming_snapshot(pair, source_info)
    if not snap:
        return clean.copy(), None
    pd = _get_pandas()
    out = clean.copy()
    row = pd.DataFrame([{
        "Open": snap["o"], "High": snap["h"], "Low": snap["l"], "Close": snap["c"], "Volume": 0.0
    }], index=pd.DatetimeIndex([snap["timestamp"]]))
    # Display can replace only the same timestamp or append a newer one. Strategy still uses closed_base_df.
    out = pd.concat([out[out.index != snap["timestamp"]], row]).sort_index()
    out = out[~out.index.duplicated(keep="last")]
    return out, snap


def calculate_live_strategy_signal(pair, selected_expiry=None, scan_options=None, bridge_user=None, broker=None, chart_count=32):
    """Single-pair scan. V50 scans the same latest closed OHLC candles shown on the live chart."""
    opts=normalize_scan_options(scan_options)
    tf=str(selected_expiry or "1m").strip().lower()
    if tf not in TIMEFRAMES:
        result=no_signal_result(pair,f"Live source does not provide {tf} closed candles. Type 11 (30s) remains available in Chart Scanner camera mode.")
        result.update({"scan_mode":"SK25_STRICT","selected_expiry":tf,"exclude_from_history":True})
        return result

    broker_key=str(broker or "").strip().casefold().replace(" ","")
    broker_otc="(otc)" in str(pair).casefold() and broker_key in {"quotex","pocketoption","pocket_option","pocket"}
    if pair not in YAHOO_SYMBOLS and not broker_otc:
        return no_signal_result(pair,"Pair is not configured for this market-data source.")

    base_df,data_age,symbol,source_info=get_market_data(pair,bridge_user=bridge_user,broker=broker)
    if base_df is None or base_df.empty:
        result=no_signal_result(pair,source_info.get("unavailable_reason") or "Live market data is unavailable.",symbol=symbol,data_age=data_age,source_info=source_info)
        result.update({"data_delayed":True,"scan_paused":True,"market_status":"UNAVAILABLE","data_status":"UNAVAILABLE","exclude_from_history":True,"scan_skip_reason":"market_data_unavailable"})
        return result
    if data_age is not None and float(data_age)>MAX_SOURCE_CANDLE_AGE_SECONDS:
        age_label=format_market_data_age(data_age)
        result=no_signal_result(pair,f"STALE DATA — latest source candle is {age_label} old. No strategy signal generated.",symbol=symbol,data_age=data_age,source_info=source_info)
        result.update({"data_delayed":True,"source_stale":True,"scan_paused":True,"market_status":"BAD","data_status":"STALE","data_age_label":age_label,"exclude_from_history":True,"scan_skip_reason":"stale_market_data"})
        return result

    # V80: one clean CLOSED provider dataset feeds strategy + indicators.
    # The live Dukascopy forming candle is appended only to the display copy.
    base_df=_raja_clean_ohlc_frame(base_df)
    tf_minutes=TIMEFRAMES[tf]
    tf_df=build_timeframe(base_df,tf_minutes)
    display_base_df,forming_snapshot=_raja_display_base_with_forming(pair,base_df,source_info)
    display_tf_df=build_timeframe_display(display_base_df if display_base_df is not None else base_df,tf_minutes)

    if tf_df is None or tf_df.empty:
        result=no_signal_result(
            pair,
            f"No CLOSED {tf} market candles are available yet.",
            symbol=symbol,
            data_age=data_age,
            source_info=source_info,
        )
        result.update({
            "scan_paused":True,
            "data_status":"WARMING",
            "market_status":"WARMING",
            "exclude_from_history":True,
            "scan_skip_reason":"closed_candle_warming",
        })
        return result

    # Signals scan CLOSED candles only. The chart may additionally display
    # the currently-forming candle so it can be compared with Pocket Option.
    visible_count=max(25,min(300,int(chart_count or 50)))
    chart_scan_df=tf_df.tail(visible_count).copy()
    display_chart_df=(display_tf_df if display_tf_df is not None and not display_tf_df.empty else tf_df).tail(visible_count).copy()
    market_name="OTC" if "(OTC)" in str(pair).upper() else "LIVE"
    last_outcome=_last_strategy_outcome(bridge_user)
    strategy=analyze_sk25_ohlc(chart_scan_df,tf,market_name,last_outcome)
    strategy=_apply_balanced_acceptance(strategy,chart_scan_df,tf,market_name,opts)
    strategy=_apply_strategy_learning(strategy,bridge_user)
    strategy=_apply_adaptive_context(strategy,bridge_user,pair,tf,chart_scan_df)
    strategy=_v76_hybrid_indicator_fallback(strategy,tf_df,tf,market_name,opts)
    strategy=_v46_apply_technical_confirmation(strategy,base_df,tf_df,tf)
    indicator_diag=_v46_indicator_snapshot(tf_df)
    movement=_movement_info(chart_scan_df)
    summary={tf:{
        "signal":strategy.get("signal"),"score":strategy.get("score",0),"pattern_type":strategy.get("pattern_type",0),
        "selected_pattern":strategy.get("selected_pattern"),"setup_match":strategy.get("setup_match",0),
        "next_candle_color":strategy.get("next_candle_color"),"closed_candle_epoch":strategy.get("closed_candle_epoch"),
    }}
    result=dict(strategy)
    result.update({
        "pair":pair,"selected_expiry":tf,"required_expiry_timeframe":tf,"timeframe":tf,
        "timeframe_summary":summary,"timeframes_scanned":[tf],"aligned_timeframes":[tf] if strategy.get("signal") in {"CALL","PUT"} else [],
        "opposing_timeframes":[],"multi_tf_agreement":100.0 if strategy.get("signal") in {"CALL","PUT"} else 0.0,
        "confirmation_mode":"V80 · V79 SIGNAL FLOW + CANDLE SYNC + ROBUST INDICATOR WARM-UP","scan_mode":opts.get("mode","SK25_BALANCED"),"scan_thresholds":opts,
        "accuracy_mode":"V80_CANDLE_SYNC","accuracy_exact_sk25":bool(V78_REQUIRE_EXACT_SK25),"accuracy_hybrid_fallback":bool(V78_ALLOW_HYBRID_FALLBACK),"accuracy_htf_required":bool(V78_REQUIRE_HTF),
        "data_age":round(float(data_age),2) if data_age is not None else None,
        "source":source_info.get("source") or "Broker Native WebSocket","source_mode":source_info.get("source_mode") or ("underlying_proxy" if market_name=="OTC" else "live_reference"),
        "backup_used":bool(source_info.get("backup_used")),"provider_symbol":source_info.get("provider_symbol"),"yahoo_symbol":symbol,
        "feed_quality":source_info.get("feed_quality") or _market_candle_quality(tf_df),"quality_warning":source_info.get("quality_warning"),"source_switch_reason":source_info.get("source_switch_reason"),
        "chart_preview":serialize_candles(display_chart_df,visible_count),"engine":SK25_ENGINE_VERSION,"pattern_library":"RAJA SK25 Pattern Type 1-25","pattern_library_size":SK25_PATTERN_LIBRARY_SIZE,
        "visible_candles_analyzed":True,"pattern_scan_candles":int(len(chart_scan_df)),"requested_chart_candles":visible_count,
        "technical_warmup_candles":int(len(tf_df)),"indicator_usable_candles":int(indicator_diag.get("usable_candles") or 0),"indicator_required_candles":int(indicator_diag.get("required_candles") or 0),"indicator_ready":bool(indicator_diag.get("ready")),"indicator_reason":indicator_diag.get("reason"),
        "chart_signal_sync":"V80 · SIGNAL=CLOSED PROVIDER CANDLES · DISPLAY FORMING=DUKASCOPY BID TICK WHEN AVAILABLE",
        "closed_candle_verified":True,"forming_candle_excluded":True,"chart_includes_forming_candle":bool(forming_snapshot) or bool(len(display_chart_df)>len(tf_df.tail(visible_count))),
        "forming_candle_source":(forming_snapshot or {}).get("source"),"forming_candle_age_ms":(forming_snapshot or {}).get("age_ms"),"base_1m_gap_count":_raja_1m_gap_count(base_df),
        "movement_info":movement,"volatility_pct":movement["percent"],"market_stability_score":100.0 if strategy.get("signal") in {"CALL","PUT"} else 0.0,
        "market_risk_level":"INFO ONLY","market_regime":strategy.get("selected_pattern") or "NO SETUP",
        "deep_quality_score":float(strategy.get("pattern_priority") or 0),"calibration_status":strategy.get("calibration_status") or "V80 CANDLE-SYNC TECHNICAL FILTER", "calibrated_confidence":float(strategy.get("calibrated_confidence") or strategy.get("model_confidence") or 0.0),
        "no_trade":strategy.get("signal") not in {"CALL","PUT"},"quality_gate":"PASSED" if strategy.get("signal") in {"CALL","PUT"} else "PATTERN_ONLY",
    })
    if result["no_trade"]: result["no_trade_reason"]=result.get("reason")
    return result


SIDE_AUTO_SIGNAL_TIMEFRAMES = ("1m", "5m")
SIDE_AUTO_SIGNAL_DEADLINE_SECONDS = max(20.0, min(75.0, float(os.environ.get("RAJA_SIDE_AUTO_DEADLINE_SECONDS", "68"))))



def _raja_side_signal_power(row, user, pair, timeframe):
    """
    UI setup-power score for ranking background candidates.
    This is NOT a win probability. It blends coded strategy priority,
    feed freshness, movement regime, and a conservative history adjustment.
    """
    row = row or {}
    try:
        priority = float(row.get("pattern_priority") or 100.0)
    except Exception:
        priority = 100.0

    # Active priorities are roughly 105..210. Map that into a conservative 60..94 base.
    power = 60.0 + max(0.0, min(1.0, (priority - 100.0) / 110.0)) * 34.0

    try:
        age = float(row.get("data_age") or 0.0)
        if age <= 25:
            power += 3.0
        elif age <= 60:
            power += 1.5
        elif age > 90:
            power -= 4.0
    except Exception:
        pass

    movement = row.get("movement_info") if isinstance(row.get("movement_info"), dict) else {}
    label = str(movement.get("label") or "").upper()
    if label == "NORMAL":
        power += 2.5
    elif label == "HIGH":
        power -= 3.0
    elif label == "LOW":
        power -= 1.5

    perf = {}
    try:
        perf = pair_timeframe_performance(user, pair, timeframe) if user else {}
    except Exception:
        perf = {}

    sample = int(perf.get("sample_size") or 0) if isinstance(perf, dict) else 0
    smoothed = float(perf.get("smoothed_win_rate") or 50.0) if isinstance(perf, dict) else 50.0
    if sample >= 5:
        power += max(-7.0, min(7.0, (smoothed - 50.0) * 0.22))
    if isinstance(perf, dict) and perf.get("hard_block"):
        power -= 12.0
    if row.get("recovery_trade"):
        power -= 5.0

    return round(max(50.0, min(98.0, power)), 1), perf


def calculate_side_auto_signal_candidates(pair, scan_options=None, bridge_user=None, broker=None):
    """Background feed: exact RAJA rules only. Returns ranked 1m/5m candidates with setup power."""
    out=[]
    for tf in ("1m","5m"):
        row=calculate_live_strategy_signal(pair,tf,scan_options,bridge_user,broker)
        if row.get("signal") not in {"CALL","PUT"}:
            continue
        power, perf = _raja_side_signal_power(row, bridge_user, pair, tf)
        out.append({
            "pair":pair,
            "timeframe":tf,
            "signal":row["signal"],
            "direction":"UP" if row["signal"]=="CALL" else "DOWN",
            "trade":"BUY / CALL" if row["signal"]=="CALL" else "SELL / PUT",
            "score":float(row.get("score") or 100.0),
            "power":power,
            "rank_score":float(row.get("pattern_priority") or 0),
            "stability":float(row.get("market_stability_score") or 0.0),
            "risk":"INFO ONLY",
            "volatility_pct":row.get("volatility_pct",0),
            "market_regime":row.get("selected_pattern"),
            "pattern_type":row.get("pattern_type"),
            "selected_pattern":row.get("selected_pattern"),
            "setup_match":float(row.get("setup_match") or 0.0),
            "next_candle_color":row.get("next_candle_color"),
            "rules":row.get("rules") or [],
            "recovery_trade":bool(row.get("recovery_trade")),
            "closed_candle_epoch":row.get("closed_candle_epoch"),
            "data_age_seconds":row.get("data_age"),
            "source":row.get("source"),
            "source_mode":row.get("source_mode"),
            "yahoo_symbol":row.get("yahoo_symbol"),
            "history_sample":int((perf or {}).get("sample_size") or 0),
            "history_smoothed_win_rate":(perf or {}).get("smoothed_win_rate"),
            "performance_status":(perf or {}).get("status") or "LEARNING",
            "power_is_probability":False,
        })
    return out


def calculate_forex_otc_fallback_snapshot(pair, selected_expiry=None):
    """Reference-only diagnostic; never upgrades a proxy OTC candle into an exact broker signal."""
    tf=str(selected_expiry or "1m").strip().lower()
    if tf not in TIMEFRAMES: tf="1m"
    base_df,data_age,symbol,source_info=get_market_data(pair)
    if base_df is None or base_df.empty:
        return {"pair":pair,"available":False,"live_fresh":False,"reason":"No reference history available.","source":source_info.get("source") or "Broker Native WebSocket","source_mode":"fallback_reference_only"}
    tf_df=build_timeframe(base_df,TIMEFRAMES[tf])
    watch=analyze_sk25_ohlc(tf_df,tf,"OTC","")
    return {
        "pair":pair,"available":True,"live_fresh":bool(float(data_age or 0)<=MAX_SOURCE_CANDLE_AGE_SECONDS),
        "normal_scan_required":True,"reference_stale":bool(float(data_age or 0)>MAX_SOURCE_CANDLE_AGE_SECONDS),
        "source":source_info.get("source") or "Broker Native WebSocket","source_mode":"fallback_reference_only","provider_symbol":source_info.get("provider_symbol"),"yahoo_symbol":symbol,
        "data_age_seconds":round(float(data_age),2) if data_age is not None else None,"data_age_label":format_market_data_age(data_age),
        "selected_expiry":tf,"strategy_watch":watch,"signal":"NO SIGNAL","reason":"Reference-only OTC data is never used for a trading signal.",
        "chart_preview":serialize_candles(tf_df,40),
    }


# =========================================================
# LIVE ECONOMIC NEWS / CALENDAR
# =========================================================

def fetch_forex_factory_calendar():
    """Fetch and normalize Forex Factory's official weekly JSON calendar export."""
    req = UrlRequest(
        FOREX_FACTORY_CALENDAR_URL,
        headers={
            "User-Agent": "RAJA-AI-PREMIUM/2.5 (+economic-calendar)",
            "Accept": "application/json",
        },
    )
    with urlopen(req, timeout=6) as response:
        payload = response.read().decode("utf-8", errors="replace")

    raw_items = json.loads(payload)
    if not isinstance(raw_items, list):
        raise ValueError("Unexpected Forex Factory calendar response")

    normalized = []
    for item in raw_items[:300]:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title", "")).strip()
        country = str(item.get("country", "")).strip().upper()
        date_value = str(item.get("date", "")).strip()
        if not title or not date_value:
            continue
        impact = str(item.get("impact", "Low")).strip().title() or "Low"
        if impact not in {"High", "Medium", "Low", "Holiday"}:
            impact = "Low"
        normalized.append({
            "title": title[:180],
            "currency": country[:8],
            "date": date_value[:40],
            "impact": impact,
            "forecast": str(item.get("forecast", "")).strip()[:40],
            "previous": str(item.get("previous", "")).strip()[:40],
        })

    normalized.sort(key=lambda x: x.get("date", ""))
    return normalized


def _calendar_for_safety_lock():
    now = time.time()
    with market_news_lock:
        cached_data = list(market_news_cache.get("data") or [])
        cached_at = float(market_news_cache.get("timestamp") or 0.0)

    if cached_data and (now - cached_at) <= MARKET_NEWS_CACHE_SECONDS:
        return cached_data

    try:
        fresh = fetch_forex_factory_calendar()
        with market_news_lock:
            market_news_cache["timestamp"] = time.time()
            market_news_cache["data"] = fresh
        return fresh
    except Exception as exc:
        print(f"News safety calendar warning: {exc}")
        return cached_data


def _forex_pair_currencies(pair):
    clean = str(pair or "").replace(" (OTC)", "").strip().upper()
    if "/" in clean:
        left, right = clean.split("/", 1)
        out = {x for x in (left.strip(), right.strip()) if len(x.strip()) == 3}
        return out
    if clean == "XAUUSD":
        return {"USD"}
    return set()


def evaluate_news_safety_lock(pairs, market, before_minutes=15, after_minutes=15):
    # User requested this safety lock for Forex markets only.
    if "FOREX" not in str(market or "").upper():
        return None

    currencies = set()
    for pair in pairs or []:
        currencies.update(_forex_pair_currencies(pair))
    if not currencies:
        return None

    now_ts = time.time()
    candidates = []
    for item in _calendar_for_safety_lock():
        if str(item.get("impact") or "").title() != "High":
            continue
        currency = str(item.get("currency") or "").upper()
        if currency not in currencies:
            continue
        raw_date = str(item.get("date") or "").strip()
        if not raw_date:
            continue
        try:
            parsed = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            event_ts = parsed.timestamp()
        except Exception:
            continue

        if now_ts - (after_minutes * 60) <= event_ts <= now_ts + (before_minutes * 60):
            candidates.append((abs(event_ts - now_ts), event_ts, item))

    if not candidates:
        return None

    _, event_ts, event = min(candidates, key=lambda row: row[0])
    seconds_to_event = int(event_ts - now_ts)
    if seconds_to_event >= 0:
        timing = f"in about {max(1, round(seconds_to_event / 60))} minute(s)"
    else:
        timing = f"about {max(1, round(abs(seconds_to_event) / 60))} minute(s) ago"

    return {
        "active": True,
        "currency": str(event.get("currency") or ""),
        "title": str(event.get("title") or "High-impact economic event"),
        "date": str(event.get("date") or ""),
        "impact": "High",
        "window_before_minutes": int(before_minutes),
        "window_after_minutes": int(after_minutes),
        "reason": (
            f"News Safety Lock: {event.get('currency','')} high-impact event "
            f"“{event.get('title','Economic event')}” is {timing}. "
            f"Forex scans are paused inside the {before_minutes}-minute before / "
            f"{after_minutes}-minute after safety window."
        ),
    }


def news_locked_no_signal(pair, news_lock):
    reason = str((news_lock or {}).get("reason") or "High-impact news safety lock is active.")
    result = no_signal_result(pair, reason, symbol=YAHOO_SYMBOLS.get(pair))
    result.update({
        "news_safety_lock": news_lock,
        "no_trade": True,
        "no_trade_reason": reason,
        "quality_gate": "NEWS_LOCK",
    })
    return result


# =========================================================
# ROUTES
# =========================================================

@app.route("/")
@app.route("/index.html")
def home():
    index_path = BASE_DIR / "index.html"
    if index_path.exists():
        try:
            html = index_path.read_text(encoding="utf-8")
            html = html.replace(APP_BUILD_TOKEN, APP_BUILD_ID)
            response = app.response_class(html, mimetype="text/html")
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
            return response
        except Exception:
            # Safe fallback if the template token cannot be injected for any reason.
            return send_from_directory(str(BASE_DIR), "index.html")
    return (
        "RAJA AI backend is running. "
        "Place index.html in the same folder as bot.py."
    )


@app.route("/app-version", methods=["GET"])
def app_version():
    response = jsonify({
        "status": "success",
        "build": APP_BUILD_ID,
        "update_policy": "auto",
        "server_epoch_ms": int(time.time() * 1000),
    })
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/manifest.json", methods=["GET"])
def pwa_manifest():
    # Serve a build-versioned manifest so Android/Chrome sees changed icon URLs
    # after every real deployment. Keep app id/start_url untouched so this updates
    # the existing installation instead of creating a second app.
    manifest_path = BASE_DIR / "manifest.json"
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))

        def _version_icons(rows):
            if not isinstance(rows, list):
                return
            for icon in rows:
                if not isinstance(icon, dict):
                    continue
                src = str(icon.get("src") or "").strip()
                if not src:
                    continue
                base = src.split("?", 1)[0]
                icon["src"] = f"{base}?v={APP_BUILD_ID}"

        _version_icons(payload.get("icons"))
        for shortcut in payload.get("shortcuts") or []:
            if isinstance(shortcut, dict):
                _version_icons(shortcut.get("icons"))

        response = jsonify(payload)
        response.mimetype = "application/manifest+json"
    except Exception:
        response = send_from_directory(str(BASE_DIR), "manifest.json", mimetype="application/manifest+json")

    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/sw.js", methods=["GET"])
def pwa_service_worker():
    response = send_from_directory(str(BASE_DIR), "sw.js", mimetype="application/javascript")
    response.headers["Service-Worker-Allowed"] = "/"
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/raja-ai-icon-<size>.png", methods=["GET"])
@app.route("/raja-ai-icon-<size>-v2.png", methods=["GET"])
@app.route("/raja-ai-icon-<size>-v3.png", methods=["GET"])
def pwa_icon(size):
    # Accept current/future versioned icon filenames (for example 192-v3)
    # while validating the actual icon size.
    raw_size = str(size or "").strip()
    base_size = raw_size.split("-", 1)[0]
    if base_size not in {"192", "512"}:
        return jsonify({"status": "error", "message": "Icon not found."}), 404
    requested = request.path.rsplit("/", 1)[-1]
    candidate = BASE_DIR / requested
    if not candidate.exists():
        requested = f"raja-ai-icon-{base_size}.png"
    response = send_from_directory(str(BASE_DIR), requested, mimetype="image/png")
    # PWA icon identity is versioned by filename/build and must never be pinned by HTTP cache.
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/raja-splash-logo.png", methods=["GET"])
def pwa_splash_logo():
    response = send_from_directory(str(BASE_DIR), "raja-splash-logo.png", mimetype="image/png")
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.after_request
def disable_html_cache(response):
    # Fresh app shell/update metadata must never get pinned by Safari/Chrome HTTP cache.
    # User data/localStorage is untouched; only network cache policy is controlled here.
    no_store_paths = {"/", "/index.html", "/chart-scanner", "/live-scanner", "/sw.js", "/app-version"}
    if request.path in no_store_paths or request.path.endswith(".html"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    elif request.path == "/manifest.json":
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return response


@app.route("/otc-fallback-config", methods=["GET"])
def otc_fallback_config():
    auth, error = _auth_session(request.args)
    if error:
        return error

    companion_url = RAJA_QUOTEX_OTC_COMPANION_URL
    bridge = _get_quotex_bridge_status(auth["user"])
    bridge_connected = bool(bridge.get("connected"))
    pairs_with_data = int(bridge.get("pairs_with_data") or 0)
    native = native_feed_status()
    qx_native = dict(native.get("quotex") or {})
    po_native = dict(native.get("pocket_option") or {})
    qx_native_ready = bool(qx_native.get("configured") and qx_native.get("connected"))
    po_native_ready = bool(po_native.get("configured") and po_native.get("connected"))
    # No-bridge reference mode is always scan-capable when the normal
    # Yahoo/Twelve reference chain is available. Native broker data remains optional.
    direct_ready = True
    if qx_native_ready:
        message = "Exact Quotex native feed is connected. It will be preferred automatically."
    else:
        message = "No-bridge OTC reference mode is active. Yahoo/Twelve reference candles will be used when exact broker OTC data is unavailable."
    return jsonify({
        "status": "success",
        "data": {
            "enabled": bool(RAJA_QUOTEX_OTC_URL),
            "market": "ForexOTC",
            "launch_url": RAJA_QUOTEX_OTC_URL,
            "companion_connected": bridge_connected or bool(companion_url),
            "companion_url": companion_url,
            "direct_scan_available": direct_ready,
            "native_primary": True,
            "native_feeds": native,
            "quotex_native_ready": qx_native_ready,
            "pocket_native_ready": po_native_ready,
            "bridge": bridge,
            "bridge_required_for_quotex_otc": False,
            "bridge_is_backup": True,
            "reference_fallback_enabled": True,
            "strict_broker_otc": False,
            "message": message,
        },
    })


@app.route("/broker-bridge/pair-code", methods=["POST"])
@app.route("/quotex-bridge/pair-code", methods=["POST"])
def quotex_bridge_pair_code():
    data = request.get_json(silent=True) or {}
    auth, error = _auth_session(data)
    if error:
        return error
    now = time.time()
    with quotex_bridge_pair_codes_lock:
        for old_code, row in list(quotex_bridge_pair_codes.items()):
            if float(row.get("expires_at") or 0) <= now:
                quotex_bridge_pair_codes.pop(old_code, None)
        code = None
        for _ in range(20):
            candidate = f"{secrets.randbelow(900000) + 100000:06d}"
            if candidate not in quotex_bridge_pair_codes:
                code = candidate
                break
        if not code:
            return jsonify({"status": "error", "message": "Could not create a bridge pairing code. Retry."}), 503
        quotex_bridge_pair_codes[code] = {
            "user": auth["user"],
            "device": auth["device"],
            "expires_at": now + QUOTEX_BRIDGE_PAIR_CODE_TTL_SECONDS,
        }
    return jsonify({
        "status": "success",
        "data": {
            "pair_code": code,
            "expires_in_seconds": QUOTEX_BRIDGE_PAIR_CODE_TTL_SECONDS,
            "server_url": request.host_url.rstrip("/"),
            "instructions": "Open the RAJA Broker Bridge extension and enter this code once."
        }
    })


@app.route("/broker-bridge/activate", methods=["POST"])
@app.route("/quotex-bridge/activate", methods=["POST"])
def quotex_bridge_activate():
    data = request.get_json(silent=True) or {}
    code = str(data.get("pair_code") or "").strip()
    if not code:
        return jsonify({"status": "error", "message": "Pairing code is required."}), 400
    now = time.time()
    with quotex_bridge_pair_codes_lock:
        row = quotex_bridge_pair_codes.pop(code, None)
    if not row or float(row.get("expires_at") or 0) <= now:
        return jsonify({"status": "error", "message": "Pairing code is invalid or expired. Generate a new code in RAJA AI."}), 401
    token = _issue_quotex_bridge_token(row["user"], row["device"])
    return jsonify({
        "status": "success",
        "data": {
            "bridge_token": token,
            "expires_at": int(now) + QUOTEX_BRIDGE_TOKEN_TTL_SECONDS,
            "user": row["user"],
        }
    })


@app.route("/broker-bridge/status", methods=["POST"])
@app.route("/quotex-bridge/status", methods=["POST"])
def quotex_bridge_status_route():
    data = request.get_json(silent=True) or {}
    auth, error = _auth_session(data)
    if error:
        return error
    pair = str(data.get("pair") or "").strip()
    if pair and pair not in YAHOO_SYMBOLS:
        pair = ""
    broker = str(data.get("broker") or ("Quotex" if request.path.startswith("/quotex-bridge/") else "Quotex")).strip().casefold().replace(" ", "")
    if broker in {"pocketoption", "pocket_option", "pocket"}:
        status = _get_pocket_bridge_status(auth["user"], pair or None)
    else:
        status = _get_quotex_bridge_status(auth["user"], pair or None)
        status.setdefault("broker", "Quotex")
    return jsonify({"status": "success", "data": status})


@app.route("/broker-bridge/tick", methods=["POST"])
@app.route("/quotex-bridge/tick", methods=["POST"])
def quotex_bridge_tick():
    data = request.get_json(silent=True) or {}
    token = request.headers.get("X-RAJA-Bridge-Token") or data.get("bridge_token")
    bridge_auth = _validate_quotex_bridge_token(token)
    if not bridge_auth:
        return jsonify({"status": "error", "message": "Bridge token is invalid or expired. Pair the extension again."}), 401
    pair = str(data.get("pair") or "").strip()
    if pair not in YAHOO_SYMBOLS or "(OTC)" not in pair:
        return jsonify({"status": "error", "message": "Unsupported or non-OTC bridge pair."}), 400

    broker = str(data.get("broker") or ("Quotex" if request.path.startswith("/quotex-bridge/") else "")).strip().casefold().replace(" ", "")
    is_pocket = broker in {"pocketoption", "pocket_option", "pocket"}
    if not is_pocket and broker not in {"quotex", ""}:
        return jsonify({"status": "error", "message": "Unsupported broker bridge."}), 400

    accepted = 0
    candles = data.get("candles")
    if isinstance(candles, list):
        for candle in candles[-500:]:
            if is_pocket:
                accepted += int(_pocket_bridge_upsert_candle(bridge_auth["user"], pair, candle))
            else:
                accepted += int(_bridge_upsert_candle(bridge_auth["user"], pair, candle))

    price = data.get("price")
    epoch = data.get("timestamp")
    tick_ok = False
    if price is not None:
        if is_pocket:
            tick_ok = _pocket_bridge_upsert_tick(bridge_auth["user"], pair, price, epoch)
        else:
            tick_ok = _bridge_upsert_tick(bridge_auth["user"], pair, price, epoch)
        accepted += int(tick_ok)

    if not accepted:
        label = "Pocket Option" if is_pocket else "Quotex"
        return jsonify({"status": "error", "message": f"No valid {label} price/candle data was supplied."}), 400

    if is_pocket:
        _set_pocket_bridge_status(
            bridge_auth["user"], bridge_auth["device"], pair=pair,
            price=price if tick_ok else None, source_page=data.get("source_page")
        )
        status = _get_pocket_bridge_status(bridge_auth["user"], pair)
    else:
        _set_quotex_bridge_status(
            bridge_auth["user"], bridge_auth["device"], pair=pair,
            price=price if tick_ok else None, source_page=data.get("source_page")
        )
        status = _get_quotex_bridge_status(bridge_auth["user"], pair)
        status.setdefault("broker", "Quotex")
    return jsonify({"status": "success", "data": {"accepted": accepted, **status}})


@app.route("/broker-bridge/ready-pairs", methods=["POST"])
def broker_bridge_ready_pairs_route():
    data = request.get_json(silent=True) or {}
    auth, error = _auth_session(data)
    if error:
        return error
    broker = str(data.get("broker") or "").strip()
    broker_key = broker.casefold().replace(" ", "")
    if broker_key not in {"quotex", "pocketoption", "pocket_option", "pocket"}:
        return jsonify({"status": "error", "message": "Broker bridge status is available only for Quotex or Pocket Option."}), 400
    requested = data.get("pairs") if isinstance(data.get("pairs"), list) else None
    opts = normalize_scan_options(data.get("scan_options"))
    required_candles = _bridge_required_base_candles(opts.get("min_tf"))
    rows = _broker_bridge_ready_pairs(auth["user"], broker, requested)
    for row in rows:
        row["mode_required_candles"] = required_candles
        row["mode_scan_ready"] = bool(row.get("stream_fresh") and row.get("market_fresh") and int(row.get("candle_count") or 0) >= required_candles)
    ready = [row for row in rows if row.get("mode_scan_ready")]
    try:
        native_state = native_feed_status() if callable(native_feed_status) else {}
        native_row = (native_state or {}).get("pocket_option" if broker_key in {"pocketoption", "pocket_option", "pocket"} else "quotex") or {}
    except Exception:
        native_row = {}
    return jsonify({
        "status": "success",
        "data": {
            "broker": "Pocket Option" if broker_key in {"pocketoption", "pocket_option", "pocket"} else "Quotex",
            "pairs": rows,
            "ready_pairs": [row["pair"] for row in ready],
            "ready_count": len(ready),
            "cached_count": len(rows),
            "native_configured": bool(native_row.get("enabled") and native_row.get("configured")),
            "native_connected": bool(native_row.get("connected")),
            "pair_fresh_seconds": BROKER_BRIDGE_PAIR_FRESH_SECONDS,
            "market_max_age_seconds": BROKER_BRIDGE_MARKET_MAX_AGE_SECONDS,
            "scan_min_candles": BROKER_BRIDGE_SCAN_MIN_CANDLES,
            "mode": opts.get("mode"),
            "min_tf": opts.get("min_tf"),
            "mode_required_candles": required_candles,
        },
    })



# =========================================================
# V59 FINNHUB / OANDA LIVE CHART TEST
# - Subscribes normal Forex pairs to Finnhub OANDA WebSocket.
# - Builds live forming candles from Finnhub ticks.
# - Uses existing Yahoo closed history only as an immediate chart seed
#   because Finnhub historical forex candles may require a paid plan.
# - RAJA signal engine is NOT switched in this test build.
# =========================================================
FINNHUB_API_KEY = str(os.getenv("FINNHUB_API_KEY") or "").strip()

FINNHUB_FOREX_SYMBOLS = {
    "EUR/USD": "OANDA:EUR_USD",
    "GBP/USD": "OANDA:GBP_USD",
    "USD/JPY": "OANDA:USD_JPY",
    "AUD/USD": "OANDA:AUD_USD",
    "USD/CAD": "OANDA:USD_CAD",
    "USD/CHF": "OANDA:USD_CHF",
    "NZD/USD": "OANDA:NZD_USD",
    "EUR/GBP": "OANDA:EUR_GBP",
    "EUR/JPY": "OANDA:EUR_JPY",
    "GBP/JPY": "OANDA:GBP_JPY",
    "AUD/JPY": "OANDA:AUD_JPY",
    "EUR/AUD": "OANDA:EUR_AUD",
    "GBP/AUD": "OANDA:GBP_AUD",
    "CAD/JPY": "OANDA:CAD_JPY",
    "EUR/CAD": "OANDA:EUR_CAD",
    "GBP/CAD": "OANDA:GBP_CAD",
    "NZD/JPY": "OANDA:NZD_JPY",
    "AUD/NZD": "OANDA:AUD_NZD",
    "EUR/CHF": "OANDA:EUR_CHF",
    "GBP/CHF": "OANDA:GBP_CHF",
    "XAUUSD": "OANDA:XAU_USD",
}
FINNHUB_SYMBOL_TO_PAIR = {v: k for k, v in FINNHUB_FOREX_SYMBOLS.items()}
FINNHUB_BUCKET_MINUTES = (1, 2, 5, 10, 15, 30)

_finnhub_live_lock = threading.Lock()
_finnhub_live_thread = None
_finnhub_live_connected = False
_finnhub_live_error = None
_finnhub_live_started_at = None

# =========================================================
# V66 PERSISTENT FINNHUB FOREX CANDLE STORE
# =========================================================
FINNHUB_STORE_FILE = DATA_DIR / "finnhub_forex_candles.json"
_finnhub_store_lock = threading.RLock()
_finnhub_store_ready = False

def _finnhub_store_init():
    """Create PostgreSQL candle table when DATABASE_URL is available."""
    global _finnhub_store_ready
    if _finnhub_store_ready:
        return
    with _finnhub_store_lock:
        if _finnhub_store_ready:
            return
        if DATABASE_URL and psycopg is not None:
            try:
                with psycopg.connect(DATABASE_URL) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            CREATE TABLE IF NOT EXISTS raja_finnhub_candles (
                                pair TEXT NOT NULL,
                                t BIGINT NOT NULL,
                                o DOUBLE PRECISION NOT NULL,
                                h DOUBLE PRECISION NOT NULL,
                                l DOUBLE PRECISION NOT NULL,
                                c DOUBLE PRECISION NOT NULL,
                                created_at TIMESTAMPTZ DEFAULT NOW(),
                                PRIMARY KEY (pair, t)
                            )
                        """)
                    conn.commit()
            except Exception:
                pass
        _finnhub_store_ready = True

def _finnhub_store_save_candle(pair, candle):
    """Persist one completed Finnhub 1m candle."""
    try:
        t = int(candle["t"])
        o = float(candle["o"])
        h = float(candle["h"])
        l = float(candle["l"])
        c = float(candle["c"])
    except Exception:
        return

    _finnhub_store_init()

    if DATABASE_URL and psycopg is not None:
        try:
            with psycopg.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO raja_finnhub_candles(pair,t,o,h,l,c)
                        VALUES (%s,%s,%s,%s,%s,%s)
                        ON CONFLICT(pair,t) DO UPDATE SET
                            o=EXCLUDED.o,h=EXCLUDED.h,l=EXCLUDED.l,c=EXCLUDED.c
                    """, (pair, t, o, h, l, c))
                conn.commit()
            return
        except Exception:
            pass

    # File fallback for local/testing. Railway ephemeral disks can reset on deploy.
    try:
        with _finnhub_store_lock:
            payload = {}
            if FINNHUB_STORE_FILE.exists():
                try:
                    payload = json.loads(FINNHUB_STORE_FILE.read_text(encoding="utf-8"))
                except Exception:
                    payload = {}
            rows = list(payload.get(pair) or [])
            by_t = {int(r["t"]): r for r in rows if isinstance(r, dict) and "t" in r}
            by_t[t] = {"t":t,"o":o,"h":h,"l":l,"c":c}
            payload[pair] = [by_t[k] for k in sorted(by_t)[-2500:]]
            FINNHUB_STORE_FILE.write_text(json.dumps(payload), encoding="utf-8")
    except Exception:
        pass

def _finnhub_store_load(pair, limit=1200):
    """Load persisted completed Finnhub candles for immediate chart display."""
    _finnhub_store_init()

    rows = []
    if DATABASE_URL and psycopg is not None:
        try:
            with psycopg.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT t,o,h,l,c
                        FROM raja_finnhub_candles
                        WHERE pair=%s
                        ORDER BY t DESC
                        LIMIT %s
                    """, (pair, int(limit)))
                    data = cur.fetchall()
            for t,o,h,l,c in reversed(data):
                rows.append({"t":int(t),"o":float(o),"h":float(h),"l":float(l),"c":float(c)})
            return rows
        except Exception:
            pass

    try:
        if FINNHUB_STORE_FILE.exists():
            payload = json.loads(FINNHUB_STORE_FILE.read_text(encoding="utf-8"))
            data = list(payload.get(pair) or [])
            return data[-int(limit):]
    except Exception:
        pass
    return rows

def _finnhub_store_load_into_memory(pair):
    """Prime in-memory candle book from persistent storage."""
    rows = _finnhub_store_load(pair, limit=2500)
    if not rows:
        return 0
    with _finnhub_live_lock:
        state = _finnhub_live_state.get(pair)
        if state is None:
            return 0
        closed = state.setdefault("closed_1m", OrderedDict())
        for r in rows:
            try:
                t = int(r["t"])
                closed[t] = {
                    "Open": float(r["o"]),
                    "High": float(r["h"]),
                    "Low": float(r["l"]),
                    "Close": float(r["c"]),
                    "Volume": 0.0,
                }
            except Exception:
                continue
        while len(closed) > 2500:
            closed.popitem(last=False)
    return len(rows)

_finnhub_live_state = {
    pair: {
        "symbol": symbol,
        "price": None,
        "last_tick_ms": None,
        "message_count": 0,
        "forming": {},
        "closed_1m": OrderedDict(),
    }
    for pair, symbol in FINNHUB_FOREX_SYMBOLS.items()
}

def _finnhub_bucket_epoch(tick_ms, minutes):
    seconds = int(tick_ms // 1000)
    span = max(1, int(minutes)) * 60
    return seconds - (seconds % span)

def _finnhub_update_tick(pair, price, tick_ms):
    """Update forming candles and persist completed Finnhub 1m bars in memory."""
    with _finnhub_live_lock:
        state = _finnhub_live_state.get(pair)
        if state is None:
            return
        state["price"] = float(price)
        state["last_tick_ms"] = int(tick_ms)
        state["message_count"] = int(state.get("message_count") or 0) + 1

        for minutes in FINNHUB_BUCKET_MINUTES:
            bucket = _finnhub_bucket_epoch(tick_ms, minutes)
            key = str(minutes)
            c = state["forming"].get(key)

            # Persist the completed previous 1m candle before starting the new one.
            if minutes == 1 and isinstance(c, dict) and int(c.get("t") or -1) != bucket:
                try:
                    closed = state.setdefault("closed_1m", OrderedDict())
                    prev_t = int(c["t"])
                    closed[prev_t] = {
                        "Open": float(c["o"]),
                        "High": float(c["h"]),
                        "Low": float(c["l"]),
                        "Close": float(c["c"]),
                        "Volume": 0.0,
                    }
                    _finnhub_store_save_candle(pair, {
                        "t": prev_t,
                        "o": float(c["o"]),
                        "h": float(c["h"]),
                        "l": float(c["l"]),
                        "c": float(c["c"]),
                    })
                    closed.move_to_end(prev_t)
                    while len(closed) > 2500:
                        closed.popitem(last=False)
                except Exception:
                    pass

            if not isinstance(c, dict) or int(c.get("t") or -1) != bucket:
                state["forming"][key] = {
                    "t": bucket,
                    "o": float(price),
                    "h": float(price),
                    "l": float(price),
                    "c": float(price),
                }
            else:
                c["h"] = max(float(c.get("h", price)), float(price))
                c["l"] = min(float(c.get("l", price)), float(price))
                c["c"] = float(price)

def _finnhub_live_snapshot(pair=None):
    with _finnhub_live_lock:
        connected = bool(_finnhub_live_connected)
        error = _finnhub_live_error
        started = _finnhub_live_started_at
        if pair:
            raw = dict(_finnhub_live_state.get(pair) or {})
            raw["forming"] = dict(raw.get("forming") or {})
        else:
            raw = {}
    tick_ms = raw.get("last_tick_ms")
    age = None
    if tick_ms:
        try:
            age = max(0.0, time.time() - float(tick_ms) / 1000.0)
        except Exception:
            pass
    return {
        "configured": bool(FINNHUB_API_KEY),
        "dependency_ok": _finnhub_ws is not None,
        "connected": connected,
        "started_at": started,
        "last_error": error,
        "pair": pair,
        "symbol": raw.get("symbol"),
        "price": raw.get("price"),
        "last_tick_ms": tick_ms,
        "age_seconds": age,
        "message_count": int(raw.get("message_count") or 0),
        "forming": raw.get("forming") or {},
        "status": (
            "LIVE" if connected and raw.get("price") is not None
            else "CONNECTED_NO_TICKS" if connected
            else "NOT_CONFIGURED" if not FINNHUB_API_KEY
            else "CONNECTING"
        ),
    }

def _run_finnhub_live_stream():
    global _finnhub_live_connected, _finnhub_live_error, _finnhub_live_started_at
    if not FINNHUB_API_KEY or _finnhub_ws is None:
        with _finnhub_live_lock:
            _finnhub_live_error = (
                "FINNHUB_API_KEY is not configured."
                if not FINNHUB_API_KEY
                else f"websocket-client unavailable: {_finnhub_ws_import_error_text}"
            )
        return

    url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"

    def on_open(ws):
        global _finnhub_live_connected, _finnhub_live_error
        with _finnhub_live_lock:
            _finnhub_live_connected = True
            _finnhub_live_error = None
        # Subscribe all normal FX pairs. Finnhub can silently omit symbols not
        # available on the user's plan; each pair status remains visible.
        for symbol in FINNHUB_FOREX_SYMBOLS.values():
            try:
                ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
                time.sleep(0.02)
            except Exception:
                pass

    def on_message(ws, message):
        global _finnhub_live_error
        try:
            payload = json.loads(message)
        except Exception:
            return
        if payload.get("type") == "error":
            with _finnhub_live_lock:
                _finnhub_live_error = str(payload.get("msg") or payload)
            return
        rows = payload.get("data") or []
        if not isinstance(rows, list):
            return
        for row in rows:
            symbol = str(row.get("s") or "")
            pair = FINNHUB_SYMBOL_TO_PAIR.get(symbol)
            if not pair:
                continue
            try:
                price = float(row.get("p"))
                tick_ms = int(row.get("t") or int(time.time() * 1000))
            except Exception:
                continue
            _finnhub_update_tick(pair, price, tick_ms)

    def on_error(ws, error):
        global _finnhub_live_connected, _finnhub_live_error
        with _finnhub_live_lock:
            _finnhub_live_connected = False
            _finnhub_live_error = f"{type(error).__name__}: {error}"

    def on_close(ws, code, msg):
        global _finnhub_live_connected, _finnhub_live_error
        with _finnhub_live_lock:
            _finnhub_live_connected = False
            if code or msg:
                _finnhub_live_error = f"WebSocket closed {code or ''} {msg or ''}".strip()

    try:
        with _finnhub_live_lock:
            _finnhub_live_started_at = datetime.now(timezone.utc).isoformat()
        app_ws = _finnhub_ws.WebSocketApp(
            url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        app_ws.run_forever(ping_interval=20, ping_timeout=10)
    except Exception as exc:
        with _finnhub_live_lock:
            _finnhub_live_connected = False
            _finnhub_live_error = f"{type(exc).__name__}: {exc}"

def _ensure_finnhub_live_stream():
    global _finnhub_live_thread
    if not FINNHUB_API_KEY or _finnhub_ws is None:
        return
    if _finnhub_live_thread is not None and _finnhub_live_thread.is_alive():
        return
    _finnhub_live_thread = threading.Thread(
        target=_run_finnhub_live_stream,
        name="finnhub-oanda-multipair-live",
        daemon=True,
    )
    _finnhub_live_thread.start()

def _finnhub_tf_minutes(tf):
    key = str(tf or "1m").strip().lower()
    return {
        "1m": 1, "2m": 2, "5m": 5, "10m": 10, "15m": 15, "30m": 30
    }.get(key, 1)

def _finnhub_seed_history(pair, tf, count):
    """Immediate closed chart seed using the project's existing Yahoo 1m history."""
    try:
        base_df, age, symbol, info = _fetch_yahoo_1m(pair, count=max(300, int(count) * 35))
    except Exception as exc:
        return [], None, f"Yahoo seed error: {type(exc).__name__}: {exc}"
    if base_df is None or getattr(base_df, "empty", True):
        return [], age, "Yahoo history seed unavailable."
    try:
        minutes = _finnhub_tf_minutes(tf)
        tf_df = build_timeframe(base_df, minutes)
        if tf_df is None or getattr(tf_df, "empty", True):
            return [], age, "History timeframe seed unavailable."
        return serialize_candles(tf_df, int(count)), age, None
    except Exception as exc:
        return [], age, f"History seed conversion error: {type(exc).__name__}: {exc}"


_FINNHUB_HISTORY_CACHE = {}
_FINNHUB_HISTORY_CACHE_LOCK = threading.RLock()
_FINNHUB_HISTORY_CACHE_SECONDS = 120

def _finnhub_rest_history_1m(pair, count=1600):
    """Try Finnhub Forex candles first. Returns None when the plan has no candle access."""
    symbol = FINNHUB_FOREX_SYMBOLS.get(str(pair or "").strip())
    if not symbol or not FINNHUB_API_KEY:
        return None, "Finnhub history unavailable"
    now = time.time()
    key = (symbol, int(count))
    with _FINNHUB_HISTORY_CACHE_LOCK:
        cached = _FINNHUB_HISTORY_CACHE.get(key)
        if cached and now - float(cached.get("ts") or 0) <= _FINNHUB_HISTORY_CACHE_SECONDS:
            return cached["df"].copy(), cached.get("note")

    try:
        # Request enough wall-clock history to cover 1m trading gaps/weekends.
        to_epoch = int(now)
        from_epoch = to_epoch - max(3 * 24 * 3600, int(count) * 120)
        params = urlencode({
            "symbol": symbol,
            "resolution": "1",
            "from": from_epoch,
            "to": to_epoch,
            "token": FINNHUB_API_KEY,
        })
        req = UrlRequest(
            "https://finnhub.io/api/v1/forex/candle?" + params,
            headers={"Accept": "application/json", "User-Agent": "RAJA-AI-FINNHUB/60"},
            method="GET",
        )
        with urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if str(payload.get("s") or "").lower() != "ok":
            return None, str(payload.get("error") or payload.get("s") or "Finnhub candle history not available on this plan")

        ts = payload.get("t") or []
        opens = payload.get("o") or []
        highs = payload.get("h") or []
        lows = payload.get("l") or []
        closes = payload.get("c") or []
        n = min(len(ts), len(opens), len(highs), len(lows), len(closes))
        if n < 20:
            return None, f"Finnhub returned only {n} historical 1m candles"

        pd = _get_pandas()
        rows = []
        for i in range(n):
            try:
                rows.append({
                    "time": pd.to_datetime(int(ts[i]), unit="s", utc=True),
                    "Open": float(opens[i]),
                    "High": float(highs[i]),
                    "Low": float(lows[i]),
                    "Close": float(closes[i]),
                    "Volume": 0.0,
                })
            except Exception:
                continue
        if len(rows) < 20:
            return None, "Finnhub history parse produced too few candles"
        df = pd.DataFrame(rows).set_index("time").sort_index()
        df = df[~df.index.duplicated(keep="last")].tail(max(100, int(count)))
        with _FINNHUB_HISTORY_CACHE_LOCK:
            _FINNHUB_HISTORY_CACHE[key] = {"ts": now, "df": df.copy(), "note": "Finnhub/OANDA history"}
        return df, "Finnhub/OANDA history"
    except Exception as exc:
        return None, f"Finnhub history unavailable: {type(exc).__name__}: {exc}"

def _finnhub_fallback_seed_1m(pair, count=1600):
    """Fallback only when Finnhub history is not included in the account plan."""
    symbol = YAHOO_SYMBOLS.get(str(pair or "").strip())
    if not symbol:
        return None, "No fallback symbol"
    try:
        df = fetch_yahoo_1m(symbol)
        if df is None or getattr(df, "empty", True):
            return None, "Yahoo fallback seed unavailable"
        return df.tail(max(100, int(count))).copy(), "Yahoo closed-history seed"
    except Exception as exc:
        return None, f"Yahoo fallback seed error: {type(exc).__name__}: {exc}"

def _finnhub_live_base_df(pair, count=1800):
    """HYBRID normal-Forex feed requested by user.

    CLOSED/history candles: Yahoo Finance 1m history.
    LIVE/current price + forming candle: Finnhub/OANDA WebSocket.
    This gives an immediate 25/50-candle chart while preserving Finnhub as
    the real-time live Forex source.
    """
    pair = str(pair or "").strip()
    _ensure_finnhub_live_stream()

    if pair not in FINNHUB_FOREX_SYMBOLS:
        return None, None, pair, {
            "source": "Finnhub / OANDA + Yahoo",
            "source_mode": "unsupported_finnhub_pair",
            "provider_symbol": None,
            "unavailable_reason": f"{pair} is not configured for Finnhub/OANDA.",
        }

    # Load previously collected Finnhub candles so charts are populated immediately
    # on future app opens/restarts.
    try:
        _finnhub_store_load_into_memory(pair)
    except Exception:
        pass

    pd = _get_pandas()
    pieces = []
    history_note = None

    # 1) Yahoo closed/history candles for immediate chart depth.
    yahoo_symbol = YAHOO_SYMBOLS.get(pair)
    if yahoo_symbol:
        try:
            update_symbol_cache(yahoo_symbol)
            with cache_lock:
                cached = market_cache.get(yahoo_symbol)
            if cached and cached.get("data") is not None:
                hist = _raja_ensure_datetime_index(cached["data"].copy())
                if hist is not None and not hist.empty:
                    # Drop the current open minute from Yahoo so the live/forming
                    # candle is owned only by Finnhub/OANDA.
                    now_minute = int(time.time() // 60 * 60)
                    try:
                        hist = hist[hist.index.view("int64") // 10**9 < now_minute]
                    except Exception:
                        pass
                    if not hist.empty:
                        cols = ["Open", "High", "Low", "Close"]
                        if "Volume" in hist.columns:
                            cols.append("Volume")
                        pieces.append(hist[cols].tail(max(100, int(count))).copy())
                        history_note = "Yahoo closed-history candles"
        except Exception as exc:
            history_note = f"Yahoo history unavailable: {type(exc).__name__}: {exc}"

    # 2) Finnhub WebSocket completed bars collected since this process started.
    with _finnhub_live_lock:
        state = _finnhub_live_state.get(pair) or {}
        closed = OrderedDict(state.get("closed_1m") or {})
        forming = dict((state.get("forming") or {}).get("1") or {})
        last_tick_ms = state.get("last_tick_ms")
        price = state.get("price")
        messages = int(state.get("message_count") or 0)

    if closed:
        idx = pd.to_datetime(list(closed.keys()), unit="s", utc=True)
        live_closed = pd.DataFrame(list(closed.values()), index=idx).sort_index()
        pieces.append(live_closed)

    # 3) Finnhub current/forming candle.
    if forming:
        try:
            idx = pd.to_datetime([int(forming["t"])], unit="s", utc=True)
            form_df = pd.DataFrame([{
                "Open": float(forming["o"]),
                "High": float(forming["h"]),
                "Low": float(forming["l"]),
                "Close": float(forming["c"]),
                "Volume": 0.0,
            }], index=idx)
            pieces.append(form_df)
        except Exception:
            pass

    if not pieces:
        snap = _finnhub_live_snapshot(pair)
        return None, snap.get("age_seconds"), FINNHUB_FOREX_SYMBOLS[pair], {
            "source": "Finnhub / OANDA LIVE + Yahoo history",
            "source_mode": "hybrid_waiting_for_candles",
            "provider_symbol": FINNHUB_FOREX_SYMBOLS[pair],
            "unavailable_reason": (
                snap.get("last_error")
                or history_note
                or "Waiting for Forex candles."
            ),
            "backup_used": True,
        }

    df = pd.concat(pieces).sort_index()
    df = _raja_ensure_datetime_index(df)
    if df is None or df.empty:
        return None, None, FINNHUB_FOREX_SYMBOLS[pair], {
            "source": "Finnhub / OANDA LIVE + Yahoo history",
            "source_mode": "hybrid_bad_index",
            "provider_symbol": FINNHUB_FOREX_SYMBOLS[pair],
            "unavailable_reason": "Forex candles could not be normalized.",
            "backup_used": True,
        }

    if "Volume" not in df.columns:
        df["Volume"] = 0.0
    df = df[["Open", "High", "Low", "Close", "Volume"]]
    df = df[~df.index.duplicated(keep="last")].tail(max(100, int(count)))

    age = None
    if last_tick_ms:
        try:
            age = max(0.0, time.time() - float(last_tick_ms) / 1000.0)
        except Exception:
            pass
    if age is None:
        age = _source_candle_age_seconds(df)

    return df, age, FINNHUB_FOREX_SYMBOLS[pair], {
        "source": "Finnhub / OANDA LIVE + Yahoo closed candles",
        "source_mode": "finnhub_live_yahoo_history",
        "provider_symbol": FINNHUB_FOREX_SYMBOLS[pair],
        "history_source": "Yahoo Finance",
        "live_source": "Finnhub/OANDA",
        "history_note": history_note,
        "live_price": price,
        "messages": messages,
        "feed_quality": _market_candle_quality(df),
        "backup_used": True,
        "credentials_required": True,
        "hybrid_forex": True,
    }


# =========================================================
# V70 DUKASCOPY BRIDGE CLIENT — NORMAL LIVE FOREX PRIMARY/ONLY
# The Java bridge supplies Dukascopy JForex BID 1-minute candles.
# Crypto Live will also use Dukascopy automatically IF the bridge exposes
# that crypto symbol via /pairs; otherwise the existing Twelve Data crypto
# path remains active. OTC routing is unchanged.
# =========================================================
DUKASCOPY_BRIDGE_URL = (os.environ.get("DUKASCOPY_BRIDGE_URL") or "").strip().rstrip("/")
DUKASCOPY_BRIDGE_TIMEOUT = max(2.0, min(15.0, float(os.environ.get("DUKASCOPY_BRIDGE_TIMEOUT", "7"))))
DUKASCOPY_PAIRS_CACHE_SECONDS = max(15, min(600, int(os.environ.get("DUKASCOPY_PAIRS_CACHE_SECONDS", "60"))))

# Broad Forex universe used by Quotex/Pocket Option live selectors.  The
# bridge remains authoritative: if Dukascopy does not expose a symbol, the
# request returns unavailable instead of silently switching normal Forex to
# Yahoo/OANDA/Twelve Data.
DUKASCOPY_FOREX_LIVE_PAIRS = {
    "EUR/USD","GBP/USD","USD/JPY","USD/CHF","AUD/USD","USD/CAD","NZD/USD",
    "EUR/JPY","GBP/JPY","CAD/JPY","AUD/JPY","CHF/JPY","NZD/JPY",
    "EUR/GBP","EUR/AUD","EUR/CAD","EUR/CHF","EUR/NZD",
    "GBP/AUD","GBP/CAD","GBP/CHF","GBP/NZD",
    "AUD/CAD","AUD/CHF","AUD/NZD","CAD/CHF","NZD/CAD","NZD/CHF",
    "USD/SGD","USD/HKD","USD/MXN","USD/ZAR","USD/TRY","USD/PLN","USD/CNH",
    "EUR/PLN","EUR/TRY","EUR/SEK","EUR/NOK","EUR/DKK",
    "GBP/SGD","GBP/SEK","GBP/NOK","AUD/SGD","NZD/SGD","CAD/SGD","CHF/SGD",
    "SGD/JPY","HKD/JPY","ZAR/JPY","TRY/JPY","USD/SEK","USD/NOK","USD/DKK",
}

# Candidate live crypto names used by RAJA.  They are NOT assumed to exist on
# Dukascopy.  The bridge /pairs response decides whether they can be routed to
# Dukascopy; otherwise Twelve Data stays the crypto source.
DUKASCOPY_CRYPTO_CANDIDATES = {
    "BTC-USD": "BTC/USD",
    "ETH-USD": "ETH/USD",
    "SOL-USD": "SOL/USD",
    "LTC-USD": "LTC/USD",
    "XRP-USD": "XRP/USD",
    "ADA-USD": "ADA/USD",
    "DOGE-USD": "DOGE/USD",
}

_dukascopy_pairs_cache = {"ts": 0.0, "pairs": set()}
_dukascopy_pairs_lock = threading.RLock()

# Let existing validation/selector code recognize the full live-Forex universe.
# These Yahoo-style values are compatibility metadata only; V70 never uses them
# as a fallback for normal live Forex.
for _d_pair in sorted(DUKASCOPY_FOREX_LIVE_PAIRS):
    YAHOO_SYMBOLS.setdefault(_d_pair, _d_pair.replace("/", "") + "=X")
    TWELVE_DATA_SYMBOLS.setdefault(_d_pair, _d_pair)

try:
    ALL_PAIRS = list(dict.fromkeys(list(ALL_PAIRS) + sorted(DUKASCOPY_FOREX_LIVE_PAIRS)))
except Exception:
    ALL_PAIRS = list(dict.fromkeys(list(YAHOO_SYMBOLS.keys())))


def _dukascopy_bridge_json(path, params=None):
    if not DUKASCOPY_BRIDGE_URL:
        raise RuntimeError("DUKASCOPY_BRIDGE_URL is not configured")
    url = DUKASCOPY_BRIDGE_URL + path
    if params:
        url += "?" + urlencode(params)
    req = UrlRequest(
        url,
        headers={"Accept": "application/json", "User-Agent": "RAJA-AI-Dukascopy/70"},
        method="GET",
    )
    with urlopen(req, timeout=DUKASCOPY_BRIDGE_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _dukascopy_bridge_pairs(force=False):
    now = time.time()
    with _dukascopy_pairs_lock:
        cached = set(_dukascopy_pairs_cache.get("pairs") or set())
        cached_ts = float(_dukascopy_pairs_cache.get("ts") or 0.0)
        if cached and not force and now - cached_ts <= DUKASCOPY_PAIRS_CACHE_SECONDS:
            return cached

    try:
        payload = _dukascopy_bridge_json("/pairs")
        pairs = {
            str(p or "").strip().upper()
            for p in (payload.get("pairs") or [])
            if str(p or "").strip()
        }
        with _dukascopy_pairs_lock:
            _dukascopy_pairs_cache["ts"] = now
            _dukascopy_pairs_cache["pairs"] = set(pairs)
        return pairs
    except Exception:
        # Keep the last known list if the /pairs request has a temporary problem.
        return cached


def _dukascopy_bridge_market_data(pair, count=1200, provider_pair=None):
    clean = str(pair or "").strip()
    bridge_pair = str(provider_pair or clean).strip().upper()

    if not DUKASCOPY_BRIDGE_URL:
        return None, None, bridge_pair, {
            "source": "Dukascopy JForex BID",
            "source_mode": "bridge_not_configured",
            "provider_symbol": bridge_pair.replace("/", ""),
            "unavailable_reason": "DUKASCOPY_BRIDGE_URL is not configured in the RAJA AI Railway service.",
            "backup_used": False,
        }

    try:
        payload = _dukascopy_bridge_json(
            "/candles",
            {"pair": bridge_pair, "count": max(25, min(1500, int(count)))},
        )
        rows = payload.get("candles") or []
        if not rows:
            raise RuntimeError("Dukascopy bridge returned no candles yet")

        pd = _get_pandas()
        frame = pd.DataFrame([
            {
                "t": int(x["t"]),
                "Open": float(x["o"]),
                "High": float(x["h"]),
                "Low": float(x["l"]),
                "Close": float(x["c"]),
                "Volume": 0.0,
            }
            for x in rows
        ])
        frame.index = pd.to_datetime(frame.pop("t"), unit="s", utc=True)
        frame = _raja_align_1m_boundaries(_raja_clean_ohlc_frame(frame))
        frame = frame[~frame.index.duplicated(keep="last")].sort_index()
        age = _source_candle_age_seconds(frame)
        gap_count = _raja_1m_gap_count(frame)

        return frame, age, bridge_pair.replace("/", ""), {
            "source": "Dukascopy JForex BID",
            "source_mode": "dukascopy_bridge",
            "provider_symbol": bridge_pair.replace("/", ""),
            "bridge_pair": bridge_pair,
            "bridge_url_configured": True,
            "candle_count": int(len(frame)),
            "gap_count_1m": int(gap_count),
            "feed_quality": _market_candle_quality(frame),
            "backup_used": False,
            "credentials_required": True,
            "exact_broker_feed": False,
        }
    except HTTPError as exc:
        try:
            body = exc.read().decode("utf-8", "ignore")
        except Exception:
            body = ""
        return None, None, bridge_pair.replace("/", ""), {
            "source": "Dukascopy JForex BID",
            "source_mode": "dukascopy_bridge_http_error",
            "provider_symbol": bridge_pair.replace("/", ""),
            "unavailable_reason": f"Dukascopy bridge HTTP {getattr(exc, 'code', '?')}: {body[:180] or 'request failed'}",
            "backup_used": False,
        }
    except Exception as exc:
        return None, None, bridge_pair.replace("/", ""), {
            "source": "Dukascopy JForex BID",
            "source_mode": "dukascopy_bridge_error",
            "provider_symbol": bridge_pair.replace("/", ""),
            "unavailable_reason": f"Dukascopy bridge: {type(exc).__name__}: {exc}",
            "backup_used": False,
        }


# =========================================================
# V70 FINAL MARKET SOURCE OVERRIDE
# 1) Normal slash-format Forex Live -> Dukascopy JForex bridge ONLY.
# 2) Crypto Live -> Dukascopy only when /pairs confirms that crypto symbol;
#    otherwise existing Twelve Data crypto path is retained.
# 3) OTC / broker-native / legacy instruments -> existing routing unchanged.
# =========================================================
_legacy_get_market_data_v70 = get_market_data


def get_market_data(pair, bridge_user=None, broker=None):
    clean = str(pair or "").strip()
    upper = clean.upper()
    is_otc = "(OTC)" in upper

    # All normal slash-format FX symbols are forced through Dukascopy.  This
    # intentionally blocks Yahoo/OANDA/Twelve fallback for normal Forex.
    is_normal_forex = (
        not is_otc
        and "/" in upper
        and upper != "XAUUSD"
        and upper not in set(DUKASCOPY_CRYPTO_CANDIDATES.values())
    )
    if is_normal_forex:
        return _dukascopy_bridge_market_data(upper, count=1500)

    # Crypto can use Dukascopy only if the Java bridge actually advertises the
    # corresponding crypto instrument.  Current Forex-only bridge deployments
    # will naturally fall through to Twelve Data here.
    if clean in TWELVE_DATA_CRYPTO_LIVE_PAIRS:
        bridge_symbol = DUKASCOPY_CRYPTO_CANDIDATES.get(clean)
        if bridge_symbol:
            supported = _dukascopy_bridge_pairs(force=False)
            if bridge_symbol.upper() in supported:
                return _dukascopy_bridge_market_data(clean, count=1500, provider_pair=bridge_symbol)
        return _twelve_data_only_market_data(clean, force=False)

    return _legacy_get_market_data_v70(clean, bridge_user=bridge_user, broker=broker)


@app.route("/dukascopy-bridge-status", methods=["GET"])
def dukascopy_bridge_status():
    health = None
    err = None
    try:
        health = _dukascopy_bridge_json("/health")
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
    pairs = sorted(_dukascopy_bridge_pairs(force=True)) if DUKASCOPY_BRIDGE_URL else []
    crypto_on_bridge = {
        app_pair: bridge_pair
        for app_pair, bridge_pair in DUKASCOPY_CRYPTO_CANDIDATES.items()
        if bridge_pair.upper() in set(pairs)
    }
    return jsonify({
        "status": "success" if health else "error",
        "configured": bool(DUKASCOPY_BRIDGE_URL),
        "health": health,
        "pair_count": len(pairs),
        "pairs": pairs,
        "crypto_on_dukascopy": crypto_on_bridge,
        "crypto_fallback": "Twelve Data" if len(crypto_on_bridge) < len(DUKASCOPY_CRYPTO_CANDIDATES) else None,
        "error": err,
    }), (200 if health else 503)


@app.route("/finnhub-test-status", methods=["GET"])
def finnhub_test_status():
    _ensure_finnhub_live_stream()
    pair = str(request.args.get("pair") or "CAD/JPY").strip()
    return jsonify({
        "status": "success",
        "data": _finnhub_live_snapshot(pair),
        "note": "Read-only Finnhub/OANDA live stream status. API key is never returned.",
    })

@app.route("/finnhub-live-chart-data", methods=["POST"])
def finnhub_live_chart_data():
    _ensure_finnhub_live_stream()
    data = request.get_json(silent=True) or {}
    pair = str(data.get("pair") or "").strip()
    tf = str(data.get("expiry") or "1m").strip().lower()
    try:
        count = max(25, min(60, int(data.get("candle_count") or 50)))
    except Exception:
        count = 50

    if pair not in FINNHUB_FOREX_SYMBOLS:
        return jsonify({
            "status": "error",
            "message": f"{pair or 'Selected pair'} is not configured for the Finnhub/OANDA Forex test.",
        }), 400

    snap = _finnhub_live_snapshot(pair)
    history, history_age, history_error = _finnhub_seed_history(pair, tf, count)
    forming = (snap.get("forming") or {}).get(str(_finnhub_tf_minutes(tf)))

    chart = list(history)
    if isinstance(forming, dict):
        # Replace same-time seed bar or append the live forming OANDA bar.
        if chart and int(chart[-1].get("t") or -1) == int(forming.get("t") or -2):
            chart[-1] = dict(forming)
        else:
            chart.append(dict(forming))
    chart = chart[-count:]

    if not chart:
        return jsonify({
            "status": "error",
            "message": snap.get("last_error") or history_error or "No chart candles available yet.",
        }), 503

    source = "Finnhub / OANDA LIVE"
    if history:
        source += " + Yahoo closed-history seed"

    result = {
        "pair": pair,
        "selected_expiry": tf,
        "timeframe": tf,
        "signal": "NO SIGNAL",
        "reason": "Finnhub/OANDA live-candle test. RAJA signal engine is not switched to this feed yet.",
        "no_trade": True,
        "model_confidence": 0,
        "chart_preview": chart,
        "pattern_scan_candles": max(0, len(chart) - (1 if forming else 0)),
        "source": source,
        "source_mode": "finnhub_oanda_live_chart_test",
        "provider_symbol": FINNHUB_FOREX_SYMBOLS[pair],
        "data_age": snap.get("age_seconds"),
        "feed_quality": {
            "grade": "LIVE" if snap.get("status") == "LIVE" else "WAIT",
            "messages": snap.get("message_count"),
        },
        "finnhub_status": snap.get("status"),
        "finnhub_live_price": snap.get("price"),
        "finnhub_tick_age": snap.get("age_seconds"),
        "forming_candle_excluded": True,
        "closed_candle_verified": True,
        "quality_warning": (
            "Closed history is temporarily seeded from Yahoo; the forming candle is Finnhub/OANDA live."
            if history else None
        ),
    }
    return jsonify({"status": "success", "data": result, "read_only": True})


def _finnhub_preload_all_pairs():
    try:
        for _pair in FINNHUB_FOREX_SYMBOLS:
            try:
                _finnhub_store_load_into_memory(_pair)
            except Exception:
                pass
        _ensure_finnhub_live_stream()
    except Exception:
        pass

_finnhub_preload_thread = threading.Thread(
    target=_finnhub_preload_all_pairs,
    name="finnhub-persistent-preload",
    daemon=True,
)
_finnhub_preload_thread.start()

@app.route("/health", methods=["GET"])
def health():
    with cache_lock:
        cached_symbols = len(market_cache)

    return jsonify({
        "status": "success",
        "service": "RAJA AI multi-timeframe backend",
        "app_build": APP_BUILD_ID,
        "license_store_ready": bool(_license_store_ready.is_set()),
        "yahoo_pairs": len(YAHOO_SYMBOLS),
        "unique_yahoo_symbols": len(UNIQUE_YAHOO_SYMBOLS),
        "cached_symbols": cached_symbols,
        "quotex_bridge_enabled": True,
        "browser_bridge_brokers": ["Quotex", "Pocket Option"],
        "broker_bridge_pair_fresh_seconds": BROKER_BRIDGE_PAIR_FRESH_SECONDS,
        "broker_bridge_market_max_age_seconds": BROKER_BRIDGE_MARKET_MAX_AGE_SECONDS,
        "broker_bridge_scan_min_candles": BROKER_BRIDGE_SCAN_MIN_CANDLES,
        "quotex_bridge_required_for_otc": False,
        "legacy_bridge_required_env": RAJA_REQUIRE_QUOTEX_BRIDGE_FOR_OTC,
        "quotex_reference_fallback_enabled": True,
        "legacy_reference_fallback_env": True,
        "strict_broker_otc": False,
        "broker_otc_source_priority": ["native_websocket", "yahoo_reference", "twelve_data_reference"],
        "native_broker_feeds": native_feed_status(),
        "base_interval": "1m",
        "timeframes_scanned": list(TIMEFRAMES.keys()),
        "cache_duration_seconds": CACHE_DURATION,
        "confirmation_mode": "SK25 STRICT · SELECTED TIMEFRAME ONLY",
        "duplicate_signal_cooldown_seconds": DUPLICATE_SIGNAL_COOLDOWN,
        "background_full_market_poller": False,
        "yahoo_fetch_concurrency": YAHOO_FETCH_CONCURRENCY,
        "yahoo_failure_cooldown_seconds": YAHOO_FAILURE_COOLDOWN,
        "batch_cache_seconds": BATCH_CACHE_DURATION,
        "yahoo_request_timeout_seconds": YAHOO_REQUEST_TIMEOUT_SECONDS,
        "yahoo_symbol_lock_wait_seconds": YAHOO_SYMBOL_LOCK_WAIT_SECONDS,
        "yahoo_semaphore_wait_seconds": YAHOO_SEMAPHORE_WAIT_SECONDS,
        "twelve_data_backup_configured": TWELVE_DATA_ENABLED,
        "twelve_data_primary_forex_crypto": True,
        "twelve_data_outputsize": TWELVE_DATA_OUTPUTSIZE,
        "twelve_data_cache_seconds": TWELVE_DATA_CACHE_SECONDS,
        "twelve_data_fetch_concurrency": TWELVE_DATA_FETCH_CONCURRENCY,
        "twelve_data_request_timeout_seconds": TWELVE_DATA_REQUEST_TIMEOUT_SECONDS,
        "market_data_priority": ["Yahoo Finance", "Twelve Data", "stale reference fallback"],
        "max_source_candle_age_seconds": MAX_SOURCE_CANDLE_AGE_SECONDS,
        "batch_deadline_seconds": BATCH_SCAN_DEADLINE_SECONDS,
        "forex_otc_fallback_deadline_seconds": FOREX_OTC_FALLBACK_DEADLINE_SECONDS,
        "automatic_outcome_tracking": list(AUTO_TRACK_EXPIRIES.keys()),
        "closed_candle_analysis": True,
        "pocket_option_reference_assets": {
            "forex_otc": len(POCKET_OPTION_FOREX_OTC_PAIRS),
            "crypto_otc": len(POCKET_OPTION_CRYPTO_OTC_PAIRS),
            "stocks_otc": len(POCKET_OPTION_STOCKS_OTC_PAIRS),
        },
        "license_store": LICENSE_STORE_MODE,
        "persistent_license_store": bool(DATABASE_URL),
    })


@app.route("/market-news", methods=["GET"])
def market_news():
    now = time.time()
    with market_news_lock:
        cached_data = list(market_news_cache.get("data") or [])
        cached_at = float(market_news_cache.get("timestamp") or 0.0)

    if cached_data and (now - cached_at) <= MARKET_NEWS_CACHE_SECONDS:
        return jsonify({
            "status": "success",
            "source": "Forex Factory weekly calendar export",
            "source_url": "https://www.forexfactory.com/calendar",
            "data": cached_data,
            "cache_hit": True,
            "stale": False,
            "updated_at": int(cached_at),
        })

    try:
        fresh_data = fetch_forex_factory_calendar()
        with market_news_lock:
            market_news_cache["timestamp"] = time.time()
            market_news_cache["data"] = fresh_data
            updated_at = market_news_cache["timestamp"]

        return jsonify({
            "status": "success",
            "source": "Forex Factory weekly calendar export",
            "source_url": "https://www.forexfactory.com/calendar",
            "data": fresh_data,
            "cache_hit": False,
            "stale": False,
            "updated_at": int(updated_at),
        })
    except Exception as exc:
        print(f"Forex Factory calendar fetch error: {exc}")
        # If the upstream feed is briefly unavailable, serve the last successful
        # snapshot instead of leaving the News panel blank.
        if cached_data:
            return jsonify({
                "status": "success",
                "source": "Forex Factory weekly calendar export",
                "source_url": "https://www.forexfactory.com/calendar",
                "data": cached_data,
                "cache_hit": True,
                "stale": True,
                "updated_at": int(cached_at),
                "warning": "Live source temporarily unavailable; showing last cached calendar.",
            })
        return jsonify({
            "status": "error",
            "message": "Live economic calendar is temporarily unavailable.",
        }), 502


@app.route("/app-state", methods=["GET"])
def app_state():
    state = load_app_control_state()
    # Only public control information is exposed here.
    return jsonify({
        "status": "success",
        "data": {
            "maintenance": bool(state.get("maintenance")),
            "maintenance_message": str(state.get("maintenance_message") or ""),
            "broadcast": state.get("broadcast") or {},
            "updated_at": int(state.get("updated_at") or 0),
            "server_time": int(time.time()),
        },
    })


@app.route("/admin/app-control", methods=["POST"])
def admin_app_control():
    data = request.get_json(silent=True) or {}
    auth_error = _validate_admin_password(data)
    if auth_error:
        return auth_error

    action = str(data.get("action") or "get").strip().lower()
    state = load_app_control_state()

    if action == "get":
        return jsonify({"status": "success", "data": state})

    if action == "broadcast":
        message = str(data.get("message") or "").strip()[:500]
        if not message:
            return jsonify({"status": "error", "message": "Broadcast message is required."}), 400
        level = str(data.get("level") or "info").lower()
        if level not in {"info", "warning", "critical"}:
            level = "info"
        now = int(time.time())
        state["broadcast"] = {
            "active": True,
            "id": f"broadcast-{now}-{secrets.token_hex(3)}",
            "message": message,
            "level": level,
            "created_at": now,
        }

    elif action == "clear_broadcast":
        state["broadcast"] = default_app_control_state()["broadcast"]

    elif action == "maintenance":
        enabled = bool(data.get("maintenance", False))
        message = str(data.get("maintenance_message") or "").strip()[:300]
        state["maintenance"] = enabled
        if message:
            state["maintenance_message"] = message
        elif enabled:
            state["maintenance_message"] = "RAJA AI scanning is temporarily paused for maintenance."

    else:
        return jsonify({"status": "error", "message": "Unknown app-control action."}), 400

    state = save_app_control_state(state)
    return jsonify({"status": "success", "data": state})


@app.route("/verify-license", methods=["POST"])
def verify_license():
    data = request.get_json(silent=True) or {}
    key = str(data.get("key", "")).strip()
    user = normalize_user_id(data.get("user", ""))
    device = str(data.get("device", "")).strip()
    device_label = str(data.get("device_label", "")).strip()[:160]
    heartbeat = bool(data.get("heartbeat", False))
    supplied_token = str(data.get("session_token", "")).strip()
    if not key or not user or not device:
        return jsonify({"status": "error", "message": "Key, user and device are required."}), 400

    now = int(time.time())

    if DATABASE_URL:
        # Heartbeats/saved-session checks use one atomic UPDATE ... RETURNING query.
        # This removes the previous SELECT + UPDATE round trip and avoids an
        # unnecessary FOR UPDATE lock for the common already-logged-in path.
        if heartbeat:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE raja_licenses
                        SET last_verified_at=%s
                        WHERE license_key=%s
                          AND active=TRUE
                          AND lower(COALESCE(user_id,''))=%s
                          AND device_id=%s
                          AND session_token=%s
                          AND (expires_at IS NULL OR expires_at > %s)
                        RETURNING device_label, session_token, COALESCE(plan,%s), expires_at
                    """, (now, key, user, device, supplied_token, now, DEFAULT_LICENSE_PLAN))
                    row = cur.fetchone()
                    if not row:
                        return jsonify({
                            "status": "error",
                            "message": "This session is invalid, expired, revoked or was replaced by a newer device login."
                        }), 409
                    device_label_db, session_token_db, plan_db, expires_at_db = row
                    return jsonify({
                        "status": "success", "message": "Session active.", "user": user,
                        "device_bound": True, "device_label": device_label_db,
                        "session_token": session_token_db,
                        "plan": plan_db or DEFAULT_LICENSE_PLAN,
                        "expires_at": expires_at_db,
                    })

        # Full login/takeover still uses a row lock so simultaneous device logins
        # cannot issue competing session tokens. It remains one DB transaction.
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT active, user_id, device_id, device_label, created_at,
                           last_verified_at, session_token, plan, expires_at, last_login_at
                    FROM raja_licenses
                    WHERE license_key = %s
                    LIMIT 1
                    FOR UPDATE
                """, (key,))
                row = cur.fetchone()

                if not row:
                    return jsonify({"status": "error", "message": "Invalid or revoked license key."}), 401

                (active, bound_user_raw, previous_device_raw, previous_label_raw, created_at,
                 last_verified_at, stored_token_raw, plan_raw, expires_at, last_login_at) = row
                record = {
                    "active": bool(active),
                    "user": bound_user_raw,
                    "device": previous_device_raw,
                    "device_label": previous_label_raw,
                    "created_at": created_at,
                    "last_verified_at": last_verified_at,
                    "session_token": stored_token_raw,
                    "plan": plan_raw or DEFAULT_LICENSE_PLAN,
                    "expires_at": expires_at,
                    "last_login_at": last_login_at,
                }

                if not record["active"]:
                    return jsonify({"status": "error", "message": "Invalid or revoked license key."}), 401
                if license_is_expired(record, now):
                    return jsonify({"status": "error", "message": "This license has expired. Contact admin to renew access."}), 401

                bound_user = normalize_user_id(record.get("user", ""))
                if bound_user and bound_user != user:
                    return jsonify({"status": "error", "message": "This key is assigned to another user/UID."}), 403

                is_free_trial = str(record.get("plan") or "").strip().upper() == "FREE TRIAL"
                if is_free_trial:
                    cur.execute(
                        "SELECT license_key FROM raja_trial_claims WHERE claim_type=%s AND claim_value=%s",
                        ("device", device),
                    )
                    claim_row = cur.fetchone()
                    if claim_row and str(claim_row[0] or "") != key:
                        return jsonify({
                            "status": "error",
                            "message": "This device has already used a free trial. Contact admin for VIP access."
                        }), 409

                previous_device = str(record.get("device") or "")
                previous_label = str(record.get("device_label") or "")
                new_token = secrets.token_urlsafe(32)
                next_label = device_label or device
                next_plan = record.get("plan") or DEFAULT_LICENSE_PLAN

                cur.execute("""
                    UPDATE raja_licenses
                    SET device_id=%s, device_label=%s, user_id=%s, session_token=%s,
                        last_verified_at=%s, last_login_at=%s, plan=%s
                    WHERE license_key=%s
                """, (device, next_label, user, new_token, now, now, next_plan, key))

                if is_free_trial:
                    cur.execute("""
                        INSERT INTO raja_trial_claims(claim_type, claim_value, license_key, created_at)
                        VALUES(%s,%s,%s,%s)
                        ON CONFLICT(claim_type, claim_value) DO NOTHING
                    """, ("device", device, key, now))

                return jsonify({
                    "status": "success", "message": "License verified successfully.", "user": user,
                    "device_bound": True, "device_label": next_label, "session_token": new_token,
                    "replaced_previous_device": bool(previous_device and previous_device != device),
                    "previous_device_label": previous_label if previous_device and previous_device != device else None,
                    "plan": next_plan, "expires_at": record.get("expires_at"),
                })

    # File-storage fallback keeps the existing behavior for local development/testing.
    record = load_license_record(key)
    if not record or not record.get("active", False):
        return jsonify({"status": "error", "message": "Invalid or revoked license key."}), 401
    if license_is_expired(record, now):
        return jsonify({"status": "error", "message": "This license has expired. Contact admin to renew access."}), 401

    bound_user = normalize_user_id(record.get("user", ""))
    if bound_user and bound_user != user:
        return jsonify({"status": "error", "message": "This key is assigned to another user/UID."}), 403

    is_free_trial = str(record.get("plan") or "").strip().upper() == "FREE TRIAL"
    if is_free_trial and not heartbeat:
        device_claim = get_trial_claim("device", device)
        if device_claim and str(device_claim.get("license_key") or "") != key:
            return jsonify({
                "status": "error",
                "message": "This device has already used a free trial. Contact admin for VIP access."
            }), 409

    if heartbeat:
        if str(record.get("device") or "") != device or not supplied_token or str(record.get("session_token") or "") != supplied_token:
            return jsonify({"status": "error", "message": "This session was replaced by a newer device login."}), 409
        record["last_verified_at"] = now
        save_license_record(key, record)
        return jsonify({
            "status": "success", "message": "Session active.", "user": user,
            "device_bound": True, "device_label": record.get("device_label"),
            "session_token": record.get("session_token"), "plan": record.get("plan") or DEFAULT_LICENSE_PLAN,
            "expires_at": record.get("expires_at"),
        })

    previous_device = str(record.get("device") or "")
    previous_label = str(record.get("device_label") or "")
    new_token = secrets.token_urlsafe(32)
    record["device"] = device
    record["device_label"] = device_label or device
    record["user"] = user
    record["session_token"] = new_token
    record["last_verified_at"] = now
    record["last_login_at"] = now
    record["plan"] = record.get("plan") or DEFAULT_LICENSE_PLAN
    save_license_record(key, record)
    if is_free_trial:
        record_trial_claim("device", device, key)
    return jsonify({
        "status": "success", "message": "License verified successfully.", "user": user,
        "device_bound": True, "device_label": record.get("device_label"), "session_token": new_token,
        "replaced_previous_device": bool(previous_device and previous_device != device),
        "previous_device_label": previous_label if previous_device and previous_device != device else None,
        "plan": record.get("plan"), "expires_at": record.get("expires_at"),
    })


@app.route("/logout-license", methods=["POST"])
def logout_license():
    data = request.get_json(silent=True) or {}
    key = str(data.get("key", "")).strip()
    user = normalize_user_id(data.get("user", ""))
    device = str(data.get("device", "")).strip()
    token = str(data.get("session_token", "")).strip()
    if not key or not user or not device:
        return jsonify({"status": "error", "message": "Key, user and device are required."}), 400

    if DATABASE_URL:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE raja_licenses
                    SET device_id=NULL, device_label=NULL, last_verified_at=NULL, session_token=NULL
                    WHERE license_key=%s
                      AND lower(COALESCE(user_id,''))=%s
                      AND device_id=%s
                      AND (session_token IS NULL OR session_token=%s)
                """, (key, user, device, token))
        return jsonify({"status": "success", "message": "Device session released."})

    record = load_license_record(key)
    if not record:
        return jsonify({"status": "success", "message": "Session already cleared."})
    if (normalize_user_id(record.get("user", "")) == user and record.get("device") == device
            and (not record.get("session_token") or record.get("session_token") == token)):
        record["device"] = None
        record["device_label"] = None
        record["last_verified_at"] = None
        record["session_token"] = None
        save_license_record(key, record)
    return jsonify({"status": "success", "message": "Device session released."})


@app.route("/user/profile", methods=["POST"])
def user_profile():
    data = request.get_json(silent=True) or {}
    auth, error = _auth_session(data)
    if error:
        return error
    record = auth["record"]
    user = auth["user"]
    now = int(time.time())
    day_start = now - (now % 86400)
    events = _load_scan_events(5000)
    user_events = [e for e in events if normalize_user_id(e.get("user", "")) == user]
    scans_today = sum(1 for e in user_events if int(e.get("created_at") or 0) >= day_start)
    return jsonify({
        "status": "success",
        "data": {
            "user": user, "plan": record.get("plan") or DEFAULT_LICENSE_PLAN,
            "expires_at": record.get("expires_at"), "created_at": record.get("created_at"),
            "last_login_at": record.get("last_login_at"), "last_active_at": record.get("last_verified_at"),
            "device": record.get("device"), "device_label": record.get("device_label"),
            "scans_today": scans_today, "total_scans": len(user_events),
        }
    })


@app.route("/admin/generate-key", methods=["POST"])
def admin_generate_key():
    data = request.get_json(silent=True) or {}
    password = str(data.get("password", ""))
    user = normalize_user_id(data.get("user", ""))
    plan = str(data.get("plan") or DEFAULT_LICENSE_PLAN).strip().upper()[:40]
    try:
        # Supports short trials too:
        # 0.5 day = 12 hours, 1 day = 24 hours.
        duration_days = max(0.0, min(3650.0, float(data.get("duration_days") or 0)))
    except Exception:
        duration_days = 0.0
    auth_error = _validate_admin_password(data)
    if auth_error:
        return auth_error
    if not user:
        return jsonify({"status": "error", "message": "User Telegram ID / UID is required."}), 400

    is_free_trial = plan == "FREE TRIAL"
    if is_free_trial:
        previous_claim = get_trial_claim("user", user)
        if previous_claim:
            return jsonify({
                "status": "error",
                "message": "Free trial already used for this Telegram ID / UID. Create a VIP license instead."
            }), 409

    while True:
        key = "RAJA-VIP-" + secrets.token_hex(4).upper() + "-2026"
        if load_license_record(key) is None:
            break
    now = int(time.time())
    record = {
        "active": True, "user": user, "device": None, "device_label": None,
        "session_token": None, "created_at": now, "last_verified_at": None,
        "last_login_at": None, "plan": plan, "expires_at": now + int(duration_days * 86400) if duration_days else None,
    }
    save_license_record(key, record)
    if is_free_trial:
        record_trial_claim("user", user, key)
    return jsonify({"status": "success", "message": "License created.", "key": key, "user": user,
                    "plan": plan, "expires_at": record.get("expires_at")})


@app.route("/admin/licenses", methods=["POST"])
def admin_list_licenses():
    data = request.get_json(silent=True) or {}
    auth_error = _validate_admin_password(data)
    if auth_error:
        return auth_error
    now = int(time.time())
    licenses = load_licenses()
    rows = []
    for key, record in licenses.items():
        record = record if isinstance(record, dict) else {}
        last_verified_at = int(record.get("last_verified_at") or 0)
        session_active = bool(record.get("device") and record.get("session_token") and last_verified_at and (now - last_verified_at) < 90)
        rows.append({
            "key": key, "active": bool(record.get("active", False)) and not license_is_expired(record, now),
            "expired": license_is_expired(record, now), "user": record.get("user"), "device": record.get("device"),
            "device_label": record.get("device_label"), "session_active": session_active,
            "created_at": record.get("created_at"), "last_verified_at": record.get("last_verified_at"),
            "last_login_at": record.get("last_login_at"), "plan": record.get("plan") or DEFAULT_LICENSE_PLAN,
            "expires_at": record.get("expires_at"),
        })
    rows.sort(key=lambda x: int(x.get("created_at") or 0), reverse=True)
    bound_count = sum(1 for row in rows if row.get("device"))
    online_count = sum(1 for row in rows if row.get("session_active"))
    return jsonify({"status": "success", "data": rows, "count": len(rows), "summary": {
        "total": len(rows), "active": sum(1 for row in rows if row.get("active")), "bound": bound_count,
        "online": online_count, "unbound": max(0, len(rows) - bound_count),
        "expired": sum(1 for row in rows if row.get("expired")),
    }})


def _validate_admin_password(data):
    # Fail closed: admin routes are disabled unless Render provides a password.
    if not ADMIN_PASSWORD:
        return jsonify({
            "status": "error",
            "message": "Admin access is disabled because RAJA_ADMIN_PASSWORD is not configured."
        }), 503

    supplied = str((data or {}).get("password", ""))
    if not hmac.compare_digest(supplied, ADMIN_PASSWORD):
        return jsonify({"status": "error", "message": "Incorrect admin password."}), 403
    return None


@app.route("/admin/analytics", methods=["POST"])
def admin_analytics():
    data = request.get_json(silent=True) or {}
    auth_error = _validate_admin_password(data)
    if auth_error:
        return auth_error
    now = int(time.time())
    day_start = now - (now % 86400)
    events = [e for e in _load_scan_events(10000) if int(e.get("created_at") or 0) >= day_start]
    user_counts = Counter(normalize_user_id(e.get("user", "")) for e in events if e.get("user"))
    market_counts = Counter(str(e.get("market") or "Unknown") for e in events)
    licenses = load_licenses()
    online = sum(1 for r in licenses.values() if r.get("device") and r.get("session_token") and int(r.get("last_verified_at") or 0) >= now - 90)
    return jsonify({"status": "success", "data": {
        "scans_today": len(events), "signals_today": sum(1 for e in events if e.get("signal_found")),
        "most_active_customer": user_counts.most_common(1)[0][0] if user_counts else "--",
        "most_active_customer_scans": user_counts.most_common(1)[0][1] if user_counts else 0,
        "most_scanned_market": market_counts.most_common(1)[0][0] if market_counts else "--",
        "most_scanned_market_count": market_counts.most_common(1)[0][1] if market_counts else 0,
        "active_licenses": sum(1 for r in licenses.values() if r.get("active") and not license_is_expired(r, now)),
        "online_users": online,
    }})


@app.route("/admin/reset-device", methods=["POST"])
def admin_reset_device():
    data = request.get_json(silent=True) or {}
    auth_error = _validate_admin_password(data)
    if auth_error:
        return auth_error
    key = str(data.get("key", "")).strip()
    if not key:
        return jsonify({"status": "error", "message": "License key is required."}), 400
    record = load_license_record(key)
    if not record:
        return jsonify({"status": "error", "message": "License key not found."}), 404
    record["device"] = None
    record["device_label"] = None
    record["last_verified_at"] = None
    record["session_token"] = None
    save_license_record(key, record)
    return jsonify({"status": "success", "message": "Device binding reset.", "key": key})


@app.route("/admin/reset-all-devices", methods=["POST"])
def admin_reset_all_devices():
    data = request.get_json(silent=True) or {}
    auth_error = _validate_admin_password(data)
    if auth_error:
        return auth_error
    updated, total = reset_all_license_devices()
    return jsonify({"status": "success", "message": "All device bindings cleared.", "updated": updated, "total": total})


def _delete_license_from_request(data):
    key = str(data.get("key", "")).strip()
    auth_error = _validate_admin_password(data)
    if auth_error:
        return None, auth_error
    if not key:
        return None, (jsonify({"status": "error", "message": "License key is required."}), 400)
    if not delete_license_record(key):
        return None, (jsonify({"status": "error", "message": "License key not found."}), 404)
    return key, None


@app.route("/admin/delete-key", methods=["POST"])
def admin_delete_key():
    data = request.get_json(silent=True) or {}; key, error = _delete_license_from_request(data)
    if error: return error
    return jsonify({"status": "success", "message": "License removed permanently.", "key": key})


@app.route("/admin/revoke-key", methods=["POST"])
def admin_revoke_key():
    data = request.get_json(silent=True) or {}; key, error = _delete_license_from_request(data)
    if error: return error
    return jsonify({"status": "success", "message": "License removed permanently.", "key": key})


@app.route("/admin/clear-keys", methods=["POST"])
def admin_clear_keys():
    data = request.get_json(silent=True) or {}
    auth_error = _validate_admin_password(data)
    if auth_error:
        return auth_error
    removed = clear_all_license_records()
    return jsonify({"status": "success", "message": "All license keys removed.", "removed": removed})




@app.route("/track-signal", methods=["POST"])
def track_signal():
    data = request.get_json(silent=True) or {}
    auth, error = _auth_session(data)
    if error:
        return error
    pair = str(data.get("pair", "")).strip(); direction = str(data.get("signal", "")).strip().upper()
    expiry = str(data.get("expiry", "")).strip(); score = data.get("score")
    timeframe_summary = data.get("timeframe_summary") or {}; client_id = str(data.get("client_id", "")).strip()
    if pair not in YAHOO_SYMBOLS and "(OTC)" not in pair:
        return jsonify({"status": "error", "message": "Unsupported pair."}), 400
    if direction not in {"CALL", "PUT"}:
        return jsonify({"status": "error", "message": "Signal must be CALL or PUT."}), 400
    if expiry not in AUTO_TRACK_EXPIRIES:
        return jsonify({"status": "success", "auto_tracking": False,
                        "message": "15s/30s outcome tracking is disabled because the configured candle feed is 1-minute."})
    now = int(time.time()); duration = AUTO_TRACK_EXPIRIES[expiry]
    try:
        closed_candle_epoch = int(float(data.get("closed_candle_epoch") or 0))
    except Exception:
        closed_candle_epoch = 0

    # V38 PREP-ENTRY MODE:
    # Give the trader a real countdown instead of returning an already-expired
    # entry. The signal is scheduled on the next timeframe-aligned candle that
    # still provides at least RAJA_MIN_ENTRY_NOTICE_SECONDS of preparation.
    # Outcome tracking uses this same scheduled entry, so UI and backend stay aligned.
    original_target_entry_epoch = (closed_candle_epoch + duration) if closed_candle_epoch else ((now // duration) + 1) * duration
    try:
        min_entry_notice_seconds = max(10, min(30, int(os.environ.get("RAJA_MIN_ENTRY_NOTICE_SECONDS", "10"))))
    except Exception:
        min_entry_notice_seconds = 10

    earliest_entry_epoch = now + min_entry_notice_seconds
    notice_aligned_entry_epoch = ((earliest_entry_epoch + duration - 1) // duration) * duration
    entry_epoch = max(original_target_entry_epoch, notice_aligned_entry_epoch)
    expiry_epoch = entry_epoch + duration

    # Keep a small post-open grace as a safety fallback for device/browser latency.
    default_entry_grace = max(10, min(45, int(duration * 0.50)))
    try:
        entry_grace_seconds = max(5, min(90, int(os.environ.get("RAJA_ENTRY_GRACE_SECONDS", str(default_entry_grace)))))
    except Exception:
        entry_grace_seconds = default_entry_grace

    signal_id = "sig_" + secrets.token_hex(8)
    item = {
        "id": signal_id, "client_id": client_id, "user": auth["user"], "pair": pair, "signal": direction,
        "score": float(score or 0), "expiry": expiry, "created_at": now, "entry_epoch": entry_epoch,
        "expiry_epoch": expiry_epoch, "entry_grace_seconds": entry_grace_seconds,
        "setup_candle_open_epoch": (closed_candle_epoch if closed_candle_epoch else original_target_entry_epoch - duration),
        "setup_candle_close_epoch": original_target_entry_epoch, "target_candle_open_epoch": entry_epoch,
        "target_candle_close_epoch": expiry_epoch, "entry_price": None, "exit_price": None, "result": None,
        "status": "PENDING", "result_source": "pending",
        "original_target_entry_epoch": original_target_entry_epoch,
        "entry_notice_seconds": max(0, entry_epoch - now),
        "source": str(data.get("source") or "Yahoo Finance"),
        "source_mode": str(data.get("source_mode") or ("underlying_proxy" if "(OTC)" in pair else "live_reference")),
        "provider_symbol": data.get("provider_symbol"),
        "timeframe_summary": timeframe_summary, "chart_preview": data.get("chart_preview") or [],
        "scan_mode": "SK25_STRICT", "entry_timing_mode": "PREP_ENTRY", "volatility_pct": data.get("volatility_pct"),
        "pattern_type": int(data.get("pattern_type") or 0),
        "timeframe": str(data.get("timeframe") or expiry),
        "market_regime": str((data.get("market_regime_profile") or {}).get("regime") or data.get("market_regime") or ""),
        "adaptive_context_learning": data.get("adaptive_context_learning") or {},
        "selected_pattern": str(data.get("selected_pattern") or ""),
        "next_candle_color": str(data.get("next_candle_color") or ""),
        "setup_match": float(data.get("setup_match") or score or 0),
        "rules": data.get("rules") or [], "recovery_trade": bool(data.get("recovery_trade")),
        "closed_candle_epoch": closed_candle_epoch or data.get("closed_candle_epoch"),
        "snapshot": data.get("snapshot") or {}, "market": data.get("market"),
        "broker": str(data.get("broker") or ""),
    }
    with signals_lock:
        items = load_signals(); items.insert(0, item); save_signals(items[:2000])
    return jsonify({"status": "success", "auto_tracking": True, "signal_id": signal_id,
                    "entry_epoch": entry_epoch, "expiry_epoch": expiry_epoch,
                    "entry_grace_seconds": entry_grace_seconds, "server_epoch": now,
                    "entry_notice_seconds": max(0, entry_epoch - now),
                    "entry_timing_mode": "PREP_ENTRY",
                    "original_target_entry_epoch": original_target_entry_epoch,
                    "setup_candle_open_epoch": (closed_candle_epoch if closed_candle_epoch else original_target_entry_epoch - duration),
                    "setup_candle_close_epoch": original_target_entry_epoch,
                    "target_candle_open_epoch": entry_epoch, "target_candle_close_epoch": expiry_epoch,
                    "message": f"Signal ready. Prepare now and enter on the scheduled {expiry} candle after the countdown."})


@app.route("/signals/result", methods=["POST"])
def set_signal_result():
    data = request.get_json(silent=True) or {}
    auth, error = _auth_session(data)
    if error: return error
    signal_id = str(data.get("signal_id", "")).strip(); result = str(data.get("result", "")).strip().upper()
    if not signal_id or result not in {"WIN", "LOSS", "DRAW"}:
        return jsonify({"status": "error", "message": "Valid signal_id and WIN/LOSS/DRAW result are required."}), 400
    with signals_lock:
        items = load_signals(); target = next((x for x in items if x.get("id") == signal_id), None)
        if target is None: return jsonify({"status": "error", "message": "Signal not found."}), 404
        if target.get("user") and normalize_user_id(target.get("user")) != auth["user"]:
            return jsonify({"status": "error", "message": "This signal belongs to another user."}), 403
        if "(OTC)" not in str(target.get("pair", "")):
            return jsonify({"status": "error", "message": "Manual Quotex result is only used for OTC signals."}), 400
        target["result"] = result; target["status"] = "COMPLETED"; target["result_source"] = "quotex_manual"; target["resolved_at"] = int(time.time())
        save_signals(items)
        with performance_history_cache_lock:
            performance_history_cache.pop(auth["user"], None)
    return jsonify({
        "status": "success",
        "signal_id": signal_id,
        "result": result,
        "result_source": "quotex_manual",
        "yahoo_proxy_result": target.get("yahoo_result"),
        "reference_proxy_result": target.get("reference_result") or target.get("yahoo_result") or target.get("backup_result"),
        "reference_proxy_source": target.get("result_reference_source") or target.get("source"),
    })


@app.route("/signals/history", methods=["GET"])
def signals_history():
    try:
        limit = max(1, min(int(request.args.get("limit", 30)), 100))
    except Exception:
        limit = 30

    auth_data = {k: request.args.get(k, "") for k in ("key", "user", "device", "session_token")}
    auth, error = _auth_session(auth_data)
    if error:
        return error

    # Do one on-demand resolution pass before returning history. This prevents
    # WIN/LOSS from appearing stuck when the background worker has not ticked yet.
    with signals_lock:
        items = load_signals()
        if resolve_due_signals(items):
            save_signals(items)

    user_items = [x for x in items if normalize_user_id(x.get("user", "")) == auth["user"]]
    # Backward compatibility: include old same-device history records that predate user tagging.
    user_items.extend([
        x for x in items
        if not x.get("user")
        and str(x.get("client_id", "")) == auth["device"]
        and x not in user_items
    ])
    user_items.sort(key=lambda x: int(x.get("created_at") or 0), reverse=True)

    now = int(time.time())
    view_items = []
    for raw in user_items[:limit]:
        item = dict(raw)
        item["phase"] = tracked_signal_phase(item, now)
        entry_epoch = int(item.get("entry_epoch") or 0)
        expiry_epoch = int(item.get("expiry_epoch") or 0)
        item["seconds_to_entry"] = max(0, entry_epoch - now) if entry_epoch else None
        item["seconds_to_expiry"] = max(0, expiry_epoch - now) if expiry_epoch else None
        view_items.append(item)

    return jsonify({
        "status": "success",
        "data": view_items,
        "stats": signal_stats(user_items),
        "server_epoch": now,
    })


@app.route("/signals/stats", methods=["GET"])
def signals_stats():
    auth_data = {k: request.args.get(k, "") for k in ("key", "user", "device", "session_token")}
    auth, error = _auth_session(auth_data)
    if error:
        return error

    with signals_lock:
        all_items = load_signals()
        if resolve_due_signals(all_items):
            save_signals(all_items)

    items = [x for x in all_items if normalize_user_id(x.get("user", "")) == auth["user"]]
    return jsonify({"status": "success", "stats": signal_stats(items)})


@app.route("/signals/strategy-stats", methods=["GET"])
def strategy_stats_endpoint():
    auth_data = {k: request.args.get(k, "") for k in ("key", "user", "device", "session_token")}
    auth, error = _auth_session(auth_data)
    if error:
        return error
    with signals_lock:
        items = load_signals()
        if resolve_due_signals(items):
            save_signals(items)
    # refresh cache after on-demand result resolution
    with performance_history_cache_lock:
        performance_history_cache.pop(auth["user"], None)
    rows = strategy_learning_summary(auth["user"])
    decided = sum(int(x.get("sample_size") or 0) for x in rows)
    return jsonify({
        "status":"success", "data":rows, "decided_strategy_trades":decided,
        "policy":{
            "minimum_sample":STRATEGY_LEARNING_MIN_SAMPLE,
            "strong_sample":STRATEGY_LEARNING_STRONG_SAMPLE,
            "hard_disable":"60+ trades + smoothed WR <45% + recent-20 WR <50% + max losing streak >=5",
        }
    })




@app.route("/signals/adaptive-stats", methods=["GET"])
def adaptive_stats_endpoint():
    auth_data = {k: request.args.get(k, "") for k in ("key", "user", "device", "session_token")}
    auth, error = _auth_session(auth_data)
    if error: return error
    with signals_lock:
        items=load_signals()
        if resolve_due_signals(items): save_signals(items)
    with performance_history_cache_lock:
        performance_history_cache.pop(auth["user"], None)
    rows=adaptive_context_summary(auth["user"])
    return jsonify({"status":"success","data":rows,"policy":{
        "minimum_sample":ADAPTIVE_CONTEXT_MIN_SAMPLE,"strong_sample":ADAPTIVE_CONTEXT_STRONG_SAMPLE,
        "cooldown_losses":ADAPTIVE_COOLDOWN_LOSSES,
        "hard_block":"40+ pair/timeframe/type trades + smoothed WR <44% + recent20 <45%"
    }})


@app.route("/forex-otc-fallback-data", methods=["POST"])
def forex_otc_fallback_data():
    """
    Authenticated data endpoint used only by the in-app Forex OTC fallback UI.
    Normal RAJA AI scans continue to use /scan and /scan-batch.
    """
    data = request.get_json(silent=True) or {}
    auth, error = _auth_session(data)
    if error:
        return error

    if RAJA_STRICT_BROKER_OTC:
        return jsonify({
            "status": "error",
            "message": "OTC reference mode is active; this strict-mode guard should not be reached.",
            "source_mode": "broker_native_required",
        }), 409

    maintenance = scan_maintenance_state()
    if maintenance:
        return jsonify({
            "status": "error",
            "maintenance": True,
            "message": maintenance.get("maintenance_message") or "RAJA AI scans are temporarily paused.",
        }), 503

    action = str(data.get("action") or "scan").strip().lower()
    selected_expiry = str(data.get("expiry") or "").strip()

    if action == "status":
        pair = str(data.get("pair") or "").strip()
        if pair not in FOREX_OTC_PAIRS:
            pair = FOREX_OTC_PAIRS[0] if FOREX_OTC_PAIRS else ""
        if not pair:
            return jsonify({"status": "error", "message": "No Forex OTC pairs are configured."}), 400

        snapshot = calculate_forex_otc_fallback_snapshot(pair, selected_expiry)
        return jsonify({
            "status": "success",
            "mode": "forex_otc_reference_status",
            "data": snapshot,
            "max_fresh_age_seconds": MAX_SOURCE_CANDLE_AGE_SECONDS,
        })

    requested = data.get("pairs")
    if not isinstance(requested, list):
        requested = [data.get("pair")] if data.get("pair") else []

    pairs = []
    seen = set()
    for raw in requested[:30]:
        pair = str(raw or "").strip()
        if pair in FOREX_OTC_PAIRS and pair not in seen:
            pairs.append(pair)
            seen.add(pair)

    if not pairs:
        return jsonify({"status": "error", "message": "No supported Forex OTC pairs were supplied."}), 400

    results_by_pair = {}
    workers = min(3, len(pairs))
    pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="raja-forex-otc-fallback")
    future_map = {
        pool.submit(calculate_forex_otc_fallback_snapshot, pair, selected_expiry): pair
        for pair in pairs
    }

    done, pending = wait(future_map.keys(), timeout=FOREX_OTC_FALLBACK_DEADLINE_SECONDS)
    for future in done:
        pair = future_map[future]
        try:
            results_by_pair[pair] = future.result()
        except Exception as exc:
            print(f"Forex OTC fallback snapshot error for {pair}: {exc}")
            results_by_pair[pair] = {
                "pair": pair,
                "available": False,
                "live_fresh": False,
                "reason": "Fallback reference analysis failed for this pair.",
                "source": "Yahoo Finance",
                "source_mode": "fallback_reference_only",
            }

    for future in pending:
        pair = future_map[future]
        future.cancel()
        results_by_pair[pair] = {
            "pair": pair,
            "available": False,
            "live_fresh": False,
            "reason": "Fallback reference analysis timed out for this pair.",
            "source": "Yahoo Finance",
            "source_mode": "fallback_reference_only",
        }

    pool.shutdown(wait=False, cancel_futures=True)
    rows = [results_by_pair[pair] for pair in pairs]

    return jsonify({
        "status": "success",
        "mode": "forex_otc_reference_fallback",
        "warning": "REFERENCE-BASED FALLBACK · NOT LIVE BROKER OTC DATA",
        "data": rows,
        "live_restored": any(bool(row.get("live_fresh")) for row in rows),
        "max_fresh_age_seconds": MAX_SOURCE_CANDLE_AGE_SECONDS,
    })




# =========================================================
# RAJA AI CHART SCANNER · V11 VISUAL SK25 ENGINE
# Separate camera/screenshot mode inside the same RAJA AI license session.
# =========================================================
Image.MAX_IMAGE_PIXELS = 25_000_000
RAJA_CHART_SCAN_MAX_UPLOAD = max(2, min(16, int(os.environ.get("RAJA_CHART_SCAN_MAX_MB", "8")))) * 1024 * 1024

def _quality_score(rgb: np.ndarray) -> tuple[float, list[str]]:
    """Estimate screenshot readability without punishing large dark chart backgrounds.

    Broker charts contain broad, intentionally smooth/dark regions. A plain mean-gradient
    metric marks those clean screenshots as "blurred", so this version measures contrast,
    edge density and the stronger part of the gradient distribution instead.
    """
    h, w, _ = rgb.shape
    gray = rgb.astype(np.float32).mean(axis=2)
    contrast = float(np.std(gray))

    gx = np.abs(np.diff(gray, axis=1)) if w > 1 else np.zeros((h, 1), dtype=np.float32)
    gy = np.abs(np.diff(gray, axis=0)) if h > 1 else np.zeros((1, w), dtype=np.float32)
    gradients = np.concatenate([gx.ravel(), gy.ravel()]) if gx.size and gy.size else np.array([0.0], dtype=np.float32)

    # Strong-edge percentile is much better for candlestick screenshots than the mean.
    p85 = float(np.percentile(gradients, 85))
    p95 = float(np.percentile(gradients, 95))
    edge_density = float(np.mean(gradients > 10.0))

    size_score = min(1.0, min(w / 700.0, h / 420.0))
    contrast_score = min(1.0, contrast / 25.0)
    edge_score = min(1.0, (0.45 * p85 + 0.55 * p95) / 12.0)
    density_score = min(1.0, edge_density / 0.05)

    score = 100.0 * (
        0.28 * size_score
        + 0.28 * contrast_score
        + 0.30 * edge_score
        + 0.14 * density_score
    )

    notes: list[str] = []
    if size_score < 0.60:
        notes.append("Image resolution is low; use a clearer full chart screenshot.")
    if contrast_score < 0.35:
        notes.append("Chart contrast is weak; candles may be hard to separate.")
    # Only warn for blur when both edge strength and edge density are genuinely poor.
    if edge_score < 0.20 and density_score < 0.24:
        notes.append("Image looks soft/blurred; hold the camera steady or upload a screenshot.")

    return round(max(0.0, min(100.0, score)), 1), notes


def _group_columns(active: np.ndarray) -> list[tuple[int, int]]:
    groups = []
    start = None
    for i, on in enumerate(active.tolist()):
        if on and start is None:
            start = i
        elif not on and start is not None:
            groups.append((start, i - 1))
            start = None
    if start is not None:
        groups.append((start, len(active) - 1))
    return groups



def _connected_color_components(mask: np.ndarray) -> list[tuple[int, int, int, int, int]]:
    """Return 8-connected component boxes as (left,right,top,bottom,pixels).

    The old V9 counter projected every coloured pixel onto the X axis. On Pocket
    Option mobile screenshots, wide BUY/SELL/sentiment UI bands could make many
    separate candles look like one huge X component. This run-length component
    labeller keeps candles separate in 2D without adding OpenCV/scipy dependencies.
    """
    h, w = mask.shape
    parent: list[int] = []
    runs: list[tuple[int, int, int, int]] = []
    prev: list[tuple[int, int, int]] = []

    def make_label() -> int:
        parent.append(len(parent))
        return len(parent) - 1

    def find(a: int) -> int:
        while parent[a] != a:
            parent[a] = parent[parent[a]]
            a = parent[a]
        return a

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for y in range(h):
        xs = np.flatnonzero(mask[y])
        curr: list[tuple[int, int, int]] = []
        if xs.size:
            run_start = run_last = int(xs[0])
            for xv in xs[1:]:
                x = int(xv)
                if x == run_last + 1:
                    run_last = x
                else:
                    curr.append((run_start, run_last, make_label()))
                    run_start = run_last = x
            curr.append((run_start, run_last, make_label()))

        # Runs are X-sorted. Connect current row to overlapping/adjacent previous runs.
        pi = 0
        for left, right, label in curr:
            while pi < len(prev) and prev[pi][1] < left - 1:
                pi += 1
            pj = pi
            while pj < len(prev) and prev[pj][0] <= right + 1:
                p_left, p_right, p_label = prev[pj]
                if left <= p_right + 1 and right >= p_left - 1:
                    union(label, p_label)
                pj += 1
            runs.append((y, left, right, label))
        prev = curr

    boxes: dict[int, list[int]] = {}
    for y, left, right, label in runs:
        root = find(label)
        box = boxes.setdefault(root, [w, -1, h, -1, 0])
        box[0] = min(box[0], left)
        box[1] = max(box[1], right)
        box[2] = min(box[2], y)
        box[3] = max(box[3], y)
        box[4] += right - left + 1
    return [tuple(v) for v in boxes.values()]


def _regular_candle_run(candles: list[dict[str, Any]], cw: int) -> list[dict[str, Any]]:
    """Keep the densest regularly-spaced candle sequence and discard UI glyphs.

    Broker candles are almost equally spaced horizontally. Price text/icons can also
    be red/green, but they normally appear as tiny duplicate components or as an
    isolated group beyond a large gap. This filter removes those without inventing
    missing candles.
    """
    if len(candles) < 6:
        return candles

    candles = sorted(candles, key=lambda c: float(c["x"]))
    xs = np.array([float(c["x"]) for c in candles], dtype=float)
    gaps = np.diff(xs)
    useful = gaps[(gaps >= max(3.0, cw * 0.008)) & (gaps <= cw * 0.14)]
    if useful.size < 3:
        return candles

    spacing = float(np.median(useful))

    # If two candidates are much closer than the normal candle spacing, they are
    # usually split wick/body fragments or coloured UI text. Keep the stronger one.
    min_gap = max(3.0, spacing * 0.72)
    de_duped: list[dict[str, Any]] = []
    for c in candles:
        if de_duped and float(c["x"]) - float(de_duped[-1]["x"]) < min_gap:
            if float(c.get("pixels") or 0) > float(de_duped[-1].get("pixels") or 0):
                de_duped[-1] = c
        else:
            de_duped.append(c)

    if len(de_duped) < 6:
        return de_duped

    xs = np.array([float(c["x"]) for c in de_duped], dtype=float)
    gaps = np.diff(xs)
    useful = gaps[(gaps >= max(3.0, cw * 0.008)) & (gaps <= cw * 0.14)]
    spacing = float(np.median(useful)) if useful.size else spacing
    split_gap = max(18.0, cw * 0.078, spacing * 1.60)

    runs: list[list[dict[str, Any]]] = []
    run_start = 0
    for i, gap in enumerate(gaps):
        if gap > split_gap:
            runs.append(de_duped[run_start:i + 1])
            run_start = i + 1
    runs.append(de_duped[run_start:])
    runs.sort(
        key=lambda seq: (len(seq), sum(float(c.get("pixels") or 0) for c in seq)),
        reverse=True,
    )
    best = runs[0]
    return best if len(best) >= 4 else de_duped


def _adaptive_candle_color_masks(chart: np.ndarray) -> tuple[np.ndarray, np.ndarray, dict[str, float]]:
    """Return bearish/bullish candle masks with phone/theme adaptive colour thresholds.

    This is deterministic computer vision (not an external ML model): it adapts saturation
    and brightness floors to each frame, then combines hue-like channel dominance with
    the older fixed masks. It is intentionally lightweight for Render and older phones.
    """
    rgb = chart.astype(np.float32)
    r, g, b = rgb[:, :, 0], rgb[:, :, 1], rgb[:, :, 2]
    mx = np.maximum(np.maximum(r, g), b)
    mn = np.minimum(np.minimum(r, g), b)
    chroma = mx - mn
    sat = chroma / np.maximum(mx, 1.0)
    value = mx / 255.0

    bright = value > 0.16
    sat_sample = sat[bright]
    if sat_sample.size:
        sat_floor = float(np.clip(np.percentile(sat_sample, 42), 0.12, 0.24))
    else:
        sat_floor = 0.16

    # Theme/phone adaptive dominance floors. Purple/blue backgrounds are rejected by
    # requiring a clear red or green/cyan dominance plus meaningful saturation.
    red_dom = r - np.maximum(g, b * 0.84)
    green_dom = g - r
    cyan_dom = ((g + b) * 0.5) - r
    red_pos = red_dom[(red_dom > 0) & bright & (sat >= sat_floor)]
    green_pos = green_dom[(green_dom > 0) & bright & (sat >= sat_floor * 0.75)]
    red_floor = float(np.clip(np.percentile(red_pos, 35), 8, 18)) if red_pos.size else 11.0
    green_floor = float(np.clip(np.percentile(green_pos, 30), 5, 14)) if green_pos.size else 7.0

    adaptive_red = (value >= 0.22) & (sat >= sat_floor) & (red_dom >= red_floor) & (r >= g + 5)
    adaptive_green = (value >= 0.19) & (sat >= sat_floor * 0.72) & (green_dom >= green_floor) & (g >= b - 58)
    adaptive_cyan = (value >= 0.22) & (sat >= sat_floor * 0.65) & (cyan_dom >= 10) & (g >= r + 6) & (b >= r - 8)

    # Preserve proven V9/V10 masks as a fallback for clean screenshots.
    fixed_red = (r > 84) & ((r - g) > 16) & ((r - b) > 3)
    fixed_green = (g > 64) & ((g - r) > 10) & ((g - b) > -42)
    fixed_cyan = (g > 88) & (b > 88) & (r < 185) & (((g + b) - 2 * r) > 24)

    red = adaptive_red | fixed_red
    bull = adaptive_green | adaptive_cyan | fixed_green | fixed_cyan
    # Pixels that accidentally satisfy both are ambiguous and are discarded.
    overlap = red & bull
    if overlap.any():
        red = red & ~overlap
        bull = bull & ~overlap

    meta = {
        "sat_floor": round(sat_floor, 3),
        "red_floor": round(red_floor, 1),
        "green_floor": round(green_floor, 1),
        "red_density": round(float(red.mean()), 6),
        "bull_density": round(float(bull.mean()), 6),
    }
    return red, bull, meta


def _detect_candles_in_chart(chart: np.ndarray) -> tuple[list[dict[str, Any]], float, list[str], float, float]:
    """Detect red/green candles with mobile-safe 2D component clustering.

    V9.1 fixes the V9 mobile under-count where 14+ visible Pocket Option candles
    could be reported as 7 because the old X-axis grouping was contaminated by
    wide coloured interface bars. No indicator values are used; geometry remains
    visual-only body/wick estimation.
    """
    ch, cw, _ = chart.shape
    quality, quality_notes = _quality_score(chart)

    # V11 Adaptive Vision Lens: per-frame red/green/cyan calibration.
    red, bull, color_meta = _adaptive_candle_color_masks(chart)
    colored = red | bull

    # Suppress only near-full-width coloured UI bands. 2D components already make
    # normal BUY/SELL buttons harmless because their width is rejected below.
    row_counts = colored.sum(axis=1)
    broad_rows = row_counts > max(80, int(cw * 0.52))
    clean_red = red.copy()
    clean_bull = bull.copy()
    clean_colored = colored.copy()
    if broad_rows.any():
        clean_red[broad_rows, :] = False
        clean_bull[broad_rows, :] = False
        clean_colored[broad_rows, :] = False

    min_pixels = max(14, min(70, int(ch * cw * 0.00020)))
    max_width = max(32, int(cw * 0.080))
    max_height = max(55, int(ch * 0.50))
    seeds: list[dict[str, Any]] = []

    # Label bullish and bearish colours separately so adjacent opposite candles do
    # not merge into one component when their anti-aliased edges touch.
    for direction, mask in ((-1, clean_red), (1, clean_bull)):
        for left, right, top, bottom, pixels in _connected_color_components(mask):
            width = right - left + 1
            height = bottom - top + 1
            if pixels < min_pixels or width > max_width or height < 4 or height > max_height:
                continue
            # Reject flat coloured labels; doji/small bodies are still allowed.
            if width > max(12, int(cw * 0.040)) and height < max(5, int(width * 0.20)):
                continue
            density = float(pixels / max(1, width * height))
            if density < 0.045:
                continue
            seeds.append({
                "left": int(left), "right": int(right), "top": int(top), "bottom": int(bottom),
                "pixels": int(pixels), "dir": int(direction),
            })

    seeds.sort(key=lambda s: (s["left"] + s["right"]) / 2.0)

    # Merge/replace only components centered at effectively the same X position.
    same_x = max(3, int(cw * 0.009))
    de_duped_seeds: list[dict[str, Any]] = []
    for seed in seeds:
        cx = (seed["left"] + seed["right"]) / 2.0
        if de_duped_seeds:
            prev = de_duped_seeds[-1]
            pcx = (prev["left"] + prev["right"]) / 2.0
            if abs(cx - pcx) <= same_x:
                if seed["pixels"] > prev["pixels"]:
                    de_duped_seeds[-1] = seed
                continue
        de_duped_seeds.append(seed)

    candles: list[dict[str, Any]] = []
    for seed in de_duped_seeds:
        left, right = seed["left"], seed["right"]
        top, bottom = seed["top"], seed["bottom"]
        direction = int(seed["dir"])
        width = right - left + 1
        height = bottom - top + 1

        # Expand only locally to recover a wick that may be a thin/disconnected
        # anti-aliased line. Never search the whole column, which could capture UI.
        xpad = max(1, min(3, width // 4))
        ypad = max(5, min(int(ch * 0.07), int(height * 0.65)))
        xl, xr = max(0, left - xpad), min(cw - 1, right + xpad)
        yt, yb = max(0, top - ypad), min(ch - 1, bottom + ypad)
        local = clean_colored[yt:yb + 1, xl:xr + 1]
        ys, _ = np.where(local)
        if ys.size:
            full_top = yt + int(ys.min())
            full_bottom = yt + int(ys.max())
        else:
            full_top, full_bottom = top, bottom

        full_height = max(1, full_bottom - full_top + 1)
        own_mask = clean_bull if direction > 0 else clean_red
        body_block = own_mask[full_top:full_bottom + 1, left:right + 1]
        row_counts_body = body_block.sum(axis=1).astype(np.int32)
        max_row = int(row_counts_body.max()) if row_counts_body.size else 0
        if max_row >= 2:
            body_thr = max(2, int(math.ceil(max_row * 0.50)))
            body_rows = np.where(row_counts_body >= body_thr)[0]
        else:
            body_rows = np.array([], dtype=int)

        if body_rows.size:
            body_top = full_top + int(body_rows.min())
            body_bottom = full_top + int(body_rows.max())
        else:
            body_top, body_bottom = top, bottom

        body_height = max(1, body_bottom - body_top + 1)
        upper_wick = max(0, body_top - full_top)
        lower_wick = max(0, full_bottom - body_bottom)
        range_px = float(max(1, full_height))
        body_ratio = float(body_height / range_px)
        open_y = float(body_bottom if direction > 0 else body_top)
        close_y = float(body_top if direction > 0 else body_bottom)

        candles.append({
            "x": float((left + right) / 2.0),
            "y": float((body_top + body_bottom) / 2.0),
            "top": int(full_top), "bottom": int(full_bottom),
            "body_top": int(body_top), "body_bottom": int(body_bottom),
            "body_height": float(body_height),
            "upper_wick": float(upper_wick), "lower_wick": float(lower_wick),
            "body_ratio": body_ratio, "open_y": open_y, "close_y": close_y,
            "dir": direction, "pixels": int(seed["pixels"]), "range": range_px,
        })

    candles = _regular_candle_run(candles, cw)[-80:]
    if len(candles) >= 2:
        span = float((candles[-1]["x"] - candles[0]["x"]) / max(cw, 1))
    else:
        span = 0.0
    density = float(colored.mean())
    return candles, quality, quality_notes, max(0.0, span), density

def _candidate_chart_regions_with_bounds(
    arr: np.ndarray,
) -> list[tuple[str, np.ndarray, tuple[float, float, float, float]]]:
    """Return chart candidates plus normalized full-frame bounds.

    The normalized bounds are reused by the V42 preflight endpoint to draw a
    proposed crop on the exact frame shown in the browser.  Signal analysis keeps
    using the same candidate list, so auto-detect does not change SK25 rules.
    """
    h, w, _ = arr.shape
    portrait = h > w * 1.12
    specs: list[tuple[str, float, float, float, float]]
    if portrait:
        specs = [
            ("mobile-chart-tight", 0.01, 0.99, 0.12, 0.64),
            ("mobile-chart-mid", 0.01, 0.99, 0.10, 0.68),
            ("mobile-chart-core", 0.03, 0.97, 0.14, 0.70),
            ("mobile-upper", 0.01, 0.99, 0.10, 0.72),
            ("mobile-middle", 0.01, 0.99, 0.18, 0.84),
            ("mobile-lower", 0.01, 0.99, 0.28, 0.96),
            ("mobile-wide", 0.01, 0.99, 0.08, 0.94),
            ("mobile-center", 0.05, 0.95, 0.14, 0.90),
        ]
    else:
        specs = [
            ("desktop-main", 0.035, 0.86, 0.24, 0.94),
            ("desktop-wide", 0.02, 0.94, 0.18, 0.94),
            ("desktop-center", 0.05, 0.90, 0.12, 0.90),
            ("desktop-fullchart", 0.01, 0.99, 0.16, 0.96),
        ]

    out: list[tuple[str, np.ndarray, tuple[float, float, float, float]]] = []
    for name, xa, xb, ya, yb in specs:
        x1, x2 = int(w * xa), int(w * xb)
        y1, y2 = int(h * ya), int(h * yb)
        if x2 - x1 >= 180 and y2 - y1 >= 140:
            out.append((name, arr[y1:y2, x1:x2], (xa, ya, xb - xa, yb - ya)))
    return out


def _candidate_chart_regions(arr: np.ndarray) -> list[tuple[str, np.ndarray]]:
    """Compatibility wrapper for the existing visual signal analyzer."""
    return [(name, crop) for name, crop, _bounds in _candidate_chart_regions_with_bounds(arr)]


def _wide_colored_overlay_count(chart: np.ndarray) -> int:
    """Count likely indicator/overlay lines without treating one price line as an indicator.

    This is only a preflight warning.  It never generates a direction.  Two or more
    long, thin saturated components usually mean an indicator/annotation overlay;
    one line is allowed because brokers commonly draw the current-price line.
    """
    h, w, _ = chart.shape
    red, bull, _meta = _adaptive_candle_color_masks(chart)
    wide = 0
    for mask in (red, bull):
        for left, right, top, bottom, pixels in _connected_color_components(mask):
            width = right - left + 1
            height = bottom - top + 1
            if (
                width >= max(70, int(w * 0.22))
                and height <= max(10, int(h * 0.055))
                and pixels >= max(45, int(width * 0.28))
            ):
                wide += 1
    return wide


def _preflight_roi_from_candles(
    candles: list[dict[str, Any]],
    chart_shape: tuple[int, ...],
    candidate_bounds: tuple[float, float, float, float],
) -> tuple[dict[str, float], int]:
    """Build a stable newest-30-candle crop in full-frame normalized coordinates."""
    ch, cw = int(chart_shape[0]), int(chart_shape[1])
    selected = sorted(candles, key=lambda c: float(c.get("x") or 0.0))[-30:]
    if not selected:
        return {}, 0

    xs = np.array([float(c.get("x") or 0.0) for c in selected], dtype=float)
    tops = np.array([float(c.get("top") or 0.0) for c in selected], dtype=float)
    bottoms = np.array([float(c.get("bottom") or 0.0) for c in selected], dtype=float)
    gaps = np.diff(xs)
    useful = gaps[gaps > 2.0]
    spacing = float(np.median(useful)) if useful.size else max(8.0, cw * 0.035)

    x1 = max(0.0, float(xs.min()) - spacing * 1.35)
    x2 = min(float(cw), float(xs.max()) + spacing * 1.65)
    raw_y1 = max(0.0, float(tops.min()))
    raw_y2 = min(float(ch), float(bottoms.max()))
    candle_height = max(1.0, raw_y2 - raw_y1)
    y_pad = max(ch * 0.08, candle_height * 0.30)
    y1 = max(0.0, raw_y1 - y_pad)
    y2 = min(float(ch), raw_y2 + y_pad)

    # Preserve enough vertical context for wicks/SR while still excluding broker UI.
    min_h = ch * 0.42
    if y2 - y1 < min_h:
        centre = (y1 + y2) / 2.0
        y1 = max(0.0, centre - min_h / 2.0)
        y2 = min(float(ch), y1 + min_h)
        y1 = max(0.0, y2 - min_h)

    xa, ya, wa, ha = candidate_bounds
    roi = {
        "x": max(0.0, min(1.0, xa + (x1 / max(cw, 1)) * wa)),
        "y": max(0.0, min(1.0, ya + (y1 / max(ch, 1)) * ha)),
        "w": max(0.0, min(1.0, ((x2 - x1) / max(cw, 1)) * wa)),
        "h": max(0.0, min(1.0, ((y2 - y1) / max(ch, 1)) * ha)),
    }
    roi["w"] = min(1.0 - roi["x"], max(0.12, roi["w"]))
    roi["h"] = min(1.0 - roi["y"], max(0.18, roi["h"]))
    return {key: round(float(value), 6) for key, value in roi.items()}, len(selected)


def analyze_chart_preflight(raw: bytes, *, already_cropped: bool = False) -> dict[str, Any]:
    """Auto-detect/test a candle crop without running strategies or creating a signal."""
    try:
        image = Image.open(io.BytesIO(raw))
        image = ImageOps.exif_transpose(image).convert("RGB")
    except (UnidentifiedImageError, OSError, ValueError) as exc:
        raise ValueError("Image could not be opened. Use a clear PNG/JPG chart frame.") from exc

    original_w, original_h = image.size
    if original_w < 240 or original_h < 180:
        raise ValueError("Frame is too small. Use a clearer chart frame.")

    scale = min(1.0, 1600.0 / max(original_w, original_h))
    if scale < 1.0:
        image = image.resize(
            (max(1, int(original_w * scale)), max(1, int(original_h * scale))),
            Image.Resampling.LANCZOS,
        )
    arr = np.asarray(image, dtype=np.uint8)

    if already_cropped:
        candidates = [("confirmed-crop", arr, (0.0, 0.0, 1.0, 1.0))]
    else:
        candidates = _candidate_chart_regions_with_bounds(arr)
        candidates.append(("full-frame-fallback", arr, (0.0, 0.0, 1.0, 1.0)))

    best: tuple[str, np.ndarray, tuple[float, float, float, float], list[dict[str, Any]], float, list[str], float, float] | None = None
    best_score = -1e9
    for name, candidate, bounds in candidates:
        candles, quality, quality_notes, span, density = _detect_candles_in_chart(candidate)
        score = (
            min(len(candles), 45) * 4.0
            + min(1.0, span / 0.55) * 36.0
            + min(16.0, density * 850.0)
            + quality * 0.14
        )
        if len(candles) < 6:
            score -= 24.0
        if name == "full-frame-fallback":
            score -= 8.0
        if score > best_score:
            best_score = score
            best = (name, candidate, bounds, candles, quality, quality_notes, span, density)

    if best is None:
        raise ValueError("No readable chart region was found.")

    name, chart, bounds, candles, quality, quality_notes, span, _density = best
    detected = len(candles)
    roi, selected_count = _preflight_roi_from_candles(candles, chart.shape, bounds)
    wide_overlays = _wide_colored_overlay_count(chart)
    possible_indicator = wide_overlays >= 2
    warnings = list(quality_notes)
    guidance = "READY"
    gate = "PASS"

    if possible_indicator:
        gate = "RESCAN"
        guidance = "REMOVE INDICATORS"
        warnings.insert(0, "REMOVE INDICATORS: multiple wide coloured overlay lines were detected.")
    elif detected < MIN_SIGNAL_CANDLES:
        gate = "RESCAN"
        guidance = "ZOOM OUT"
        warnings.insert(0, f"Only {detected} candles detected; show at least {MIN_SIGNAL_CANDLES} complete candles.")
    elif quality < MIN_SIGNAL_IMAGE_QUALITY:
        gate = "RESCAN"
        guidance = "CLEARER FRAME"
        warnings.insert(0, f"Frame quality {quality:.0f}/100 is below {MIN_SIGNAL_IMAGE_QUALITY:.0f}/100.")
    elif detected < 20:
        guidance = "ZOOM OUT SLIGHTLY"
        warnings.insert(0, f"{detected} candles pass the minimum; 20–30 is the ideal view.")
    elif detected > 30:
        guidance = "ZOOM IN SLIGHTLY"
        warnings.insert(0, f"{detected} candles detected; the proposed crop keeps the newest 30.")

    auto_detected = bool(roi) and detected >= 6 and span >= 0.10
    if not auto_detected:
        gate = "RESCAN"
        guidance = "ZOOM OUT" if detected < MIN_SIGNAL_CANDLES else "SET AREA MANUALLY"
        warnings.insert(0, "Automatic chart-area detection was not reliable; set the candle area manually.")

    xa, _ya, wa, _ha = bounds
    sidebars_excluded = not already_cropped and (xa > 0.025 or xa + wa < 0.96)
    return {
        "mode": "TEST_FRAME" if already_cropped else "AUTO_DETECT",
        "auto_detected": auto_detected,
        "roi": roi if not already_cropped else {},
        "crop_name": name,
        "detected_candles": int(detected),
        "selected_candles": int(selected_count),
        "minimum_candles": int(MIN_SIGNAL_CANDLES),
        "ideal_candles": "20-30",
        "image_quality_score": round(float(quality), 1),
        "scan_gate": gate,
        "zoom_guidance": guidance,
        "possible_indicator_overlay": possible_indicator,
        "wide_overlay_count": int(wide_overlays),
        "sidebars_excluded": bool(sidebars_excluded),
        "warnings": warnings[:6],
        "strategy_scan_performed": False,
        "signal_generated": False,
    }

def analyze_chart_image(raw: bytes, timeframe: str = "1m", market: str = "", last_outcome: str = "", *, captured_at_close: bool = False) -> dict[str, Any]:
    """Strict Pattern Type 1-25 closed-candle chart scanner.

    The engine evaluates RAJA Pattern Type 1-25 using visible candle body/wick
    geometry plus S/R/trend context.
    No RSI/EMA/MACD/Stochastic/Bollinger values are used for directional signals.
    """
    try:
        image = Image.open(io.BytesIO(raw))
        image = ImageOps.exif_transpose(image).convert("RGB")
    except (UnidentifiedImageError, OSError, ValueError) as exc:
        raise ValueError("Image could not be opened. Upload a PNG/JPG chart screenshot.") from exc

    w0, h0 = image.size
    if w0 < 240 or h0 < 180:
        raise ValueError("Image is too small. Use a clearer chart screenshot.")

    tf = str(timeframe or "1m").strip().lower()
    market_name = str(market or "").strip()
    previous_outcome = str(last_outcome or "").strip().upper()

    max_dim = 1800.0 if h0 > w0 * 1.12 else 1600.0
    scale = min(1.0, max_dim / max(w0, h0))
    if scale < 1.0:
        image = image.resize((max(1, int(w0 * scale)), max(1, int(h0 * scale))), Image.Resampling.LANCZOS)

    # Keep the V9.4 AI Lens server fallback: original + one gentle colour recovery.
    image_variants: list[tuple[str, Image.Image]] = [("raw", image)]
    try:
        enhanced = ImageOps.autocontrast(image, cutoff=1)
        enhanced = ImageEnhance.Color(enhanced).enhance(1.18)
        enhanced = ImageEnhance.Contrast(enhanced).enhance(1.07)
        enhanced = ImageEnhance.Sharpness(enhanced).enhance(1.08)
        image_variants.append(("ai-lens", enhanced))
    except Exception:
        pass

    best_region = None
    best_region_score = -1e9
    fallback_arr = np.asarray(image, dtype=np.uint8)
    for variant_name, variant_image in image_variants:
        variant_arr = np.asarray(variant_image, dtype=np.uint8)
        for crop_name, candidate in _candidate_chart_regions(variant_arr):
            cands, q, qnotes, span, density = _detect_candles_in_chart(candidate)
            score = min(len(cands), 60) * 3.2 + min(1.0, span / 0.55) * 34.0 + min(18.0, density * 900.0) + q * 0.16
            if len(cands) < 6:
                score -= 22.0
            if variant_name == "raw":
                score += 0.6
            if score > best_region_score:
                best_region_score = score
                best_region = (f"{variant_name}:{crop_name}", candidate, cands, q, qnotes)

    if best_region is None:
        crop_name, chart = "raw:full-image", fallback_arr
        candles, quality, quality_notes, _, _ = _detect_candles_in_chart(chart)
    else:
        crop_name, chart, candles, quality, quality_notes = best_region

    ch, cw, _ = chart.shape
    detected_count = len(candles)
    warnings = list(quality_notes)
    reasons: list[str] = []
    library = "RAJA SK25 Pattern Type 1-25"

    # V11 Closed Candle Lock. A normal screenshot/live-now frame can contain a
    # still-forming rightmost candle, so it is excluded from setup matching. A
    # frame captured exactly at a candle boundary may include a newborn next candle;
    # a tiny-range heuristic removes that newborn while preserving the just-closed one.
    forming_candle_excluded = False
    newborn_candle_excluded = False
    observed_latest_direction = "UNKNOWN"
    if candles:
        observed_latest_direction = "GREEN" if candles[-1]["dir"] > 0 else "RED"
    if len(candles) >= 2:
        if not captured_at_close:
            candles = candles[:-1]
            forming_candle_excluded = True
        elif len(candles) >= 4:
            prior_ranges = np.array([float(c["range"]) for c in candles[-8:-1]], dtype=float)
            prior_bodies = np.array([float(c["body_height"]) for c in candles[-8:-1]], dtype=float)
            if prior_ranges.size and prior_bodies.size:
                last = candles[-1]
                tiny_newborn = (float(last["range"]) <= float(np.median(prior_ranges)) * 0.34
                                and float(last["body_height"]) <= max(2.0, float(np.median(prior_bodies)) * 0.48))
                if tiny_newborn:
                    candles = candles[:-1]
                    newborn_candle_excluded = True

    count = len(candles)

    def legacy_aliases(pattern: str, direction: str, score: float, signals: list[dict[str, Any]], size: int = SK25_PATTERN_LIBRARY_SIZE) -> dict[str, Any]:
        return {
            "selected_strategy": pattern,
            "strategy_direction": direction,
            "strategy_score": score,
            "strategy_signals": signals,
            "strategy_library": library,
            "strategy_library_size": size,
        }

    if count < 6:
        warnings.append("Not enough candle structure was detected. Move closer to the chart and keep candles sharp.")
        return {
            "bias": "NO TRADE", "confidence": 0.0, "image_quality_score": quality,
            "detected_candles": detected_count, "closed_candles_analyzed": count, "visual_trend": "UNREADABLE", "momentum": "PATTERN TYPE 1-25 ONLY", "volatility": "NOT USED",
            "selected_pattern": "NO ACTIVE STRATEGY SETUP", "pattern_direction": "NONE", "pattern_score": 0.0,
            "pattern_signals": [], "pattern_library": library, "pattern_library_size": SK25_PATTERN_LIBRARY_SIZE,
            "confluence_count": 0, "setup_quality": "LOW", "next_candle_color": "NONE",
            "entry_instruction": "WAIT FOR A COMPLETE SETUP", "recovery_trade": False,
            "latest_candle_direction": "UNKNOWN",
            "reasons": ["Insufficient readable candle structure for Pattern Type 1-25 recognition."], "warnings": warnings,
            "pattern_status": {"Candle geometry": "Unreadable", "Pattern library": "Pattern Type 1-25"},
            "engine": "RAJA V42 · Strict SK25 + Adaptive Vision + Closed Candle Lock", "analysis_crop_mode": crop_name,
            "timing_verified": bool(captured_at_close), "forming_candle_excluded": forming_candle_excluded, "newborn_candle_excluded": newborn_candle_excluded,
            **legacy_aliases("NO ACTIVE STRATEGY SETUP", "NONE", 0.0, []),
        }

    ranges = np.array([float(c["range"]) for c in candles], dtype=float)
    bodies = np.array([float(c["body_height"]) for c in candles], dtype=float)
    med_range = float(np.median(ranges[-min(count, 24):])) if count else 8.0
    med_body = float(np.median(bodies[-min(count, 24):])) if count else 5.0
    tol = max(2.0, med_range * 0.18)

    def trend_before(end_idx: int, lookback: int = 7) -> float:
        start = max(0, end_idx - lookback)
        seq0 = candles[start:end_idx]
        if len(seq0) < 3:
            return 0.0
        yv = np.array([c["y"] for c in seq0], dtype=float) / max(ch, 1)
        xv = np.arange(len(seq0), dtype=float)
        slope = float(np.polyfit(xv, yv, 1)[0]) if len(seq0) > 1 else 0.0
        return float(np.clip(-slope * 18.0, -1.0, 1.0))

    def seq_is(seq0: list[dict[str, Any]], dirs0: list[int]) -> bool:
        return len(seq0) == len(dirs0) and all(int(c["dir"]) == d for c, d in zip(seq0, dirs0))

    def is_normal(c: dict[str, Any]) -> bool:
        # "Normal body" in the source is visual, not a fixed percentage. Keep
        # this tolerant because phone anti-aliasing can make a thin wick merge
        # into the detected body. Sequence/level rules still provide the guard.
        bh = float(c["body_height"])
        return float(c["body_ratio"]) >= 0.28 and bh >= max(2.0, med_body * 0.45) and bh <= med_body * 2.20

    def is_small(c: dict[str, Any]) -> bool:
        return float(c["body_ratio"]) <= 0.30 or float(c["body_height"]) <= max(2.0, med_body * 0.52)

    def is_long(c: dict[str, Any]) -> bool:
        return float(c["body_ratio"]) >= 0.66 and float(c["body_height"]) >= max(4.0, med_body * 1.28)

    def is_marubozu(c: dict[str, Any]) -> bool:
        return is_long(c) and (float(c["upper_wick"]) + float(c["lower_wick"])) <= max(3.0, float(c["body_height"]) * 0.42)

    def long_lower(c: dict[str, Any]) -> bool:
        return float(c["lower_wick"]) >= max(3.0, float(c["body_height"]) * 0.68, med_range * 0.20)

    def long_upper(c: dict[str, Any]) -> bool:
        return float(c["upper_wick"]) >= max(3.0, float(c["body_height"]) * 0.68, med_range * 0.20)

    def close_breaks_above(c: dict[str, Any], level_y: float, margin: float = 0.22) -> bool:
        return float(c["close_y"]) < float(level_y) - tol * margin

    def close_breaks_below(c: dict[str, Any], level_y: float, margin: float = 0.22) -> bool:
        return float(c["close_y"]) > float(level_y) + tol * margin

    def body_inside(inner: dict[str, Any], outer: dict[str, Any], extra: float = 0.0) -> bool:
        return float(inner["body_top"]) >= float(outer["body_top"]) - extra and float(inner["body_bottom"]) <= float(outer["body_bottom"]) + extra

    def strongest_level_cluster(values: list[float], cluster_tol: float, prefer: str) -> tuple[float | None, int]:
        """Cluster nearby visual highs/lows so S/R rules use repeated levels, not one pixel."""
        if not values:
            return None, 0
        vals = sorted(float(v) for v in values)
        clusters: list[list[float]] = []
        for v in vals:
            placed = False
            for cl in clusters:
                center = float(sum(cl) / len(cl))
                if abs(v - center) <= cluster_tol:
                    cl.append(v); placed = True; break
            if not placed:
                clusters.append([v])
        clusters.sort(key=lambda cl: (len(cl), -abs(sum(cl) / len(cl))), reverse=True)
        max_n = max(len(cl) for cl in clusters)
        strongest = [cl for cl in clusters if len(cl) == max_n]
        if prefer == "resistance":
            chosen = min(strongest, key=lambda cl: sum(cl) / len(cl))  # smaller y = higher price
        else:
            chosen = max(strongest, key=lambda cl: sum(cl) / len(cl))  # larger y = lower price
        return float(sum(chosen) / len(chosen)), len(chosen)

    is_otc = "OTC" in market_name.upper()
    is_live = "LIVE" in market_name.upper()
    global_trend = trend_before(len(candles), min(10, count))
    context_label = "UPTREND" if global_trend > 0.13 else "DOWNTREND" if global_trend < -0.13 else "SIDEWAYS/MIXED"

    exact: list[dict[str, Any]] = []
    near: list[dict[str, Any]] = []

    def add_setup(type_no: int, direction: int, rules: list[tuple[str, bool]], setup: str, why: str,
                  *, family: str = "Candle Sequence", recovery: bool = False, timeframe_rule: str = "ANY") -> None:
        if int(type_no) not in RAJA_ACTIVE_STRATEGY_IDS:
            return
        matched = sum(1 for _, ok in rules if ok)
        total = max(1, len(rules))
        pct = round(100.0 * matched / total, 1)
        item = {
            "name": RAJA_STRATEGY_NAMES.get(int(type_no), f"RAJA Strategy {type_no}"),
            "priority": RAJA_STRATEGY_PRIORITIES.get(int(type_no), 100),
            "pattern_type": type_no,
            "direction": "UP" if direction > 0 else "DOWN",
            "next_candle": "GREEN" if direction > 0 else "RED",
            "score": pct,
            "why": why,
            "setup": setup,
            "family": family,
            "rules_matched": matched,
            "rules_total": total,
            "rules": [{"name": name, "ok": bool(ok)} for name, ok in rules],
            "recovery_trade": bool(recovery),
            "timeframe_rule": timeframe_rule,
        }
        if matched == total:
            exact.append(item)
        elif pct >= 50.0:
            near.append(item)

    # TYPE 1 - OTC continuation with exhaustion/spike guard.
    if count >= 8:
        last8 = candles[-8:]
        tail = last8[-1]
        green_exhaustion_ok = is_normal(tail) and not long_upper(tail) and float(tail["body_height"]) <= med_body * 1.80
        red_exhaustion_ok = is_normal(tail) and not long_lower(tail) and float(tail["body_height"]) <= med_body * 1.80
        add_setup(1, 1, [("OTC market", is_otc), ("8 back-to-back GREEN candles", all(c["dir"] > 0 for c in last8)), ("8th GREEN is normal, not an exhaustion spike", green_exhaustion_ok)],
                  "8 GREEN candles in OTC + exhaustion guard", "Continuation is accepted only while the 8th candle remains structurally healthy.", timeframe_rule="OTC ONLY")
        add_setup(1, -1, [("OTC market", is_otc), ("8 back-to-back RED candles", all(c["dir"] < 0 for c in last8)), ("8th RED is normal, not an exhaustion spike", red_exhaustion_ok)],
                  "8 RED candles in OTC + exhaustion guard", "Continuation is accepted only while the 8th candle remains structurally healthy.", timeframe_rule="OTC ONLY")

    # TYPE 2 - 2 green + confirmed red rejection at respected resistance.
    if count >= 3:
        a, b, c = candles[-3:]
        resistance_touch = abs(float(a["top"]) - float(b["top"])) <= tol * 1.55
        midpoint = (float(b["body_top"]) + float(b["body_bottom"])) / 2.0
        reversal = c["dir"] < 0 and float(c["close_y"]) > midpoint and (long_upper(c) or float(c["body_height"]) >= med_body * 0.55)
        add_setup(2, -1, [("GREEN, GREEN, RED setup", seq_is([a,b,c],[1,1,-1])), ("Recent highs respect one resistance area", resistance_touch), ("RED closes through prior GREEN midpoint / rejects high", reversal)],
                  "2 GREEN + confirmed RED rejection at resistance", "The resistance reversal requires a meaningful rejection close before targeting RED.", family="Resistance")

    # TYPE 3 - downside wick sweep must reclaim the swept visual lows.
    if count >= 3:
        a, b, c = candles[-3:]
        sweep_level = max(float(a["bottom"]), float(b["bottom"]))
        wick_break_down = c["dir"] > 0 and float(c["bottom"]) > sweep_level + tol * 0.20 and long_lower(c)
        wick_reclaim = float(c["close_y"]) < sweep_level - tol * 0.05
        add_setup(3, -1, [("GREEN, RED, GREEN setup", seq_is([a,b,c],[1,-1,1])), ("3rd GREEN wick sweeps below prior lows", wick_break_down), ("3rd GREEN closes back above swept lows", wick_reclaim), ("Sideways/mixed context", abs(global_trend) < 0.60)],
                  "GREEN - RED - GREEN sweep + reclaim", "The third green candle must sweep and reclaim the prior lows before the next-candle RED target is allowed.", family="Sideways")

    # TYPE 4 - long-tail rejection must occur near a recent visual support extreme.
    if count >= 2:
        a, b = candles[-2:]
        tail_vs_head = float(a["lower_wick"]) > max(float(b["upper_wick"]) * 1.12, med_range * 0.18)
        prior_support = max((float(x["bottom"]) for x in candles[-10:-2]), default=float(a["bottom"]))
        support_rejection = float(a["bottom"]) >= prior_support - tol * 1.20 and float(a["close_y"]) <= prior_support + tol * 0.20
        add_setup(4, 1, [("RED then GREEN", seq_is([a,b],[-1,1])), ("1st RED tail is long", long_lower(a)), ("RED tail longer than GREEN head", tail_vs_head), ("Long tail rejects recent support area", support_rejection)],
                  "RED long-tail support rejection + GREEN", "The long wick must reject a meaningful support extreme before targeting the next GREEN candle.")

    # TYPE 5 - R,R with long tails; 2nd red head does not break 1st; then G -> next R.
    if count >= 3:
        a, b, c = candles[-3:]
        no_head_break = float(b["top"]) >= float(a["top"]) - tol * 0.35
        add_setup(5, -1, [("RED, RED, GREEN setup", seq_is([a,b,c],[-1,-1,1])), ("First two RED tails are long", long_lower(a) and long_lower(b)), ("2nd RED head does not break 1st RED", no_head_break), ("Sideways/mixed context", abs(global_trend) < 0.68)],
                  "2 long-tail RED + GREEN", "The two red candles keep the required wick/level structure and a green setup candle follows; the strategy targets the next candle RED.", family="Sideways")

    # TYPE 6 - previous loss is only a context flag; bearish structure must still qualify.
    if count >= 3:
        a, b, c = candles[-3:]
        add_setup(6, -1, [("Previous trade marked LOSS", previous_outcome == "LOSS"), ("RED, GREEN, RED setup", seq_is([a,b,c],[-1,1,-1])), ("Bearish context still present", global_trend < -0.08), ("Final RED body is normal", is_normal(c))],
                  "Recovery: RED - GREEN - RED + bearish context", "A previous loss never creates the edge by itself; the current bearish structure must still qualify. No stake increase is implied.", family="Recovery", recovery=True, timeframe_rule="AFTER LOSS ONLY")

    # TYPE 7 - R,G,G reversal requires a recent upper extreme.
    if count >= 3:
        a, b, c = candles[-3:]
        prior = candles[-10:-3] if count >= 10 else candles[:-3]
        prior_res = min((float(x["top"]) for x in prior), default=min(float(b["top"]), float(c["top"])))
        at_res = min(float(b["top"]), float(c["top"])) <= prior_res + tol * 1.10
        add_setup(7, -1, [("RED, GREEN, GREEN setup", seq_is([a,b,c],[-1,1,1])), ("Two GREEN candles have normal bodies", is_normal(b) and is_normal(c)), ("GREEN push reaches recent resistance / upper extreme", at_res)],
                  "RED + 2 normal GREEN at resistance", "The two-green push must reach a meaningful upper extreme before targeting RED.")

    # TYPE 8 - G,R,R reversal requires a recent lower extreme.
    if count >= 3:
        a, b, c = candles[-3:]
        prior = candles[-10:-3] if count >= 10 else candles[:-3]
        prior_sup = max((float(x["bottom"]) for x in prior), default=max(float(b["bottom"]), float(c["bottom"])))
        at_sup = max(float(b["bottom"]), float(c["bottom"])) >= prior_sup - tol * 1.10
        add_setup(8, 1, [("GREEN, RED, RED setup", seq_is([a,b,c],[1,-1,-1])), ("Two RED candles have normal bodies", is_normal(b) and is_normal(c)), ("RED push reaches recent support / lower extreme", at_sup)],
                  "GREEN + 2 normal RED at support", "The two-red push must reach a meaningful lower extreme before targeting GREEN.")

    # TYPE 9 - bullish continuation requires trend alignment.
    if count >= 4:
        a,b,c,d = candles[-4:]
        add_setup(9, 1, [("3 GREEN + 1 RED setup", seq_is([a,b,c,d],[1,1,1,-1])), ("Opposite RED has long tail", long_lower(d)), ("Broader visual trend is bullish", global_trend > 0.08)],
                  "GREEN, GREEN, GREEN + long-tail RED in bullish context", "The continuation signal is allowed only when the broader visual trend agrees.")

    # TYPE 10 - bearish continuation requires trend alignment.
    if count >= 4:
        a,b,c,d = candles[-4:]
        add_setup(10, -1, [("3 RED + 1 GREEN setup", seq_is([a,b,c,d],[-1,-1,-1,1])), ("Opposite GREEN has long head", long_upper(d)), ("Broader visual trend is bearish", global_trend < -0.08)],
                  "RED, RED, RED + long-head GREEN in bearish context", "The continuation signal is allowed only when the broader visual trend agrees.")

    # TYPE 11 - 30s only: 3 normal red + green that does not break prior 3 -> next green.
    if count >= 4:
        a,b,c,d = candles[-4:]
        prior_high = min(float(x["top"]) for x in (a,b,c))
        no_break = float(d["top"]) >= prior_high - tol * 0.35
        add_setup(11, 1, [("30-second timeframe", tf == "30s"), ("RED, RED, RED, GREEN setup", seq_is([a,b,c,d],[-1,-1,-1,1])), ("First 3 RED candles have normal bodies", all(is_normal(x) for x in (a,b,c))), ("GREEN does not break previous 3 RED highs", no_break)],
                  "3 normal RED + contained GREEN", "On a 30-second chart, the green setup candle stays within the previous red structure; the strategy targets the next 30-second candle GREEN.", timeframe_rule="30S ONLY")

    # TYPE 12 - 2m only: RR + GG contained under horizontal resistance -> next red.
    if count >= 4:
        a,b,c,d = candles[-4:]
        resistance = min(float(a["top"]), float(b["top"]))
        greens_contained = max(float(c["top"]), float(d["top"])) >= resistance - tol * 0.45 and float(c["top"]) >= resistance - tol * 0.45 and float(d["top"]) >= resistance - tol * 0.45
        close_near = abs(float(d["close_y"]) - resistance) <= max(tol * 2.8, med_range * 0.65)
        add_setup(12, -1, [("2-minute timeframe", tf == "2m"), ("RED, RED, GREEN, GREEN setup", seq_is([a,b,c,d],[-1,-1,1,1])), ("Normal body candles", all(is_normal(x) for x in (a,b,c,d))), ("GREEN candles do not break first RED resistance", greens_contained), ("Last GREEN stays near horizontal level", close_near)],
                  "2 RED + 2 GREEN below horizontal resistance", "The 2-minute setup stays below the first red resistance area; the strategy targets the next 2-minute candle RED.", family="Horizontal Level", timeframe_rule="2M ONLY")

    # TYPE 13 - 2m false-break reversal: level must be swept and reclaimed.
    if count >= 7:
        prior = candles[-10:-1] if count >= 10 else candles[:-1]
        last = candles[-1]
        resistance, res_touches = strongest_level_cluster([float(x["top"]) for x in prior], tol * 1.25, "resistance")
        support, sup_touches = strongest_level_cluster([float(x["bottom"]) for x in prior], tol * 1.25, "support")
        resistance = float(resistance if resistance is not None else min(x["top"] for x in prior))
        support = float(support if support is not None else max(x["bottom"] for x in prior))
        up_sweep_reject = float(last["top"]) < resistance - tol * 0.18 and float(last["close_y"]) > resistance - tol * 0.05 and long_upper(last)
        dn_sweep_reject = float(last["bottom"]) > support + tol * 0.18 and float(last["close_y"]) < support + tol * 0.05 and long_lower(last)
        add_setup(13, -1, [("2-minute timeframe", tf == "2m"), ("Resistance retested several times", res_touches >= 2), ("Latest candle sweeps above resistance then closes back below", up_sweep_reject)],
                  "Repeated resistance retest + false breakout rejection", "The next RED target requires a sweep above resistance and a close back inside the level.", family="Breakout Reversal", timeframe_rule="2M ONLY")
        add_setup(13, 1, [("2-minute timeframe", tf == "2m"), ("Support retested several times", sup_touches >= 2), ("Latest candle sweeps below support then closes back above", dn_sweep_reject)],
                  "Repeated support retest + false breakdown rejection", "The next GREEN target requires a sweep below support and a close back inside the level.", family="Breakout Reversal", timeframe_rule="2M ONLY")

    # TYPE 14 - horizontal S/R break -> same direction next candle.
    if count >= 5:
        prior = candles[-9:-1] if count >= 9 else candles[:-1]
        last = candles[-1]
        greens = [x for x in prior if x["dir"] > 0]
        reds = [x for x in prior if x["dir"] < 0]
        support, support_touches = strongest_level_cluster([float(x["bottom"]) for x in greens], tol * 1.25, "support")
        resistance, resistance_touches = strongest_level_cluster([float(x["top"]) for x in reds], tol * 1.25, "resistance")
        if support is not None and support_touches >= 2:
            add_setup(14, -1, [("2+ GREEN candles define one support cluster", True), ("Latest candle is RED", last["dir"] < 0), ("RED closes below clustered support", close_breaks_below(last, support, 0.25))],
                      "Clustered horizontal support breakdown", "A red candle breaks support confirmed by repeated green-candle lows; the strategy targets the next candle RED.", family="Horizontal Break")
        if resistance is not None and resistance_touches >= 2:
            add_setup(14, 1, [("2+ RED candles define one resistance cluster", True), ("Latest candle is GREEN", last["dir"] > 0), ("GREEN closes above clustered resistance", close_breaks_above(last, resistance, 0.25))],
                      "Clustered horizontal resistance breakout", "A green candle breaks resistance confirmed by repeated red-candle highs; the strategy targets the next candle GREEN.", family="Horizontal Break")

    # TYPE 15 - V / inverted-V reversal only after a failed breakout is visible.
    if count >= 7:
        shape = candles[-7:-1]
        last = candles[-1]
        yv = np.array([float(x["y"]) for x in shape], dtype=float)
        low_i = int(np.argmax(yv))
        high_i = int(np.argmin(yv))
        v_shape = 1 <= low_i <= len(shape)-2 and (yv[low_i]-yv[0]) >= med_range * 0.80 and (yv[low_i]-yv[-1]) >= med_range * 0.70
        iv_shape = 1 <= high_i <= len(shape)-2 and (yv[0]-yv[high_i]) >= med_range * 0.80 and (yv[-1]-yv[high_i]) >= med_range * 0.70
        v_level = min(float(shape[0]["top"]), float(shape[1]["top"]))
        iv_level = max(float(shape[0]["bottom"]), float(shape[1]["bottom"]))
        v_false_break = float(last["top"]) < v_level - tol * 0.15 and float(last["close_y"]) > v_level - tol * 0.03 and long_upper(last)
        iv_false_break = float(last["bottom"]) > iv_level + tol * 0.15 and float(last["close_y"]) < iv_level + tol * 0.03 and long_lower(last)
        add_setup(15, -1, [("V shape formed", v_shape), ("Upside breakout attempt is rejected", v_false_break)],
                  "V pattern + failed upside breakout", "The opposite RED target is allowed only after price rejects back inside the breakout level.", family="V Reversal")
        add_setup(15, 1, [("Inverted-V shape formed", iv_shape), ("Downside breakout attempt is rejected", iv_false_break)],
                  "Inverted V + failed downside breakout", "The opposite GREEN target is allowed only after price rejects back inside the breakout level.", family="V Reversal")

    # TYPE 16 - 3/4 green normal + 1 red -> next green.
    if count >= 4 and candles[-1]["dir"] < 0:
        run = 0
        i = count - 2
        while i >= 0 and candles[i]["dir"] > 0 and run < 5:
            run += 1; i -= 1
        setup_c = candles[count-run-1:count-1] if run else []
        add_setup(16, 1, [("3 to 4 back-to-back GREEN candles", 3 <= run <= 4), ("GREEN bodies are normal", bool(setup_c) and all(is_normal(x) for x in setup_c)), ("One opposite RED setup candle", candles[-1]["dir"] < 0)],
                  "3-4 normal GREEN + 1 RED", "The continuation setup is complete; the strategy targets the next candle GREEN.")

    # TYPE 17 - 3/4 red normal + 1 green -> next red.
    if count >= 4 and candles[-1]["dir"] > 0:
        run = 0
        i = count - 2
        while i >= 0 and candles[i]["dir"] < 0 and run < 5:
            run += 1; i -= 1
        setup_c = candles[count-run-1:count-1] if run else []
        add_setup(17, -1, [("3 to 4 back-to-back RED candles", 3 <= run <= 4), ("RED bodies are normal", bool(setup_c) and all(is_normal(x) for x in setup_c)), ("One opposite GREEN setup candle", candles[-1]["dir"] > 0)],
                  "3-4 normal RED + 1 GREEN", "The continuation setup is complete; the strategy targets the next candle RED.")

    # TYPE 18 - long red marubozu + GGG + R, no resistance break -> next red.
    if count >= 5:
        a,b,c,d,e = candles[-5:]
        no_res_break = all(float(x["top"]) >= float(a["top"]) - tol * 0.30 for x in (b,c,d,e))
        add_setup(18, -1, [("Long RED marubozu first candle", a["dir"] < 0 and is_marubozu(a)), ("Then GREEN, GREEN, GREEN, RED", seq_is([b,c,d,e],[1,1,1,-1])), ("Three GREEN candles have normal bodies", all(is_normal(x) for x in (b,c,d))), ("No wick/body breaks first RED resistance", no_res_break), ("Sideways/mixed context", abs(global_trend) < 0.72)],
                  "Long RED + 3 GREEN + RED below resistance", "The entire four-candle response stays below the first long red resistance; the strategy targets the next candle RED.", family="Sideways Level")

    # TYPE 19 - long green marubozu + RRR + G, no support break -> next green.
    if count >= 5:
        a,b,c,d,e = candles[-5:]
        no_sup_break = all(float(x["bottom"]) <= float(a["bottom"]) + tol * 0.30 for x in (b,c,d,e))
        add_setup(19, 1, [("Long GREEN marubozu first candle", a["dir"] > 0 and is_marubozu(a)), ("Then RED, RED, RED, GREEN", seq_is([b,c,d,e],[-1,-1,-1,1])), ("Three RED candles have normal bodies", all(is_normal(x) for x in (b,c,d))), ("No wick/body breaks first GREEN support", no_sup_break), ("Sideways/mixed context", abs(global_trend) < 0.72)],
                  "Long GREEN + 3 RED + GREEN above support", "The entire four-candle response stays above the first long green support; the strategy targets the next candle GREEN.", family="Sideways Level")

    # TYPE 20 - downtrend: R,R,G,R where 4th red does not break previous green -> next red.
    if count >= 4:
        a,b,c,d = candles[-4:]
        no_green_breakdown = float(d["bottom"]) <= float(c["bottom"]) + tol * 0.35
        add_setup(20, -1, [("Downtrend context", global_trend < -0.10), ("RED, RED, GREEN, RED setup", seq_is([a,b,c,d],[-1,-1,1,-1])), ("First two RED candles normal", is_normal(a) and is_normal(b)), ("4th RED does not break previous GREEN low", no_green_breakdown)],
                  "Downtrend R-R-G-R hold", "The fourth red candle holds above the prior green low; the strategy targets the next candle RED.", family="Downtrend")

    # TYPE 21 - uptrend: G,G,R,G where 4th green does not break previous red -> next green.
    if count >= 4:
        a,b,c,d = candles[-4:]
        no_red_breakout = float(d["top"]) >= float(c["top"]) - tol * 0.35
        add_setup(21, 1, [("Uptrend context", global_trend > 0.10), ("GREEN, GREEN, RED, GREEN setup", seq_is([a,b,c,d],[1,1,-1,1])), ("First two GREEN candles normal", is_normal(a) and is_normal(b)), ("4th GREEN does not break previous RED high", no_red_breakout)],
                  "Uptrend G-G-R-G hold", "The fourth green candle stays below the prior red high; the strategy targets the next candle GREEN.", family="Uptrend")

    # TYPE 22 - uptrend 3-5 green + small red contained in prior green body -> next green.
    if count >= 4 and candles[-1]["dir"] < 0:
        last = candles[-1]
        run = 0; i = count - 2
        while i >= 0 and candles[i]["dir"] > 0 and run < 6:
            run += 1; i -= 1
        greens = candles[count-run-1:count-1] if run else []
        prev = candles[-2]
        add_setup(22, 1, [("Uptrend context", global_trend > 0.08), ("3 to 5 back-to-back GREEN candles", 3 <= run <= 5), ("GREEN candles normal", bool(greens) and all(is_normal(x) for x in greens)), ("Opposite RED body smaller than previous GREEN", float(last["body_height"]) < float(prev["body_height"])), ("RED body does not break previous GREEN body", float(last["body_bottom"]) <= float(prev["body_bottom"]) + tol * 0.28)],
                  "3-5 GREEN + smaller contained RED", "The small red pullback stays within the prior green body; the strategy targets the next candle GREEN.", family="Uptrend")

    # TYPE 23 - downtrend 3-5 red + small green contained in prior red body -> next red.
    if count >= 4 and candles[-1]["dir"] > 0:
        last = candles[-1]
        run = 0; i = count - 2
        while i >= 0 and candles[i]["dir"] < 0 and run < 6:
            run += 1; i -= 1
        reds = candles[count-run-1:count-1] if run else []
        prev = candles[-2]
        add_setup(23, -1, [("Downtrend context", global_trend < -0.08), ("3 to 5 back-to-back RED candles", 3 <= run <= 5), ("RED candles normal", bool(reds) and all(is_normal(x) for x in reds)), ("Opposite GREEN body smaller than previous RED", float(last["body_height"]) < float(prev["body_height"])), ("GREEN body does not break previous RED body", float(last["body_top"]) >= float(prev["body_top"]) - tol * 0.28)],
                  "3-5 RED + smaller contained GREEN", "The small green pullback stays within the prior red body; the strategy targets the next candle RED.", family="Downtrend")

    # TYPE 24 - live market sideways: G,R,R(small),G,R(smaller) -> next green, SNR respected.
    if count >= 5:
        a,b,c,d,e = candles[-5:]
        snr = (float(a["top"]) + float(b["top"])) / 2.0
        snr_respected = all(float(x["top"]) >= snr - tol * 0.40 for x in (a,b,c,d,e))
        add_setup(24, 1, [("LIVE market", is_live), ("GREEN, RED, RED, GREEN, RED setup", seq_is([a,b,c,d,e],[1,-1,-1,1,-1])), ("First GREEN and second RED maintain SNR", abs(float(a["top"])-float(b["top"])) <= tol * 1.45), ("3rd RED is Doji/small body", is_small(c)), ("4th GREEN does not break SNR", float(d["top"]) >= snr - tol * 0.40), ("5th RED body smaller than previous GREEN", float(e["body_height"]) < float(d["body_height"])), ("No setup candle breaks SNR", snr_respected), ("Sideways/mixed context", abs(global_trend) < 0.72)],
                  "LIVE sideways G-R-smallR-G-smallR at SNR", "All five live-market SNR conditions are present; the strategy targets the next candle GREEN.", family="Live SNR", timeframe_rule="LIVE MARKET ONLY")

    # TYPE 25 - small red, normal red, long green breaks first red SNR -> next green.
    if count >= 3:
        a,b,c = candles[-3:]
        snr = float(a["top"])
        add_setup(25, 1, [("RED, RED, GREEN setup", seq_is([a,b,c],[-1,-1,1])), ("1st RED is small/Doji", is_small(a)), ("2nd RED has normal body", is_normal(b)), ("3rd GREEN is long", is_long(c)), ("Long GREEN breaks 1st RED SNR", close_breaks_above(c, snr, 0.20))],
                  "Small RED + normal RED + long GREEN SNR breakout", "The long green candle breaks the SNR level created by the first small red candle; the strategy targets the next candle GREEN.", family="SNR Breakout")

    # V39 visual equivalents of the 10 new selected strategies.
    def vtrend_before_setup(setup_len: int, lookback: int) -> float:
        return trend_before(max(0, count-setup_len), lookback)

    def vtouch(c, level_y, margin=0.42):
        return float(c["top"]) <= float(level_y)+tol*margin and float(c["bottom"]) >= float(level_y)-tol*margin

    def vprior_res(setup_len: int, lookback: int = 14):
        seq0 = candles[max(0,count-setup_len-lookback):count-setup_len]
        return min((float(x["top"]) for x in seq0), default=None)

    def vprior_sup(setup_len: int, lookback: int = 14):
        seq0 = candles[max(0,count-setup_len-lookback):count-setup_len]
        return max((float(x["bottom"]) for x in seq0), default=None)

    def vbull_engulf(curr, prev, extra=0.18):
        return curr["dir"]>0 and body_inside(prev,curr,tol*extra)

    def vbear_engulf(curr, prev, extra=0.18):
        return curr["dir"]<0 and body_inside(prev,curr,tol*extra)

    if count >= 8:
        a,b=candles[-2:]; res=vprior_res(2); sup=vprior_sup(2); big=vtrend_before_setup(2,12); smallt=vtrend_before_setup(2,5)
        if res is not None:
            add_setup(26,1,[("Big trend UP",big>0.04),("Small trend UP",smallt>0.04),("RED opposite candle",a["dir"]<0),("RED touches S/R",vtouch(a,res,0.75)),("GREEN breakout closes above S/R",b["dir"]>0 and is_normal(b) and close_breaks_above(b,res,0.18))],"PDF S1 bullish S/R breakout","Trend-aligned S/R test followed by a bullish breakout close.",family="PDF · Trend Breakout")
        if sup is not None:
            add_setup(26,-1,[("Big trend DOWN",big<-0.04),("Small trend DOWN",smallt<-0.04),("GREEN opposite candle",a["dir"]>0),("GREEN touches S/R",vtouch(a,sup,0.75)),("RED breakout closes below S/R",b["dir"]<0 and is_normal(b) and close_breaks_below(b,sup,0.18))],"PDF S1 bearish S/R breakout","Trend-aligned S/R test followed by a bearish breakdown close.",family="PDF · Trend Breakout")

    if count >= 9:
        a,b,c=candles[-3:]; res=vprior_res(3); sup=vprior_sup(3); big=vtrend_before_setup(3,12); smallt=vtrend_before_setup(3,5)
        if res is not None:
            add_setup(27,1,[("Big trend UP",big>0.04),("Small trend UP",smallt>0.04),("Candle 1 GREEN breaks S/R",a["dir"]>0 and close_breaks_above(a,res,0.15)),("Candle 2 GREEN continuation",b["dir"]>0 and is_normal(b)),("Candle 3 RED engulfs candle 2",vbear_engulf(c,b,0.20)),("Candle 3 retests S/R",vtouch(c,res,0.45)),("Candle 3 closes above S/R",float(c["close_y"])<res+tol*0.05)],"PDF S4 bullish breakout-engulf-retest","Opposite engulfing retest holds the broken resistance as support.",family="PDF · Breakout Retest")
        if sup is not None:
            add_setup(27,-1,[("Big trend DOWN",big<-0.04),("Small trend DOWN",smallt<-0.04),("Candle 1 RED breaks S/R",a["dir"]<0 and close_breaks_below(a,sup,0.15)),("Candle 2 RED continuation",b["dir"]<0 and is_normal(b)),("Candle 3 GREEN engulfs candle 2",vbull_engulf(c,b,0.20)),("Candle 3 retests S/R",vtouch(c,sup,0.45)),("Candle 3 closes below S/R",float(c["close_y"])>sup-tol*0.05)],"PDF S4 bearish breakout-engulf-retest","Opposite engulfing retest holds the broken support as resistance.",family="PDF · Breakout Retest")

    if count >= 10:
        a,b,c,d=candles[-4:]; big=vtrend_before_setup(4,12); smallt=vtrend_before_setup(4,5)
        wick_both=float(b["upper_wick"])>=max(2.0,float(b["body_height"])*0.16) and float(b["lower_wick"])>=max(2.0,float(b["body_height"])*0.16)
        if a["dir"]<0:
            level=float(a["top"]); add_setup(28,1,[("Big trend UP",big>0.02),("Small trend UP",smallt>0.02),("Candle 1 normal RED",is_normal(a)),("Candle 2 small RED with wicks",b["dir"]<0 and is_small(b) and wick_both),("Candle 3 GREEN covers candle 1",vbull_engulf(c,a,0.22) and close_breaks_above(c,level,0.05)),("Candle 4 small RED",d["dir"]<0 and is_small(d)),("Candle 4 retests level",vtouch(d,level,0.40)),("Candle 4 closes above level",float(d["close_y"])<level+tol*0.05)],"PDF S6 bullish four-candle hold","Engulfing impulse plus small retest holds candle-1 S/R.",family="PDF · Four Candle Hold")
        if a["dir"]>0:
            level=float(a["bottom"]); add_setup(28,-1,[("Big trend DOWN",big<-0.02),("Small trend DOWN",smallt<-0.02),("Candle 1 normal GREEN",is_normal(a)),("Candle 2 small GREEN with wicks",b["dir"]>0 and is_small(b) and wick_both),("Candle 3 RED covers candle 1",vbear_engulf(c,a,0.22) and close_breaks_below(c,level,0.05)),("Candle 4 small GREEN",d["dir"]>0 and is_small(d)),("Candle 4 retests level",vtouch(d,level,0.40)),("Candle 4 closes below level",float(d["close_y"])>level-tol*0.05)],"PDF S6 bearish four-candle hold","Engulfing impulse plus small retest holds candle-1 S/R.",family="PDF · Four Candle Hold")

    if count >= 2:
        a,b=candles[-2:]; mid=(float(a["body_top"])+float(a["body_bottom"]))/2.0; mid_ok=abs(float(b["close_y"])-mid)<=max(float(a["body_height"])*0.28,tol*0.50)
        add_setup(29,1,[("Candle 1 normal GREEN",a["dir"]>0 and is_normal(a)),("Candle 2 RED",b["dir"]<0),("Candle 2 sweeps candle-1 low",float(b["bottom"])>float(a["bottom"])+tol*0.10 and long_lower(b)),("Close near 50% body",mid_ok)],"PDF S11 bullish 50% wick sweep","Low sweep rejects back near candle-1 midpoint.",family="PDF · Wick Sweep")
        add_setup(29,-1,[("Candle 1 normal RED",a["dir"]<0 and is_normal(a)),("Candle 2 GREEN",b["dir"]>0),("Candle 2 sweeps candle-1 high",float(b["top"])<float(a["top"])-tol*0.10 and long_upper(b)),("Close near 50% body",mid_ok)],"PDF S11 bearish 50% wick sweep","High sweep rejects back near candle-1 midpoint.",family="PDF · Wick Sweep")

    if count >= 3:
        a,b,c=candles[-3:]
        if a["dir"]<0:
            level=float(a["open_y"]); add_setup(30,1,[("Candle 1 RED",True),("Candle 2 GREEN engulfs candle 1",vbull_engulf(b,a,0.18)),("Candle 3 RED",c["dir"]<0),("Candle 3 holds candle-1 open",float(c["bottom"])<=level+tol*0.22 and float(c["close_y"])<=level+tol*0.06)],"PDF S13 bullish engulf + open-level hold","Bullish engulfing is followed by a red hold above candle-1 open.",family="PDF · Engulf Hold")
        if a["dir"]>0:
            level=float(a["open_y"]); add_setup(30,-1,[("Candle 1 GREEN",True),("Candle 2 RED engulfs candle 1",vbear_engulf(b,a,0.18)),("Candle 3 GREEN",c["dir"]>0),("Candle 3 holds candle-1 open",float(c["top"])>=level-tol*0.22 and float(c["close_y"])>=level-tol*0.06)],"PDF S13 bearish engulf + open-level hold","Bearish engulfing is followed by a green hold below candle-1 open.",family="PDF · Engulf Hold")

    if count >= 9:
        a,b,c=candles[-3:]; res=vprior_res(3); sup=vprior_sup(3)
        if res is not None:
            add_setup(31,1,[("GREEN breakout",a["dir"]>0 and close_breaks_above(a,res,0.15)),("RED retest",b["dir"]<0 and vtouch(b,res,0.45)),("Retest holds above",float(b["close_y"])<res+tol*0.04),("GREEN confirmation",c["dir"]>0 and is_normal(c)),("Confirmation extends",float(c["close_y"])<float(b["body_top"])-tol*0.04)],"S/R breakout-retest-confirmation CALL","Breakout, retest and fresh bullish confirmation.",family="Premium · Breakout Retest")
        if sup is not None:
            add_setup(31,-1,[("RED breakdown",a["dir"]<0 and close_breaks_below(a,sup,0.15)),("GREEN retest",b["dir"]>0 and vtouch(b,sup,0.45)),("Retest holds below",float(b["close_y"])>sup-tol*0.04),("RED confirmation",c["dir"]<0 and is_normal(c)),("Confirmation extends",float(c["close_y"])>float(b["body_bottom"])+tol*0.04)],"S/R breakout-retest-confirmation PUT","Breakdown, retest and fresh bearish confirmation.",family="Premium · Breakout Retest")

    if count >= 8:
        a,b=candles[-2:]; res=vprior_res(2); sup=vprior_sup(2)
        if sup is not None:
            add_setup(32,1,[("Sweep below prior support",float(a["bottom"])>sup+tol*0.12),("Sweep closes back above support",float(a["close_y"])<sup-tol*0.04),("Long lower wick",long_lower(a)),("GREEN confirmation",b["dir"]>0 and is_normal(b)),("Confirmation clears sweep body",float(b["close_y"])<float(a["body_top"])-tol*0.04)],"Liquidity sweep bullish reversal","Support sweep is reclaimed and confirmed bullish.",family="Premium · Liquidity Sweep")
        if res is not None:
            add_setup(32,-1,[("Sweep above prior resistance",float(a["top"])<res-tol*0.12),("Sweep closes back below resistance",float(a["close_y"])>res+tol*0.04),("Long upper wick",long_upper(a)),("RED confirmation",b["dir"]<0 and is_normal(b)),("Confirmation clears sweep body",float(b["close_y"])>float(a["body_bottom"])+tol*0.04)],"Liquidity sweep bearish reversal","Resistance sweep is reclaimed and confirmed bearish.",family="Premium · Liquidity Sweep")

    if count >= 9:
        a,b,c=candles[-3:]; big=vtrend_before_setup(3,12); smallt=vtrend_before_setup(3,6)
        add_setup(33,1,[("Big trend UP",big>0.05),("Small trend UP",smallt>0.03),("Two RED pullback candles",seq_is([a,b],[-1,-1])),("Pullback not oversized",float(a["body_height"])<=med_body*1.45 and float(b["body_height"])<=med_body*1.45),("GREEN continuation",c["dir"]>0 and float(c["body_ratio"])>=0.45),("Continuation clears pullback highs",float(c["close_y"])<min(float(a["top"]),float(b["top"]))-tol*0.04)],"Trend pullback continuation CALL","Controlled pullback followed by bullish continuation.",family="Premium · Trend Pullback")
        add_setup(33,-1,[("Big trend DOWN",big<-0.05),("Small trend DOWN",smallt<-0.03),("Two GREEN pullback candles",seq_is([a,b],[1,1])),("Pullback not oversized",float(a["body_height"])<=med_body*1.45 and float(b["body_height"])<=med_body*1.45),("RED continuation",c["dir"]<0 and float(c["body_ratio"])>=0.45),("Continuation clears pullback lows",float(c["close_y"])>max(float(a["bottom"]),float(b["bottom"]))+tol*0.04)],"Trend pullback continuation PUT","Controlled pullback followed by bearish continuation.",family="Premium · Trend Pullback")

    if count >= 9:
        a,b,c=candles[-3:]; res=vprior_res(3); sup=vprior_sup(3)
        if sup is not None:
            add_setup(34,1,[("RED closes below support",a["dir"]<0 and close_breaks_below(a,sup,0.12)),("GREEN reclaims support",b["dir"]>0 and float(b["close_y"])<sup-tol*0.04),("GREEN confirmation",c["dir"]>0 and is_normal(c)),("Confirmation extends",float(c["close_y"])<float(b["close_y"])-tol*0.04)],"Failed breakdown bullish reversal","Breakdown fails, level is reclaimed, bullish confirmation follows.",family="Premium · Failed Breakout")
        if res is not None:
            add_setup(34,-1,[("GREEN closes above resistance",a["dir"]>0 and close_breaks_above(a,res,0.12)),("RED falls back below resistance",b["dir"]<0 and float(b["close_y"])>res+tol*0.04),("RED confirmation",c["dir"]<0 and is_normal(c)),("Confirmation extends",float(c["close_y"])>float(b["close_y"])+tol*0.04)],"Failed breakout bearish reversal","Breakout fails, level is lost, bearish confirmation follows.",family="Premium · Failed Breakout")

    if count >= 8:
        a,b=candles[-2:]; res=vprior_res(2); sup=vprior_sup(2)
        if sup is not None:
            add_setup(35,1,[("RED setup at support",a["dir"]<0 and vtouch(a,sup,0.55)),("GREEN bullish engulfing",vbull_engulf(b,a,0.18)),("Engulf closes above support",float(b["close_y"])<sup-tol*0.04)],"Bullish engulfing at key support","Engulfing occurs at support, not in the middle of the range.",family="Premium · Engulfing S/R")
        if res is not None:
            add_setup(35,-1,[("GREEN setup at resistance",a["dir"]>0 and vtouch(a,res,0.55)),("RED bearish engulfing",vbear_engulf(b,a,0.18)),("Engulf closes below resistance",float(b["close_y"])>res+tol*0.04)],"Bearish engulfing at key resistance","Engulfing occurs at resistance, not in the middle of the range.",family="Premium · Engulfing S/R")

    # V11 conflict gate: an opposite exact setup is never overridden by a numeric
    # priority. Same-direction exact setups reinforce one another; opposite exact
    # setups produce NO TRADE until the chart resolves.
    exact.sort(key=lambda s: (int(s.get("priority") or 0), int(s["rules_total"]), int(s["pattern_type"])), reverse=True)
    near.sort(key=lambda s: (float(s["score"]), int(s["rules_total"])), reverse=True)
    exact_directions = {str(x.get("direction") or "") for x in exact}
    conflict_gate = len({x for x in exact_directions if x in {"UP", "DOWN"}}) > 1
    best = None if conflict_gate else (exact[0] if exact else None)
    best_near = near[0] if near else None

    if conflict_gate:
        bias = "NO TRADE"
        next_color = "NONE"
        selected_dir = "NONE"
        setup_quality = "LOW"
        selected = "CONFLICTING ACTIVE STRATEGIES"
        match_score = 100.0
        confidence = 0.0
        signals_out = exact[:8]
        conflict_names = ", ".join(f"{x['name']} {x['direction']}" for x in exact[:6])
        reasons.append(f"Conflict Gate blocked the entry because opposite exact setups are present: {conflict_names}.")
        reasons.append("Wait for a fresh closed candle and re-scan; V11 never chooses UP/DOWN by priority when exact setups disagree.")
    elif best:
        direction = str(best["direction"])
        next_color = str(best["next_candle"])
        bias = "UP SIGNAL" if direction == "UP" else "DOWN SIGNAL"
        # 100% means every coded source rule for this setup matched; it is not a profit probability.
        match_score = 100.0
        setup_quality = "HIGH" if quality >= 72 else "MEDIUM"
        selected = str(best["name"])
        reasons.extend([
            f"{selected} exact setup matched: {best['rules_matched']}/{best['rules_total']} coded rules.",
            f"Strategy target: NEXT candle {next_color} ({direction}). Entry alert is for the next candle open after setup confirmation.",
        ])
        if best.get("recovery_trade"):
            reasons.append("RECOVERY TRADE: Pattern Type 6 is active because the previous trade is recorded as LOSS.")
        confidence = match_score
        selected_dir = direction
        signals_out = exact[:8]
        if not captured_at_close:
            # Pattern can be inspected, but a static/mid-candle frame cannot prove
            # that the setup completed exactly at the boundary. Do not arm an entry.
            bias = "NO TRADE"
            next_color = "NONE"
            confidence = 0.0
            selected_dir = "NONE"
            selected = f"WAIT CLOSE: {best['name']}"
            reasons.append("Closed Candle Lock: exact setup geometry was seen, but timing was not captured at candle close. Use ONE-TAP CAMERA AUTO SCAN for the next boundary.")
    else:
        bias = "NO TRADE"
        next_color = "NONE"
        selected_dir = "NONE"
        setup_quality = "LOW"
        if best_near:
            selected = f"WATCH: {best_near['name']}"
            match_score = float(best_near["score"])
            reasons.append(f"No exact active strategy setup yet. Closest is {best_near['name']} with {best_near['rules_matched']}/{best_near['rules_total']} rules currently visible.")
        else:
            selected = "NO ACTIVE STRATEGY SETUP"
            match_score = 0.0
            reasons.append("No exact active strategy setup is complete on the newest readable candles.")
        reasons.append("No directional entry is armed until every required rule for one strategy setup is present.")
        confidence = match_score
        signals_out = near[:8]

    latest_dir = "GREEN" if candles and candles[-1]["dir"] > 0 else "RED" if candles else "UNKNOWN"

    # Strategy Proof: normalized candle geometry for the newest closed candles.
    candle_debug: list[dict[str, Any]] = []
    debug_seq = candles[-10:]
    for i, c in enumerate(debug_seq, start=max(1, count - len(debug_seq) + 1)):
        rng = max(1.0, float(c.get("range") or 1.0))
        body_pct = round(float(c.get("body_height") or 0.0) / rng * 100.0, 1)
        upper_pct = round(float(c.get("upper_wick") or 0.0) / rng * 100.0, 1)
        lower_pct = round(float(c.get("lower_wick") or 0.0) / rng * 100.0, 1)
        body_class = "SMALL" if is_small(c) else "LONG" if is_long(c) else "NORMAL" if is_normal(c) else "OTHER"
        candle_debug.append({
            "n": i, "color": "GREEN" if c["dir"] > 0 else "RED",
            "body_pct": body_pct, "upper_wick_pct": upper_pct, "lower_wick_pct": lower_pct,
            "body_class": body_class,
        })

    if quality < 65:
        warnings.append("Image is usable, but a sharper screenshot/photo will improve wick/body and SNR-level measurement.")
    warnings.append("Setup Match measures coded rule agreement only; it is not a guaranteed win probability.")

    result = {
        "bias": bias,
        "confidence": round(float(confidence), 1),
        "setup_match": round(float(match_score), 1),
        "image_quality_score": quality,
        "detected_candles": detected_count,
        "closed_candles_analyzed": count,
        "visual_trend": context_label,
        "momentum": "PATTERN TYPE 1-25 ONLY",
        "volatility": "NOT USED",
        "selected_pattern": selected,
        "pattern_type": int(best.get("pattern_type") or 0) if best else 0,
        "pattern_direction": selected_dir,
        "pattern_score": round(float(match_score), 1),
        "setup_rules": list(best.get("rules") or []) if best else list((best_near or {}).get("rules") or []),
        "rules_matched": int((best or best_near or {}).get("rules_matched") or 0),
        "rules_total": int((best or best_near or {}).get("rules_total") or 0),
        "pattern_signals": signals_out,
        "pattern_library": library,
        "pattern_library_size": SK25_PATTERN_LIBRARY_SIZE,
        "confluence_count": len(exact) if (best and not conflict_gate) else 0,
        "setup_quality": setup_quality,
        "conflict_gate": bool(conflict_gate),
        "timing_verified": bool(captured_at_close),
        "forming_candle_excluded": bool(forming_candle_excluded),
        "newborn_candle_excluded": bool(newborn_candle_excluded),
        "observed_latest_candle_direction": observed_latest_direction,
        "candle_debug": candle_debug,
        "next_candle_color": next_color,
        "entry_instruction": "NEXT CANDLE OPEN" if (best and captured_at_close and not conflict_gate) else "WAIT FOR VERIFIED CANDLE CLOSE" if best else "WAIT FOR EXACT SETUP",
        "recovery_trade": bool(best and best.get("recovery_trade")),
        "recovery_candidate": bool(any(int(x.get("pattern_type") or 0) == 6 for x in near)),
        "latest_candle_direction": latest_dir,
        "last_outcome_used": previous_outcome or "NONE",
        "reasons": reasons[:10],
        "warnings": warnings[:7],
        "pattern_status": {
            "Mode": "RAJA Pattern Type 1-25 only",
            "Indicators": "OFF - signal engine uses only closed-candle SK25 rules",
            "Context": f"Visual candle context: {context_label}",
            "Candle geometry": f"{count} closed candles analysed / {detected_count} visible structures",
            "Closed Candle Lock": "VERIFIED AT CLOSE" if captured_at_close else "FORMING CANDLE EXCLUDED - ENTRY NOT ARMED",
            "Conflict Gate": "BLOCK" if conflict_gate else "PASS",
            "Timeframe rules": "Type 11=30s; Type 12/13=2m; Type 1=OTC; Type 24=Live market",
        },
        "engine": "RAJA V42 · Strict SK25 + Adaptive Vision + Closed Candle Lock + Conflict Gate",
        "analysis_crop_mode": crop_name,
    }
    result.update(legacy_aliases(selected, selected_dir, round(float(match_score), 1), signals_out, 25))
    return result

MIN_SIGNAL_CANDLES = max(10, min(30, int(os.environ.get("RAJA_SCANNER_MIN_SIGNAL_CANDLES", "14"))))
MIN_SIGNAL_IMAGE_QUALITY = max(45.0, min(90.0, float(os.environ.get("RAJA_SCANNER_MIN_IMAGE_QUALITY", "65"))))

def _rotate_image_bytes(raw: bytes, angle: int) -> bytes:
    """Rotate the uploaded visual frame for mobile/sideways-photo rescue."""
    image = Image.open(io.BytesIO(raw))
    image = ImageOps.exif_transpose(image).convert("RGB")
    rotated = image.rotate(angle, expand=True)
    out = io.BytesIO()
    rotated.save(out, format="JPEG", quality=94, optimize=True)
    return out.getvalue()


def _analysis_candidate_score(result: dict[str, Any]) -> float:
    candles = float(result.get("detected_candles") or 0)
    quality = float(result.get("image_quality_score") or 0)
    strategy = float(result.get("pattern_score") or result.get("strategy_score") or 0)
    readable = 0.0 if str(result.get("visual_trend") or "").upper() == "UNREADABLE" else 40.0
    # Candle count dominates because a sharp photo of the wrong/rotated region can still have a high quality score.
    return candles * 12.0 + quality * 0.6 + strategy * 0.25 + readable


def analyze_chart_image_mobile_safe(raw: bytes, timeframe: str = "1m", market: str = "", last_outcome: str = "", *, captured_at_close: bool = False) -> dict[str, Any]:
    """Analyze the frame, rescue sideways mobile photos, then apply a strict signal-quality gate."""
    candidates: list[tuple[int, dict[str, Any]]] = []
    base = analyze_chart_image(raw, timeframe=timeframe, market=market, last_outcome=last_outcome, captured_at_close=captured_at_close)
    candidates.append((0, base))

    base_candles = int(base.get("detected_candles") or 0)
    base_trend = str(base.get("visual_trend") or "").upper()
    # Only spend extra CPU when the original frame is suspicious/too sparse.
    if base_candles < max(MIN_SIGNAL_CANDLES + 4, 18) or base_trend == "UNREADABLE":
        for angle in (90, 270):
            try:
                candidates.append((angle, analyze_chart_image(_rotate_image_bytes(raw, angle), timeframe=timeframe, market=market, last_outcome=last_outcome, captured_at_close=captured_at_close)))
            except Exception:
                pass
        # 180° is less common, so try it only for a very poor original read.
        if base_candles < 8:
            try:
                candidates.append((180, analyze_chart_image(_rotate_image_bytes(raw, 180), timeframe=timeframe, market=market, last_outcome=last_outcome, captured_at_close=captured_at_close)))
            except Exception:
                pass

    angle, best = max(candidates, key=lambda item: _analysis_candidate_score(item[1]))
    best = dict(best)
    best["auto_rotation_degrees"] = int(angle)

    candles = int(best.get("detected_candles") or 0)
    quality = float(best.get("image_quality_score") or 0)
    trend = str(best.get("visual_trend") or "").upper()
    reasons: list[str] = []
    if candles < MIN_SIGNAL_CANDLES:
        reasons.append(f"Only {candles} candles were read; at least {MIN_SIGNAL_CANDLES} clear candles are required for an UP/DOWN signal.")
    if quality < MIN_SIGNAL_IMAGE_QUALITY:
        reasons.append(f"Image quality {quality:.0f}/100 is below the signal threshold {MIN_SIGNAL_IMAGE_QUALITY:.0f}/100.")
    if trend == "UNREADABLE":
        reasons.append("The chart structure is unreadable.")

    rescan_required = bool(reasons)
    best["rescan_required"] = rescan_required
    best["scan_gate"] = "RESCAN" if rescan_required else "PASS"
    best["scan_gate_reason"] = " ".join(reasons) if reasons else "Frame has enough readable candles and image quality."

    if rescan_required:
        # Never emit directional trading guidance from a poor/mobile-misaligned frame.
        best["raw_bias_before_scan_gate"] = best.get("bias")
        best["bias"] = "NO TRADE"
        best["confidence"] = 0.0
        best["selected_strategy"] = "RESCAN REQUIRED"
        best["selected_pattern"] = "RESCAN REQUIRED"
        best["strategy_direction"] = "NONE"
        best["pattern_direction"] = "NONE"
        best["strategy_score"] = 0.0
        best["pattern_score"] = 0.0
        best["setup_match"] = 0.0
        best["next_candle_color"] = "NONE"
        best["entry_instruction"] = "RESCAN FIRST"
        best["recovery_trade"] = False
        best["confluence_count"] = 0
        best["setup_quality"] = "LOW"
        warnings = list(best.get("warnings") or [])
        warnings.insert(0, "Scan Gate blocked UP/DOWN: retake a straight, focused chart photo with more visible candles.")
        best["warnings"] = warnings[:6]
    elif angle:
        notes = list(best.get("warnings") or [])
        notes.append(f"Mobile orientation rescue: chart was auto-rotated {angle}° before analysis.")
        best["warnings"] = notes[:6]

    return best


@app.route("/live-scanner")
def live_scanner_page():
    response = send_from_directory(str(BASE_DIR), "live_scanner.html")
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/chart-scanner")
def chart_scanner_page():
    response = send_from_directory(str(BASE_DIR), "chart_scanner.html")
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/chart-preflight", methods=["POST"])
def chart_preflight_api():
    """Detect/test a candle crop. This endpoint never evaluates SK25 or emits a signal."""
    auth_data = {
        "key": request.form.get("key"), "user": request.form.get("user"),
        "device": request.form.get("device"), "session_token": request.form.get("session_token"),
    }
    _auth, error = _auth_session(auth_data)
    if error:
        return error
    upload = request.files.get("image")
    if not upload:
        return jsonify({"status": "error", "message": "A live chart frame is required."}), 400
    raw = upload.read(RAJA_CHART_SCAN_MAX_UPLOAD + 1)
    if len(raw) > RAJA_CHART_SCAN_MAX_UPLOAD:
        return jsonify({"status": "error", "message": "Image is too large."}), 413
    already_cropped = str(request.form.get("already_cropped") or "").lower() in {"1", "true", "yes", "on"}
    try:
        result = analyze_chart_preflight(raw, already_cropped=already_cropped)
    except (ValueError, UnidentifiedImageError) as exc:
        return jsonify({"status": "error", "message": str(exc)}), 400
    finished_ms = int(time.time() * 1000)
    result.update({
        "broker": str(request.form.get("broker") or "Quotex").strip()[:60],
        "market": str(request.form.get("market") or "OTC").strip()[:40],
        "engine": "RAJA V42 · AUTO CHART PREFLIGHT · NO SIGNAL",
    })
    response = jsonify({"status": "success", "result": result, "server_epoch_ms": finished_ms})
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return response


@app.route("/chart-scan", methods=["POST"])
def chart_scan_api():
    request_received_ms = int(time.time() * 1000)
    auth_data = {
        "key": request.form.get("key"), "user": request.form.get("user"),
        "device": request.form.get("device"), "session_token": request.form.get("session_token"),
    }
    auth, error = _auth_session(auth_data)
    if error: return error
    upload=request.files.get("image")
    if not upload: return jsonify({"status":"error","message":"Camera photo or screenshot is required."}),400
    raw=upload.read(RAJA_CHART_SCAN_MAX_UPLOAD+1)
    if len(raw)>RAJA_CHART_SCAN_MAX_UPLOAD: return jsonify({"status":"error","message":"Image is too large."}),413
    broker=str(request.form.get("broker") or "Quotex").strip()[:60]
    market=str(request.form.get("market") or "ForexLive").strip()[:40]
    pair=str(request.form.get("pair") or "").strip()[:120]
    timeframe=str(request.form.get("timeframe") or "1m").strip().lower()[:20]
    source=str(request.form.get("source") or "CHART_SCANNER").strip()[:80]
    capture_mode=str(request.form.get("capture_mode") or "SCREENSHOT").strip().upper()[:30]
    chart_crop_set=str(request.form.get("chart_crop_set") or "").lower() in {"1","true","yes","on"}
    chart_crop_tested=str(request.form.get("chart_crop_tested") or "").lower() in {"1","true","yes","on"}
    previous_signal_key=str(request.form.get("last_signal_key") or "").strip()[:96]
    if timeframe not in {"30s","1m","2m","5m","10m","15m","30m"}:
        return jsonify({"status":"error","message":"Unsupported timeframe."}),400
    captured=str(request.form.get("captured_at_close") or "").lower() in {"1","true","yes","on"}
    try:
        captured_at_epoch_ms = int(float(request.form.get("captured_at_epoch_ms") or 0))
    except Exception:
        captured_at_epoch_ms = 0
    last_outcome=_last_strategy_outcome(auth.get("user"))
    try:
        result=analyze_chart_image_mobile_safe(raw,timeframe=timeframe,market=market,last_outcome=last_outcome,captured_at_close=captured)
    except (ValueError,UnidentifiedImageError) as exc:
        return jsonify({"status":"error","message":str(exc)}),400
    now_epoch_ms = int(time.time() * 1000)
    now_epoch = now_epoch_ms // 1000
    result.update({
        "broker":broker,"market":market,"pair":pair,"timeframe":timeframe,
        "created_at":now_epoch,"created_at_ms":now_epoch_ms,
        "engine":"RAJA V42 · VISUAL SK25 · AUTO CHART PREFLIGHT",
        "pattern_library":"RAJA SK25 Pattern Type 1-25",
        "pattern_library_size":25,
        "chart_crop_set":chart_crop_set,
        "chart_crop_tested":chart_crop_tested,
        "capture_mode":capture_mode,
        "source":source,
    })

    # V42 Auto Chart Gate: a live boundary frame requires both an explicitly
    # confirmed crop and a passing TEST FRAME. Full-screen/unverified frames remain
    # WATCH-only because broker controls and indicators can resemble candle geometry.
    live_boundary_source = source == "RAJA_LIVE_SCREEN_SCANNER" and captured
    if live_boundary_source and (not chart_crop_set or not chart_crop_tested):
        gate_message = (
            "Confirm CHART AREA and pass TEST FRAME before a close scan; "
            "full-frame or untested live signals are blocked."
        )
        result.update({
            "raw_bias_before_chart_gate": result.get("bias"),
            "bias":"NO TRADE","confidence":0.0,"pattern_direction":"NONE",
            "next_candle_color":"NONE","entry_instruction":"CONFIRM AND TEST CHART AREA FIRST",
            "rescan_required":True,"scan_gate":"RESCAN","scan_gate_reason":gate_message,
            "signal_state":"GATE BLOCKED","entry_allowed":False,
            "duplicate_signal":False,"late_signal":False,"signal_key":"",
        })
        warnings=list(result.get("warnings") or [])
        warnings.insert(0,gate_message)
        result["warnings"]=warnings[:7]

    # V42 Exact Entry Clock. The close frame targets the candle that opens at the
    # captured boundary. Short windows prevent a late mid-candle entry from being
    # presented as the original setup.
    duration_seconds={"30s":30,"1m":60,"2m":120,"5m":300,"10m":600,"15m":900,"30m":1800}.get(timeframe,60)
    duration_ms=duration_seconds*1000
    entry_window_seconds={"30s":5,"1m":10,"2m":10,"5m":12,"10m":12,"15m":12,"30m":12}.get(timeframe,10)
    capture_boundary_ms=int(captured_at_epoch_ms or 0)
    max_clock_error_ms=max(180000,duration_ms)
    clock_alignment_corrected=False
    if not capture_boundary_ms or abs(request_received_ms-capture_boundary_ms)>max_clock_error_ms:
        capture_boundary_ms=(request_received_ms//duration_ms)*duration_ms
        clock_alignment_corrected=True
    else:
        boundary_remainder=capture_boundary_ms%duration_ms
        boundary_error=min(boundary_remainder,duration_ms-boundary_remainder)
        if boundary_error>1800:
            capture_boundary_ms=(request_received_ms//duration_ms)*duration_ms
            clock_alignment_corrected=True

    directional = captured and str(result.get("bias") or "").upper().startswith(("UP","DOWN")) and not result.get("rescan_required")
    signal_key=""
    duplicate_signal=False
    entry_allowed=False
    if directional:
        bias=str(result.get("bias") or "").upper()
        key_material="|".join([
            str(auth.get("user") or ""),broker,market,pair,timeframe,
            str(capture_boundary_ms),str(result.get("pattern_type") or 0),bias,
        ])
        signal_key=hashlib.sha256(key_material.encode("utf-8")).hexdigest()[:24]
        duplicate_signal=bool(previous_signal_key and hmac.compare_digest(previous_signal_key,signal_key))
        latest_ms=int(time.time()*1000)
        late_signal=latest_ms>capture_boundary_ms+entry_window_seconds*1000
        entry_allowed=not duplicate_signal and not late_signal
        instruction=(
            "DUPLICATE SIGNAL — DO NOT PLACE AGAIN" if duplicate_signal else
            "ENTRY WINDOW PASSED — DO NOT ENTER THIS CANDLE" if late_signal else
            "TAKE TRADE NOW"
        )
        result.update({
            "entry_epoch":capture_boundary_ms//1000,
            "entry_epoch_ms":capture_boundary_ms,
            "expiry_epoch":(capture_boundary_ms+duration_ms)//1000,
            "expiry_epoch_ms":capture_boundary_ms+duration_ms,
            "entry_window_seconds":entry_window_seconds,
            "entry_timing_mode":"target_candle_open",
            "entry_instruction":instruction,
            "entry_allowed":entry_allowed,
            "duplicate_signal":duplicate_signal,
            "late_signal":late_signal,
            "signal_key":signal_key,
            "signal_state":"CONFIRMED" if entry_allowed else "DUPLICATE" if duplicate_signal else "EXPIRED",
            "seconds_since_entry_open":max(0,(latest_ms-capture_boundary_ms)//1000),
            "seconds_left_in_entry_window":max(0,(capture_boundary_ms+entry_window_seconds*1000-latest_ms+999)//1000),
        })
    elif not result.get("signal_state"):
        selected=str(result.get("selected_pattern") or "").upper()
        result["signal_state"]="ARMED" if selected.startswith("WAIT CLOSE:") else "CLOSE CHECKED" if captured else "WATCH"
        result["entry_allowed"]=False
        result["duplicate_signal"]=False
        result["late_signal"]=False
        result["signal_key"]=""

    finished_ms=int(time.time()*1000)
    proof={
        "version":"V42_AUTO_CHART_PROOF",
        "broker":broker,"market":market,"pair":pair,"timeframe":timeframe,
        "signal_state":result.get("signal_state"),"direction":result.get("pattern_direction"),
        "pattern_type":int(result.get("pattern_type") or 0),
        "selected_pattern":str(result.get("selected_pattern") or ""),
        "rules_matched":int(result.get("rules_matched") or 0),
        "rules_total":int(result.get("rules_total") or 0),
        "captured_at_close":bool(captured),"chart_crop_set":bool(chart_crop_set),
        "chart_crop_tested":bool(chart_crop_tested),
        "scan_gate":str(result.get("scan_gate") or "PASS"),
        "image_quality_score":float(result.get("image_quality_score") or 0),
        "detected_candles":int(result.get("detected_candles") or 0),
        "setup_candle_close_epoch_ms":capture_boundary_ms if captured else 0,
        "entry_epoch_ms":int(result.get("entry_epoch_ms") or 0),
        "expiry_epoch_ms":int(result.get("expiry_epoch_ms") or 0),
        "server_analysis_ms":max(0,finished_ms-request_received_ms),
        "capture_to_response_ms":max(0,finished_ms-capture_boundary_ms) if captured else 0,
        "clock_alignment_corrected":clock_alignment_corrected,
        "signal_key":signal_key,
    }
    result["proof"]=proof
    return jsonify({
        "status":"success","result":result,"server_epoch":finished_ms//1000,
        "server_epoch_ms":finished_ms,"request_received_epoch_ms":request_received_ms,
    })


@app.route("/side-auto-signals", methods=["POST"])
def side_auto_signals():
    """Continuous 3-minute strong-signal feed. Returns ranked 1m/5m candidates without changing the HUD."""
    data = request.get_json(silent=True) or {}
    auth, error = _auth_session(data)
    if error:
        return error

    maintenance = scan_maintenance_state()
    if maintenance:
        return jsonify({
            "status": "error",
            "maintenance": True,
            "message": maintenance.get("maintenance_message") or "RAJA AI scans are temporarily paused.",
        }), 503

    requested_pairs = data.get("pairs") or []
    market = str(data.get("market") or "Unknown")[:80]
    broker = str(data.get("broker") or "").strip()[:80]
    opts = normalize_scan_options(data.get("scan_options"))
    if not isinstance(requested_pairs, list):
        return jsonify({"status": "error", "message": "pairs must be an array."}), 400

    pairs, seen = [], set()
    broker_key = broker.casefold().replace(" ", "")
    broker_is_native = broker_key in {"quotex", "pocketoption", "pocket_option", "pocket"}
    for raw in requested_pairs[:40]:
        pair = str(raw).strip()
        supported = pair in YAHOO_SYMBOLS or (broker_is_native and "(otc)" in pair.casefold())
        if supported and pair not in seen:
            pairs.append(pair)
            seen.add(pair)
    if not pairs:
        return jsonify({"status": "error", "message": "No supported pairs were supplied."}), 400

    results_by_pair = {}
    timed_out_pairs = []
    workers = min(BATCH_SCAN_WORKERS, len(pairs))
    started = time.time()
    pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="raja-side-auto")
    future_map = {
        pool.submit(calculate_side_auto_signal_candidates, pair, opts, auth["user"], broker): pair
        for pair in pairs
    }
    done, pending = wait(future_map.keys(), timeout=SIDE_AUTO_SIGNAL_DEADLINE_SECONDS)
    for future in done:
        pair = future_map[future]
        try:
            results_by_pair[pair] = future.result() or []
        except Exception as exc:
            print(f"Side auto signal error for {pair}: {exc}")
            results_by_pair[pair] = []
    for future in pending:
        pair = future_map[future]
        timed_out_pairs.append(pair)
        future.cancel()
        results_by_pair[pair] = []
    pool.shutdown(wait=False, cancel_futures=True)

    grouped = {"1m": [], "5m": []}
    for pair in pairs:
        for item in results_by_pair.get(pair, []):
            tf = str(item.get("timeframe") or "")
            if tf in grouped:
                grouped[tf].append(item)

    # Highest quality candidate first. Return several so the UI can evolve without another backend change.
    for tf in grouped:
        grouped[tf].sort(
            key=lambda item: (
                float(item.get("power") or 0.0),
                float(item.get("rank_score") or 0.0),
                float(item.get("score") or 0.0),
            ),
            reverse=True,
        )
        grouped[tf] = grouped[tf][:8]

    return jsonify({
        "status": "success",
        "signals": grouped,
        "generated_at": int(time.time()),
        "interval_seconds": 180,
        "news_safety_lock": None,
        "diagnostics": {
            "total_pairs": len(pairs),
            "completed_pairs": len(done),
            "timed_out_pairs": timed_out_pairs,
            "timed_out_pairs_count": len(timed_out_pairs),
            "candidates_1m": len(grouped["1m"]),
            "candidates_5m": len(grouped["5m"]),
            "elapsed_seconds": round(time.time() - started, 2),
            "scan_mode": opts["mode"],
        },
    })


@app.route("/market-health", methods=["POST"])
def market_health_test():
    data = request.get_json(silent=True) or {}
    auth, error = _auth_session(data)
    if error: return error

    maintenance = scan_maintenance_state()
    if maintenance:
        return jsonify({"status":"error","maintenance":True,"message":maintenance.get("maintenance_message") or "RAJA AI scans are temporarily paused."}), 503

    raw_pairs = data.get("pairs")
    if not isinstance(raw_pairs, list):
        single = str(data.get("pair") or "").strip()
        raw_pairs = [single] if single else []
    tf = str(data.get("expiry") or "1m").strip().lower()
    broker = str(data.get("broker") or "").strip()[:80]
    market = str(data.get("market") or "Unknown")[:80]
    broker_key = broker.casefold().replace(" ", "")
    broker_is_native = broker_key in {"quotex","pocketoption","pocket_option","pocket"}

    pairs, seen = [], set()
    for raw in raw_pairs[:40]:
        pair = str(raw or "").strip()
        supported = pair in YAHOO_SYMBOLS or (broker_is_native and "(otc)" in pair.casefold())
        if pair and supported and raja_pair_allowed_for_broker(broker, pair) and pair not in seen:
            pairs.append(pair); seen.add(pair)
    if not pairs:
        return jsonify({"status":"error","message":"No supported pairs were supplied for market health testing."}), 400

    started = time.time()
    workers = min(max(1, BATCH_SCAN_WORKERS), len(pairs))
    results_by_pair = {}
    pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="raja-health")
    future_map = {pool.submit(calculate_market_health, pair, tf, auth["user"], broker): pair for pair in pairs}
    done, pending = wait(future_map.keys(), timeout=min(BATCH_SCAN_DEADLINE_SECONDS, 70.0))
    for fut in done:
        pair = future_map[fut]
        try:
            results_by_pair[pair] = fut.result()
        except Exception as exc:
            results_by_pair[pair] = {"pair":pair,"selected_expiry":tf,"grade":"BAD","status":"BAD","score":0.0,"tradeable":False,"reason":f"Market health worker failed: {type(exc).__name__}."}
    timed_out = []
    for fut in pending:
        pair = future_map[fut]; timed_out.append(pair); fut.cancel()
        results_by_pair[pair] = {"pair":pair,"selected_expiry":tf,"grade":"BAD","status":"BAD","score":0.0,"tradeable":False,"reason":"Market health test timed out; pair was blocked conservatively."}
    pool.shutdown(wait=False, cancel_futures=True)

    rows = [results_by_pair[p] for p in pairs]
    good = sum(1 for r in rows if r.get("grade") == "GOOD")
    caution = sum(1 for r in rows if r.get("grade") == "CAUTION")
    bad = len(rows) - good - caution
    allowed_pairs = [r.get("pair") for r in rows if r.get("tradeable")]
    summary_score = round(sum(float(r.get("score") or 0.0) for r in rows) / max(1, len(rows)), 1)
    summary_grade = "GOOD" if good and bad == 0 and summary_score >= 70 else ("CAUTION" if allowed_pairs else "BAD")

    return jsonify({
        "status":"success",
        "data":rows,
        "summary":{
            "market":market,"expiry":tf,"grade":summary_grade,"score":summary_score,
            "total_pairs":len(rows),"good_pairs":good,"caution_pairs":caution,"bad_pairs":bad,
            "tradeable_pairs":allowed_pairs,"blocked_pairs":[r.get("pair") for r in rows if not r.get("tradeable")],
            "timed_out_pairs":timed_out,"elapsed_seconds":round(time.time()-started,2),
            "method":"Closed-candle structure + rolling SK25 next-candle replay",
            "backtest_is_guarantee":False,
        }
    })


@app.route("/scan-batch", methods=["POST"])
def scan_batch():
    data = request.get_json(silent=True) or {}
    auth, error = _auth_session(data)
    if error: return error

    maintenance = scan_maintenance_state()
    if maintenance:
        return jsonify({
            "status": "error",
            "maintenance": True,
            "message": maintenance.get("maintenance_message") or "RAJA AI scans are temporarily paused.",
        }), 503

    requested_pairs = data.get("pairs") or []; selected_expiry = str(data.get("expiry", "")).strip()
    market = str(data.get("market") or "Unknown")[:80]; broker = str(data.get("broker") or "").strip()[:80]; opts = normalize_scan_options(data.get("scan_options"))
    if not isinstance(requested_pairs, list):
        return jsonify({"status": "error", "message": "pairs must be an array."}), 400
    pairs, seen = [], set()
    broker_key = broker.casefold().replace(" ", "")
    broker_is_native = broker_key in {"quotex", "pocketoption", "pocket_option", "pocket"}
    for raw in requested_pairs[:40]:
        pair = str(raw).strip()
        supported = pair in YAHOO_SYMBOLS or (broker_is_native and "(otc)" in pair.casefold())
        broker_allowed = raja_pair_allowed_for_broker(broker, pair)
        if supported and broker_allowed and pair not in seen: pairs.append(pair); seen.add(pair)
    if not pairs:
        return jsonify({"status": "error", "message": "No supported pairs were supplied."}), 400

    # Strategy-only mode: news is informational; exact SK25 rules are never overridden by an indicator/news gate.
    options_key = (opts["mode"], opts["min_tf"], opts["min_agreement"], opts["min_score"], opts["vol_min"], opts["vol_max"])
    # Accuracy V24 includes user-specific pair/expiry history, so cached qualified
    # results must never cross user boundaries. Quotex bridge data is user-specific too.
    performance_cache_user = auth["user"]
    bridge_cache_signature = None
    if broker_is_native and all("(otc)" in str(pair).casefold() for pair in pairs):
        try:
            bridge_cache_signature = _broker_bridge_cache_signature(auth["user"], broker, pairs)
        except Exception:
            bridge_cache_signature = None
    key = (broker, performance_cache_user, selected_expiry, tuple(pairs), options_key, bridge_cache_signature); now = time.time()
    with batch_cache_lock:
        cached = batch_cache.get(key)
        if cached and (now - cached["timestamp"]) <= BATCH_CACHE_DURATION:
            payload = cached["payload"]
            found = any(r.get("signal") in {"CALL", "PUT"} for r in payload["data"])
            if batch_results_are_countable(payload["data"]):
                _append_scan_event(auth["user"], market, "AUTO", opts["mode"], found)
            return jsonify({"status": "success", "data": payload["data"], "diagnostics": payload["diagnostics"], "cache_hit": True})
    key_lock = _get_batch_key_lock(key)
    with key_lock:
        now = time.time()
        with batch_cache_lock:
            cached = batch_cache.get(key)
            if cached and (now - cached["timestamp"]) <= BATCH_CACHE_DURATION:
                payload = cached["payload"]
                found = any(r.get("signal") in {"CALL", "PUT"} for r in payload["data"])
                _append_scan_event(auth["user"], market, "AUTO", opts["mode"], found)
                return jsonify({"status": "success", "data": payload["data"], "diagnostics": payload["diagnostics"], "cache_hit": True})

        results_by_pair, timed_out_pairs = {}, []
        batch_started = time.time()

        # V29 fast exact-OTC path: if the unofficial native connector is disabled
        # (the recommended bridge-only setup), do not create workers for broker
        # pairs that the paired browser is not currently streaming.  This removes
        # the apparent "stuck on one pair" behaviour and makes the response time
        # proportional to READY pairs rather than the full configured market.
        compute_pairs = list(pairs)
        bridge_skipped_pairs = []
        broker_otc_batch = broker_is_native and all("(otc)" in str(pair).casefold() for pair in pairs)
        native_ready = False
        try:
            native_state = native_feed_status() if callable(native_feed_status) else {}
            native_row = (native_state or {}).get("pocket_option" if broker_key in {"pocketoption", "pocket_option", "pocket"} else "quotex") or {}
            native_ready = bool(native_row.get("enabled") and native_row.get("configured") and native_row.get("connected"))
        except Exception:
            native_ready = False

        if broker_otc_batch and not native_ready and RAJA_STRICT_BROKER_OTC:
            bridge_rows = _broker_bridge_ready_pairs(auth["user"], broker, pairs)
            bridge_meta = {row["pair"]: row for row in bridge_rows}
            compute_pairs = []
            source_label = "Pocket Option Browser Bridge" if broker_key in {"pocketoption", "pocket_option", "pocket"} else "Quotex Browser Bridge"
            required_bridge_candles = _sk25_required_base_candles(selected_expiry)
            for pair in pairs:
                meta = bridge_meta.get(pair)
                mode_ready = bool(
                    meta and meta.get("stream_fresh") and meta.get("market_fresh")
                    and int(meta.get("candle_count") or 0) >= required_bridge_candles
                )
                if mode_ready:
                    compute_pairs.append(pair)
                    continue
                bridge_skipped_pairs.append(pair)
                if meta:
                    reason = (
                        f"{source_label} cache is warming for {pair}: {int(meta.get('candle_count') or 0)}/{required_bridge_candles} "
                        f"exact 1m candles needed for {selected_expiry or '1m'} SK25 closed-candle mode; "
                        f"stream_fresh={bool(meta.get('stream_fresh'))}, market_fresh={bool(meta.get('market_fresh'))}."
                    )
                    data_age = meta.get("market_age_seconds")
                else:
                    reason = f"{source_label} is not streaming {pair} yet. Open/subscribe this OTC pair in the broker tab first."
                    data_age = None
                info = {
                    "source": source_label, "source_mode": "broker_otc_exact",
                    "provider_symbol": pair, "exact_broker_feed": True,
                    "browser_bridge": True, "backup_used": True,
                }
                row = no_signal_result(pair, reason, symbol=pair, data_age=data_age, source_info=info)
                row.update({
                    "data_delayed": True, "scan_paused": True,
                    "market_status": "WARMING" if meta else "UNAVAILABLE",
                    "data_status": "WARMING" if meta else "UNAVAILABLE",
                    "exclude_from_history": True, "exclude_from_performance": True,
                    "scan_skip_reason": "bridge_cache_warming" if meta else "bridge_pair_not_streaming",
                    "bridge_cache": meta or {},
                })
                results_by_pair[pair] = row

        workers = min(BATCH_SCAN_WORKERS, len(compute_pairs)) if compute_pairs else 0
        pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="raja-batch") if workers else None
        future_map = {pool.submit(calculate_live_strategy_signal, pair, selected_expiry, opts, auth["user"], broker): pair for pair in compute_pairs} if pool else {}
        done, pending = wait(future_map.keys(), timeout=BATCH_SCAN_DEADLINE_SECONDS) if future_map else (set(), set())
        for future in done:
            pair = future_map[future]
            try: results_by_pair[pair] = future.result()
            except Exception as exc:
                print(f"Batch scan error for {pair}: {exc}"); results_by_pair[pair] = no_signal_result(pair, "Scan worker failed for this pair.", symbol=YAHOO_SYMBOLS.get(pair))
        for future in pending:
            pair = future_map[future]; timed_out_pairs.append(pair); future.cancel()
            results_by_pair[pair] = no_signal_result(pair, "Skipped because the shared scan deadline was reached; next Auto Re-Scan will retry.", symbol=YAHOO_SYMBOLS.get(pair))
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)
        results = [results_by_pair[pair] for pair in pairs]
        stale_pairs = [r for r in results if r.get("source_stale")]
        delayed_pairs = [r for r in results if r.get("data_delayed")]
        usable_rows = [r for r in results if not r.get("source_stale") and not r.get("data_delayed")]
        data_available = sum(1 for r in usable_rows if r.get("data_age") is not None)
        data_unavailable = len(results) - data_available
        signals_found = sum(1 for r in results if r.get("signal") in {"CALL", "PUT"})
        elapsed = round(time.time() - batch_started, 2)
        all_market_data_blocked = bool(results) and not batch_results_are_countable(results)
        diagnostics = {
            "total_pairs": len(results),
            "completed_pairs": len(done) + len(bridge_skipped_pairs),
            "timed_out_pairs": timed_out_pairs,
            "timed_out_pairs_count": len(timed_out_pairs),
            "partial_response": bool(timed_out_pairs),
            "data_available": data_available,
            "data_unavailable": data_unavailable,
            "stale_pairs_count": len(stale_pairs),
            "delayed_pairs_count": len(delayed_pairs),
            "all_market_data_blocked": all_market_data_blocked,
            "signals_found": signals_found,
            "elapsed_seconds": elapsed,
            "batch_deadline_seconds": BATCH_SCAN_DEADLINE_SECONDS,
            "yahoo_request_timeout_seconds": YAHOO_REQUEST_TIMEOUT_SECONDS,
            "yahoo_fetch_concurrency": YAHOO_FETCH_CONCURRENCY,
            "batch_workers": workers,
            "bridge_ready_pairs_count": len(compute_pairs) if broker_otc_batch and not native_ready and RAJA_STRICT_BROKER_OTC else None,
            "bridge_skipped_pairs": bridge_skipped_pairs,
            "bridge_skipped_pairs_count": len(bridge_skipped_pairs),
            "bridge_mode_required_candles": (_sk25_required_base_candles(selected_expiry) if broker_otc_batch and not native_ready and RAJA_STRICT_BROKER_OTC else None),
            "scan_mode": opts["mode"],
        }
        payload = {"data": results, "diagnostics": diagnostics}
        with batch_cache_lock:
            batch_cache[key] = {"timestamp": time.time(), "payload": payload}
            if len(batch_cache) > 40:
                for old_key, _ in sorted(batch_cache.items(), key=lambda kv: kv[1]["timestamp"])[:10]: batch_cache.pop(old_key, None)
        if batch_results_are_countable(results):
            _append_scan_event(auth["user"], market, "AUTO", opts["mode"], signals_found > 0)
        return jsonify({"status": "success", "data": results, "diagnostics": diagnostics, "cache_hit": False})



@app.route("/live-chart-data", methods=["POST"])
def live_chart_data():
    """V68 live chart: Twelve Data exact-minute candles; signals use closed candles only."""
    data = request.get_json(silent=True) or {}
    auth, error = _auth_session(data)
    if error:
        return error

    selected_pair = str(data.get("pair") or "").strip()
    selected_expiry = str(data.get("expiry") or "1m").strip().lower()
    broker = str(data.get("broker") or "").strip()[:80]
    opts = normalize_scan_options(data.get("scan_options"))

    if not selected_pair or "Auto Scan Best Pair" in selected_pair:
        return jsonify({"status":"error","message":"Choose a specific pair to load the live chart."}), 400
    if not raja_pair_allowed_for_broker(broker, selected_pair):
        return jsonify({"status":"error","message":f"{selected_pair} is unavailable on the selected broker."}), 400

    try:
        chart_count = max(25, min(300, int(data.get("candle_count") or 50)))
    except Exception:
        chart_count = 50

    try:
        result = calculate_live_strategy_signal(
            selected_pair,
            selected_expiry,
            opts,
            auth["user"],
            broker,
            chart_count=chart_count,
        )
        return jsonify({
            "status":"success",
            "data":result,
            "read_only":True,
            "note":"V68: Twelve Data candles are aligned to exact minute boundaries. Signals use CLOSED candles only; the chart may include the current forming candle.",
        })
    except Exception as exc:
        # Always return JSON so the browser never receives Flask's HTML 500 page.
        return jsonify({
            "status":"error",
            "message":f"Live chart backend error: {type(exc).__name__}: {exc}",
        }), 500

@app.route("/scan", methods=["POST"])
def scan_markets():
    data = request.get_json(silent=True) or {}
    auth, error = _auth_session(data)
    if error: return error

    maintenance = scan_maintenance_state()
    if maintenance:
        return jsonify({
            "status": "error",
            "maintenance": True,
            "message": maintenance.get("maintenance_message") or "RAJA AI scans are temporarily paused.",
        }), 503

    selected_pair = str(data.get("pair", "")).strip(); selected_expiry = str(data.get("expiry", "")).strip()
    market = str(data.get("market") or "Unknown")[:80]; broker = str(data.get("broker") or "").strip()[:80]; opts = normalize_scan_options(data.get("scan_options"))
    if not selected_pair or "Auto Scan Best Pair" in selected_pair:
        return jsonify({"status": "error", "message": "Auto Scan must use /scan-batch with the selected market pair list."}), 400
    broker_key = broker.casefold().replace(" ", "")
    broker_otc = "(otc)" in selected_pair.casefold() and broker_key in {"quotex", "pocketoption", "pocket_option", "pocket"}
    if not raja_pair_allowed_for_broker(broker, selected_pair):
        return jsonify({"status": "error", "message": f"{selected_pair} is unavailable on the selected broker. Choose another pair or use Auto Scan.",
                        "data": no_signal_result(selected_pair, "Pair is not available on the selected broker; RAJA skipped it.")}), 400
    if selected_pair not in YAHOO_SYMBOLS and not broker_otc:
        return jsonify({"status": "error", "message": f"Unsupported pair: {selected_pair}",
                        "data": no_signal_result(selected_pair, "Pair is not configured for this broker/market.")}), 400

    # Pattern-only mode: exact closed-candle pattern decides the signal.
    result = calculate_live_strategy_signal(selected_pair, selected_expiry, opts, auth["user"], broker)
    if market_result_is_countable(result):
        _append_scan_event(
            auth["user"],
            market,
            selected_pair,
            opts["mode"],
            result.get("signal") in {"CALL", "PUT"},
        )
    return jsonify({"status": "success", "data": result})



# =========================================================
# TELEGRAM INTEGRATION SERVICES
# =========================================================

def issue_telegram_license(user_ref):
    """Issue or reuse an active web-compatible VIP key for an admin-approved Telegram user."""
    user = normalize_user_id(user_ref)
    if not user:
        raise ValueError("Telegram/Quotex user reference is required.")

    existing_key, _ = find_active_license_for_user(user)
    if existing_key:
        return existing_key

    while True:
        key = "RAJA-VIP-" + secrets.token_hex(4).upper() + "-2026"
        if load_license_record(key) is None:
            break

    record = {
        "active": True,
        "user": user,
        "device": None,
        "device_label": None,
        "session_token": None,
        "created_at": int(time.time()),
        "last_verified_at": None,
        "last_login_at": None,
        "plan": DEFAULT_LICENSE_PLAN,
        "expires_at": None,
    }
    save_license_record(key, record)
    return key


def validate_telegram_license(key, user_ref):
    """Validate an existing VIP key without consuming/binding a web device session."""
    key = str(key or "").strip()
    user = normalize_user_id(user_ref)
    if not key or not user:
        return False
    record = load_license_record(key)
    if not record or not record.get("active", False):
        return False
    # Important for short trials: Telegram access must stop when the license expires.
    if license_is_expired(record):
        return False
    bound_user = normalize_user_id(record.get("user", ""))
    return (not bound_user) or bound_user == user



def telegram_license_info(key, user_ref):
    """Return plan/expiry details for Telegram reminder and expiry automation."""
    key = str(key or "").strip()
    user = normalize_user_id(user_ref)
    record = load_license_record(key) if key else None
    if not isinstance(record, dict):
        return {"exists": False, "valid": False, "plan": None, "expires_at": None}
    bound_user = normalize_user_id(record.get("user", ""))
    user_matches = (not bound_user) or (bound_user == user)
    now = int(time.time())
    return {
        "exists": True,
        "valid": bool(record.get("active", False)) and user_matches and not license_is_expired(record, now),
        "plan": record.get("plan") or DEFAULT_LICENSE_PLAN,
        "expires_at": record.get("expires_at"),
        "active": bool(record.get("active", False)),
        "user_matches": user_matches,
    }


def telegram_scan_pair(pair, selected_expiry):
    # Telegram service in this project is Quotex-oriented. Passing the broker
    # prevents OTC scans from ever falling through to a reference feed.
    return calculate_live_strategy_signal(str(pair), str(selected_expiry), broker="Quotex")


def telegram_scan_auto(pairs, selected_expiry):
    """Run the same strict SK25 strategy-only analysis for Telegram Auto Best Pair."""
    pairs = [str(p).strip() for p in (pairs or []) if str(p).strip() in YAHOO_SYMBOLS][:40]
    if not pairs:
        return {"best": None, "diagnostics": {"total_pairs": 0, "data_available": 0}}

    workers = min(BATCH_SCAN_WORKERS, len(pairs))
    pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="raja-tg-scan")
    future_map = {pool.submit(calculate_live_strategy_signal, pair, selected_expiry, None, None, "Quotex"): pair for pair in pairs}
    done, pending = wait(future_map.keys(), timeout=BATCH_SCAN_DEADLINE_SECONDS)
    results = []
    for future in done:
        try:
            results.append(future.result())
        except Exception as exc:
            print(f"Telegram auto scan error for {future_map[future]}: {exc}")
    for future in pending:
        future.cancel()
    pool.shutdown(wait=False, cancel_futures=True)

    valid = [r for r in results if isinstance(r, dict) and r.get("signal") in {"CALL", "PUT"}]
    best = max(valid, key=lambda r: float(r.get("score") or 0), default=None)
    diagnostics = {
        "total_pairs": len(pairs),
        "completed_pairs": len(done),
        "timed_out_pairs_count": len(pending),
        "data_available": sum(1 for r in results if isinstance(r, dict) and r.get("data_age") is not None),
        "signals_found": len(valid),
    }
    return {"best": best, "diagnostics": diagnostics}


try:
    from telegram_bot import register_telegram_routes
    register_telegram_routes(app, {
        "issue_license": issue_telegram_license,
        "validate_license": validate_telegram_license,
        "license_info": telegram_license_info,
        "scan_pair": telegram_scan_pair,
        "scan_auto": telegram_scan_auto,
    })
except Exception as telegram_integration_error:
    # Telegram must never prevent the existing web bot from starting.
    print(f"Telegram integration disabled due to startup error: {telegram_integration_error}")

# =========================================================
# V73 HARD MARKET SOURCE POLICY — DUKASCOPY JFOREX ONLY
# =========================================================
#
# All normal LIVE market-candle requests are routed only to the Railway
# Dukascopy JForex bridge. Yahoo Finance, Twelve Data, OANDA and Finnhub
# market fallbacks are hard-disabled at runtime.
#
# OTC broker-synthetic instruments are NOT silently replaced by unrelated
# market/reference candles. If Dukascopy does not advertise the normalized
# instrument, the bot returns "unavailable" instead of switching provider.
# =========================================================

RAJA_MARKET_SOURCE_POLICY = "DUKASCOPY_JFOREX_ONLY_V73"
# V74 exception: normal Forex stays Dukascopy-only, while the seven Crypto Live
# pairs may fall back to Twelve Data when Dukascopy does not expose/serve them.
RAJA_CRYPTO_SOURCE_POLICY = "DUKASCOPY_THEN_TWELVE_DATA_V74"
CRYPTO_TWELVE_DATA_ENABLED = bool(TWELVE_DATA_API_KEY)

OANDA_ENABLED = False
# Keep the legacy/global Twelve Data switch hard-off so Forex/OTC code can never
# silently use it. Crypto Live uses the dedicated V74 function below instead.
TWELVE_DATA_ENABLED = False

def _v73_normalize_dukascopy_pair(pair):
    clean = str(pair or "").strip().upper()
    clean = clean.replace(" (OTC)", "").replace("(OTC)", "").strip()

    if clean in DUKASCOPY_CRYPTO_CANDIDATES:
        return str(DUKASCOPY_CRYPTO_CANDIDATES[clean]).upper()

    if clean == "XAUUSD":
        return "XAU/USD"
    if clean == "XAGUSD":
        return "XAG/USD"

    return clean


def _v73_dukascopy_unavailable(pair, bridge_pair, reason):
    bridge_pair = str(bridge_pair or "")
    return None, None, bridge_pair.replace("/", ""), {
        "source": "Dukascopy JForex BID",
        "source_mode": "dukascopy_only_unavailable",
        "provider_symbol": bridge_pair.replace("/", ""),
        "bridge_pair": bridge_pair,
        "backup_used": False,
        "yahoo_disabled": True,
        "twelve_data_disabled": True,
        "oanda_disabled": True,
        "finnhub_disabled": True,
        "market_source_policy": RAJA_MARKET_SOURCE_POLICY,
        "unavailable_reason": str(reason),
    }


def _v73_get_dukascopy(pair, count=1500):
    bridge_pair = _v73_normalize_dukascopy_pair(pair)

    if not bridge_pair:
        return _v73_dukascopy_unavailable(pair, "", "Empty instrument name.")

    if not DUKASCOPY_BRIDGE_URL:
        return _v73_dukascopy_unavailable(
            pair,
            bridge_pair,
            "DUKASCOPY_BRIDGE_URL is not configured in the AI-BOT Railway service.",
        )

    supported = _dukascopy_bridge_pairs(force=False)

    if supported and bridge_pair not in supported:
        return _v73_dukascopy_unavailable(
            pair,
            bridge_pair,
            f"{bridge_pair} is not advertised by the connected Dukascopy bridge.",
        )

    data, age, symbol, info = _dukascopy_bridge_market_data(
        bridge_pair,
        count=max(25, min(1500, int(count))),
        provider_pair=bridge_pair,
    )

    info = dict(info or {})
    info.update({
        "source": "Dukascopy JForex BID",
        "market_source_policy": RAJA_MARKET_SOURCE_POLICY,
        "yahoo_disabled": True,
        "twelve_data_disabled": True,
        "oanda_disabled": True,
        "finnhub_disabled": True,
        "backup_used": False,
    })
    return data, age, symbol, info


def _v74_crypto_twelve_data_market_data(pair, force=False):
    """Dedicated Twelve Data fallback for the seven normal Crypto Live pairs only.

    This deliberately ignores the global TWELVE_DATA_ENABLED kill switch so
    normal Forex remains Dukascopy-only under V73.
    """
    clean = str(pair or "").strip().upper()
    provider_symbol = TWELVE_DATA_SYMBOLS.get(clean)

    if clean not in TWELVE_DATA_CRYPTO_LIVE_PAIRS:
        return None, None, provider_symbol or clean, {
            "source": "Twelve Data",
            "source_mode": "crypto_fallback_not_allowed",
            "provider_symbol": provider_symbol,
            "backup_used": False,
            "crypto_source_policy": RAJA_CRYPTO_SOURCE_POLICY,
            "unavailable_reason": "Twelve Data fallback is restricted to Crypto Live pairs.",
        }

    if not CRYPTO_TWELVE_DATA_ENABLED or not TWELVE_DATA_API_KEY:
        return None, None, provider_symbol or clean, {
            "source": "Twelve Data",
            "source_mode": "crypto_twelve_data_not_configured",
            "provider_symbol": provider_symbol,
            "backup_used": True,
            "crypto_source_policy": RAJA_CRYPTO_SOURCE_POLICY,
            "unavailable_reason": "TWELVE_DATA_API_KEY is not configured in Railway.",
        }

    if not provider_symbol:
        return None, None, clean, {
            "source": "Twelve Data",
            "source_mode": "crypto_twelve_data_symbol_missing",
            "provider_symbol": None,
            "backup_used": True,
            "crypto_source_policy": RAJA_CRYPTO_SOURCE_POLICY,
            "unavailable_reason": f"No Twelve Data symbol is configured for {clean}.",
        }

    cached = _twelve_data_cache_get(provider_symbol, allow_stale=False)
    if cached is not None and not force:
        age = _source_candle_age_seconds(cached)
        return cached, age, provider_symbol, {
            "source": "Twelve Data",
            "source_mode": "crypto_twelve_data_cache",
            "provider_symbol": provider_symbol,
            "feed_quality": _market_candle_quality(cached),
            "backup_used": True,
            "credentials_required": True,
            "crypto_source_policy": RAJA_CRYPTO_SOURCE_POLICY,
        }

    # Reuse the existing Twelve Data cache/rate-limit infrastructure, but make
    # the HTTP request here so the global legacy kill switch stays OFF.
    if not _twelve_data_global_allowed():
        stale = _twelve_data_cache_get(provider_symbol, allow_stale=True)
        if stale is not None:
            age = _source_candle_age_seconds(stale)
            return stale, age, provider_symbol, {
                "source": "Twelve Data",
                "source_mode": "crypto_twelve_data_stale_cache",
                "provider_symbol": provider_symbol,
                "feed_quality": _market_candle_quality(stale),
                "backup_used": True,
                "credentials_required": True,
                "crypto_source_policy": RAJA_CRYPTO_SOURCE_POLICY,
                "quality_warning": "Twelve Data rate limit/cooldown active; using cached Crypto Live candles.",
            }

    symbol_lock = _get_twelve_data_symbol_lock(provider_symbol)
    acquired = symbol_lock.acquire(timeout=min(12.0, TWELVE_DATA_REQUEST_TIMEOUT_SECONDS + 2.0))
    if not acquired:
        stale = _twelve_data_cache_get(provider_symbol, allow_stale=True)
        if stale is not None:
            age = _source_candle_age_seconds(stale)
            return stale, age, provider_symbol, {
                "source": "Twelve Data",
                "source_mode": "crypto_twelve_data_busy_cache",
                "provider_symbol": provider_symbol,
                "feed_quality": _market_candle_quality(stale),
                "backup_used": True,
                "credentials_required": True,
                "crypto_source_policy": RAJA_CRYPTO_SOURCE_POLICY,
            }
        return None, None, provider_symbol, {
            "source": "Twelve Data",
            "source_mode": "crypto_twelve_data_busy",
            "provider_symbol": provider_symbol,
            "backup_used": True,
            "crypto_source_policy": RAJA_CRYPTO_SOURCE_POLICY,
            "unavailable_reason": "Crypto Twelve Data fetch is busy; retry shortly.",
        }

    try:
        cached = _twelve_data_cache_get(provider_symbol, allow_stale=False)
        if cached is not None and not force:
            age = _source_candle_age_seconds(cached)
            return cached, age, provider_symbol, {
                "source": "Twelve Data",
                "source_mode": "crypto_twelve_data_cache",
                "provider_symbol": provider_symbol,
                "feed_quality": _market_candle_quality(cached),
                "backup_used": True,
                "credentials_required": True,
                "crypto_source_policy": RAJA_CRYPTO_SOURCE_POLICY,
            }

        acquired_slot = twelve_data_fetch_semaphore.acquire(timeout=TWELVE_DATA_REQUEST_TIMEOUT_SECONDS + 2.0)
        if not acquired_slot:
            raise RuntimeError("Twelve Data fetch slot unavailable")

        try:
            _pace_twelve_data_request()
            params = {
                "symbol": provider_symbol,
                "interval": "1min",
                "outputsize": TWELVE_DATA_OUTPUTSIZE,
                "timezone": "UTC",
                "apikey": TWELVE_DATA_API_KEY,
            }
            url = TWELVE_DATA_BASE_URL + "?" + urlencode(params)
            req = UrlRequest(url, headers={"User-Agent": "RAJA-AI/74", "Accept": "application/json"})
            with urlopen(req, timeout=TWELVE_DATA_REQUEST_TIMEOUT_SECONDS) as response:
                payload = json.loads(response.read().decode("utf-8", errors="replace"))
        finally:
            twelve_data_fetch_semaphore.release()

        if not isinstance(payload, dict) or str(payload.get("status") or "").lower() == "error":
            code = payload.get("code") if isinstance(payload, dict) else None
            _twelve_data_mark_failure(provider_symbol, code)
            message = str(payload.get("message") or "API returned an error") if isinstance(payload, dict) else "Invalid response"
            raise RuntimeError(message)

        df = _parse_twelve_data_frame(payload)
        if df is None or getattr(df, "empty", True):
            _twelve_data_mark_failure(provider_symbol)
            raise RuntimeError("Twelve Data returned no usable 1-minute crypto candles")

        with twelve_data_cache_lock:
            twelve_data_cache[provider_symbol] = {"data": df.copy(), "timestamp": time.time()}
        with twelve_data_failed_lock:
            twelve_data_failed_until.pop(provider_symbol, None)

        age = _source_candle_age_seconds(df)
        return df.copy(), age, provider_symbol, {
            "source": "Twelve Data",
            "source_mode": "crypto_twelve_data_fallback",
            "provider_symbol": provider_symbol,
            "feed_quality": _market_candle_quality(df),
            "backup_used": True,
            "credentials_required": True,
            "twelve_data_primary": False,
            "crypto_source_policy": RAJA_CRYPTO_SOURCE_POLICY,
        }
    except HTTPError as exc:
        _twelve_data_mark_failure(provider_symbol, getattr(exc, "code", None))
        reason = f"Twelve Data HTTP {getattr(exc, 'code', '?')}"
    except URLError as exc:
        _twelve_data_mark_failure(provider_symbol)
        reason = f"Twelve Data network error: {getattr(exc, 'reason', exc)}"
    except Exception as exc:
        _twelve_data_mark_failure(provider_symbol)
        reason = f"Twelve Data error: {type(exc).__name__}: {exc}"
    finally:
        symbol_lock.release()

    stale = _twelve_data_cache_get(provider_symbol, allow_stale=True)
    if stale is not None:
        age = _source_candle_age_seconds(stale)
        return stale, age, provider_symbol, {
            "source": "Twelve Data",
            "source_mode": "crypto_twelve_data_error_cache",
            "provider_symbol": provider_symbol,
            "feed_quality": _market_candle_quality(stale),
            "backup_used": True,
            "credentials_required": True,
            "crypto_source_policy": RAJA_CRYPTO_SOURCE_POLICY,
            "quality_warning": reason,
        }

    return None, None, provider_symbol, {
        "source": "Twelve Data",
        "source_mode": "crypto_twelve_data_error",
        "provider_symbol": provider_symbol,
        "backup_used": True,
        "credentials_required": True,
        "crypto_source_policy": RAJA_CRYPTO_SOURCE_POLICY,
        "unavailable_reason": reason,
    }



# =========================================================
# BIQUOTE LIVE 1-MINUTE FOREX FEED
# Public read API; no API key required. BiQuote is primary for normal
# live Forex/metals while Dukascopy remains as temporary fallback.
# =========================================================
BIQUOTE_API_URL = (os.environ.get("BIQUOTE_API_URL") or "https://biquote.io/api").strip().rstrip("/")
BIQUOTE_ENABLED = str(os.environ.get("BIQUOTE_ENABLED", "1")).strip().lower() not in {"0", "false", "off", "no"}
# Keep BiQuote fast-fail so a slow/unreachable public endpoint can never hold the
# whole RAJA web app for many seconds.  Dukascopy remains the immediate fallback.
BIQUOTE_REQUEST_TIMEOUT_SECONDS = max(1.5, min(6.0, float(os.environ.get("BIQUOTE_REQUEST_TIMEOUT", "2.5"))))
BIQUOTE_TICK_TIMEOUT_SECONDS = max(1.0, min(5.0, float(os.environ.get("BIQUOTE_TICK_TIMEOUT", "1.5"))))
BIQUOTE_CANDLE_LIMIT = max(150, min(600, int(os.environ.get("BIQUOTE_CANDLE_LIMIT", "300"))))
BIQUOTE_CACHE_SECONDS = max(1.0, min(15.0, float(os.environ.get("BIQUOTE_CACHE_SECONDS", "3"))))
BIQUOTE_FAILURE_COOLDOWN_SECONDS = max(3.0, min(60.0, float(os.environ.get("BIQUOTE_FAILURE_COOLDOWN", "15"))))
BIQUOTE_TICK_FAILURE_COOLDOWN_SECONDS = max(2.0, min(30.0, float(os.environ.get("BIQUOTE_TICK_FAILURE_COOLDOWN", "5"))))
_biquote_cache = {}
_biquote_cache_lock = threading.RLock()
_biquote_failed_until = {}
_biquote_failed_reason = {}
_biquote_failed_lock = threading.RLock()
_biquote_tick_failed_until = {}
_biquote_tick_failed_reason = {}
_biquote_tick_failed_lock = threading.RLock()


def _biquote_symbol(pair):
    raw = str(pair or "").strip().upper()
    if "(OTC)" in raw or not raw:
        return None
    return raw.replace("/", "").replace("-", "")


def _biquote_json(path, params=None, timeout=None):
    if not BIQUOTE_ENABLED or not BIQUOTE_API_URL:
        raise RuntimeError("BiQuote is disabled or BIQUOTE_API_URL is not configured")
    query = urlencode(params or {})
    url = f"{BIQUOTE_API_URL}/{str(path).lstrip('/')}"
    if query:
        url += "?" + query
    req = UrlRequest(
        url,
        headers={"User-Agent": "RAJA-AI-BiQuote/1.0", "Accept": "application/json"},
        method="GET",
    )
    request_timeout = BIQUOTE_REQUEST_TIMEOUT_SECONDS if timeout is None else float(timeout)
    with urlopen(req, timeout=request_timeout) as response:
        return json.loads(response.read().decode("utf-8", errors="replace"))


def _biquote_market_data(pair, count=300):
    """Fetch BiQuote 1m OHLC candles and normalize them to RAJA columns."""
    symbol = _biquote_symbol(pair)
    if not symbol:
        return None, None, None, {"source": "BiQuote", "source_mode": "biquote_unsupported_pair", "provider_symbol": None}
    now = time.time()
    key = (symbol, int(count))

    # Circuit breaker: after a failed BiQuote request, go straight to Dukascopy
    # for a short period instead of making every browser poll wait for the timeout.
    with _biquote_failed_lock:
        blocked_until = float(_biquote_failed_until.get(symbol) or 0.0)
        if now < blocked_until:
            reason = _biquote_failed_reason.get(symbol) or "BiQuote temporarily bypassed after a recent failure."
            raise RuntimeError(f"BiQuote cooldown active for {symbol}: {reason}")

    with _biquote_cache_lock:
        cached = _biquote_cache.get(key)
        if cached and now - float(cached.get("ts") or 0) <= BIQUOTE_CACHE_SECONDS:
            df = cached["df"].copy()
            return df, _source_candle_age_seconds(df), symbol, dict(cached["info"])

    try:
        payload = _biquote_json(
            f"{symbol}/ohlc",
            {"interval": "1m", "limit": max(150, min(600, int(count)))},
            timeout=BIQUOTE_REQUEST_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        reason = f"{type(exc).__name__}: {exc}"
        with _biquote_failed_lock:
            _biquote_failed_until[symbol] = time.time() + BIQUOTE_FAILURE_COOLDOWN_SECONDS
            _biquote_failed_reason[symbol] = reason[:220]
        raise
    bars = payload.get("bars") if isinstance(payload, dict) else None
    if not bars:
        raise RuntimeError(f"BiQuote returned no 1-minute candles for {symbol}")
    pd = _get_pandas()
    rows=[]
    for bar in bars:
        if not isinstance(bar, dict):
            continue
        try:
            ts=pd.to_datetime(bar.get("openTime"), utc=True)
            rows.append({"time":ts,"Open":float(bar["open"]),"High":float(bar["high"]),"Low":float(bar["low"]),"Close":float(bar["close"]),"Volume":float(bar.get("tickVolume") or bar.get("volume") or 0)})
        except Exception:
            continue
    if len(rows)<20:
        raise RuntimeError(f"BiQuote returned only {len(rows)} usable 1-minute candles for {symbol}")
    df=pd.DataFrame(rows).set_index("time").sort_index()
    df=df[["Open","High","Low","Close","Volume"]]
    age=_source_candle_age_seconds(df)
    info={"source":"BiQuote","source_mode":"biquote_1m_live","provider_symbol":symbol,"feed_quality":_market_candle_quality(df),"backup_used":False,"exact_broker_feed":False,"credentials_required":False,"biquote_api":True,"biquote_interval":"1m"}
    with _biquote_cache_lock:
        _biquote_cache[key]={"ts":now,"df":df.copy(),"info":dict(info)}
    # A successful response clears any previous circuit-breaker state.
    with _biquote_failed_lock:
        _biquote_failed_until.pop(symbol, None)
        _biquote_failed_reason.pop(symbol, None)
    return df, age, symbol, info


def _biquote_tick(pair):
    symbol=_biquote_symbol(pair)
    if not symbol:
        return None

    now = time.time()
    with _biquote_tick_failed_lock:
        blocked_until = float(_biquote_tick_failed_until.get(symbol) or 0.0)
        if now < blocked_until:
            reason = _biquote_tick_failed_reason.get(symbol) or "BiQuote tick temporarily bypassed."
            raise RuntimeError(f"BiQuote tick cooldown active for {symbol}: {reason}")

    try:
        payload=_biquote_json(symbol, timeout=BIQUOTE_TICK_TIMEOUT_SECONDS)
    except Exception as exc:
        reason = f"{type(exc).__name__}: {exc}"
        with _biquote_tick_failed_lock:
            _biquote_tick_failed_until[symbol] = time.time() + BIQUOTE_TICK_FAILURE_COOLDOWN_SECONDS
            _biquote_tick_failed_reason[symbol] = reason[:220]
        raise
    if not isinstance(payload, dict):
        return None
    payload["provider_pair"]=symbol
    payload["tick_supported"]=True
    payload["source"]="BiQuote"
    payload["source_mode"]="biquote_live_tick"
    with _biquote_tick_failed_lock:
        _biquote_tick_failed_until.pop(symbol, None)
        _biquote_tick_failed_reason.pop(symbol, None)
    return payload

def get_market_data(pair, bridge_user=None, broker=None):
    clean = str(pair or "").strip()
    upper = clean.upper()
    is_otc = "(OTC)" in upper

    # BiQuote primary for normal LIVE Forex/metals. OTC stays on the existing
    # broker-native path. Dukascopy remains a temporary fallback.
    is_normal_live = (not is_otc and "/" in upper and upper not in set(DUKASCOPY_CRYPTO_CANDIDATES.values())) or upper == "XAUUSD"
    if is_normal_live and BIQUOTE_ENABLED:
        try:
            data, age, symbol, info = _biquote_market_data(clean, count=BIQUOTE_CANDLE_LIMIT)
            if data is not None and not getattr(data, "empty", True):
                return data, age, symbol, info
            biquote_reason = "BiQuote returned no usable candles."
        except Exception as exc:
            biquote_reason = f"BiQuote: {type(exc).__name__}: {exc}"
        data, age, symbol, info = _v73_get_dukascopy(clean, count=1500)
        info = dict(info or {})
        info["biquote_attempted"] = True
        info["biquote_fallback_reason"] = biquote_reason
        return data, age, symbol, info

    # Existing crypto policy is preserved.
    if upper in TWELVE_DATA_CRYPTO_LIVE_PAIRS:
        bridge_pair = _v73_normalize_dukascopy_pair(upper)
        supported = _dukascopy_bridge_pairs(force=False) if DUKASCOPY_BRIDGE_URL else set()
        dukascopy_reason = None
        if bridge_pair and bridge_pair in supported:
            data, age, symbol, info = _v73_get_dukascopy(upper, count=1500)
            if data is not None and not getattr(data, "empty", True):
                info = dict(info or {})
                info["crypto_source_policy"] = RAJA_CRYPTO_SOURCE_POLICY
                return data, age, symbol, info
            dukascopy_reason = str((info or {}).get("unavailable_reason") or "Dukascopy crypto candles unavailable")
        elif bridge_pair:
            dukascopy_reason = f"{bridge_pair} is not advertised by the connected Dukascopy bridge."
        data, age, symbol, info = _v74_crypto_twelve_data_market_data(upper, force=False)
        info = dict(info or {})
        if dukascopy_reason:
            info["dukascopy_attempt_reason"] = dukascopy_reason
        return data, age, symbol, info

    return _v73_get_dukascopy(clean, count=1500)


@app.route("/live-chart-tick", methods=["POST"])
def live_chart_tick():
    """Live tick proxy: BiQuote primary for normal LIVE Forex/metals."""
    payload = request.get_json(silent=True) or {}
    auth, error = _auth_session(payload)
    if error:
        return error
    pair = str(payload.get("pair") or "").strip()
    if not pair or "Auto Scan Best Pair" in pair:
        return jsonify({"status":"error","message":"Choose a specific pair for live ticks."}), 400
    upper=pair.upper()
    is_normal_live=(not "(OTC)" in upper and "/" in upper) or upper=="XAUUSD"
    biquote_error="BiQuote tick not used for this pair."
    if is_normal_live and BIQUOTE_ENABLED:
        try:
            tick=_biquote_tick(pair)
            if tick:
                return jsonify({"status":"success","data":tick})
            biquote_error="BiQuote returned no tick."
        except Exception as exc:
            biquote_error=f"BiQuote tick: {type(exc).__name__}: {exc}"

    bridge_pair=_v73_normalize_dukascopy_pair(pair)
    if not bridge_pair:
        return jsonify({"status":"success","data":{"pair":pair,"provider_pair":None,"tick_supported":False,"source":"BiQuote","reason":biquote_error}})
    if upper in TWELVE_DATA_CRYPTO_LIVE_PAIRS:
        supported=_dukascopy_bridge_pairs(force=False) if DUKASCOPY_BRIDGE_URL else set()
        if bridge_pair not in supported:
            return jsonify({"status":"success","data":{"pair":pair,"provider_pair":bridge_pair,"tick_supported":False,"source":"Twelve Data minute fallback","reason":biquote_error}})
    if not DUKASCOPY_BRIDGE_URL:
        return jsonify({"status":"success","data":{"pair":pair,"provider_pair":bridge_pair,"tick_supported":False,"source":"Dukascopy JForex BID","reason":biquote_error}})
    try:
        tick=_dukascopy_bridge_json("/tick", {"pair":bridge_pair})
        if not isinstance(tick,dict):
            raise RuntimeError("Bridge returned an invalid tick payload")
        tick["tick_supported"]=True
        tick["provider_pair"]=bridge_pair
        tick["fallback_after_biquote"]=True
        return jsonify({"status":"success","data":tick})
    except Exception as exc:
        return jsonify({"status":"success","data":{"pair":pair,"provider_pair":bridge_pair,"tick_supported":False,"source":"Dukascopy JForex BID","reason":f"BiQuote unavailable ({biquote_error}); Dukascopy tick unavailable: {type(exc).__name__}: {exc}"}})


@app.route("/biquote-status", methods=["GET"])
def biquote_status():
    now = time.time()
    with _biquote_failed_lock:
        market_cooldowns = {
            k: round(max(0.0, float(v) - now), 2)
            for k, v in _biquote_failed_until.items()
            if float(v) > now
        }
        market_errors = {
            k: v for k, v in _biquote_failed_reason.items()
            if k in market_cooldowns
        }
    with _biquote_tick_failed_lock:
        tick_cooldowns = {
            k: round(max(0.0, float(v) - now), 2)
            for k, v in _biquote_tick_failed_until.items()
            if float(v) > now
        }
    return jsonify({
        "status": "configured" if BIQUOTE_ENABLED and BIQUOTE_API_URL else "disabled",
        "enabled": bool(BIQUOTE_ENABLED),
        "api_url": BIQUOTE_API_URL,
        "interval": "1m",
        "api_key_required": False,
        "request_timeout_seconds": BIQUOTE_REQUEST_TIMEOUT_SECONDS,
        "tick_timeout_seconds": BIQUOTE_TICK_TIMEOUT_SECONDS,
        "candle_limit": BIQUOTE_CANDLE_LIMIT,
        "cache_seconds": BIQUOTE_CACHE_SECONDS,
        "failure_cooldown_seconds": BIQUOTE_FAILURE_COOLDOWN_SECONDS,
        "market_cooldowns": market_cooldowns,
        "market_errors": market_errors,
        "tick_cooldowns": tick_cooldowns,
    }), 200


# Legacy-provider hard kill switches.
def fetch_yahoo_1m(symbol):
    return None


def update_symbol_cache(symbol, force=False):
    return False


def fetch_twelve_data_1m(pair):
    clean = str(pair or "").strip().upper()
    if clean in TWELVE_DATA_CRYPTO_LIVE_PAIRS:
        data, _age, symbol, _info = _v74_crypto_twelve_data_market_data(clean, force=False)
        return data, symbol
    provider_symbol = TWELVE_DATA_SYMBOLS.get(str(pair or "").strip())
    return None, provider_symbol


def get_twelve_data_market_data(pair, force=False):
    clean = str(pair or "").strip().upper()
    if clean in TWELVE_DATA_CRYPTO_LIVE_PAIRS:
        data, _age, symbol, _info = _v74_crypto_twelve_data_market_data(clean, force=force)
        return data, symbol
    data, _age, _symbol, _info = _v73_get_dukascopy(pair, count=1500)
    return data, _v73_normalize_dukascopy_pair(pair)


def _twelve_data_only_market_data(pair, force=False):
    clean = str(pair or "").strip().upper()
    if clean in TWELVE_DATA_CRYPTO_LIVE_PAIRS:
        return _v74_crypto_twelve_data_market_data(clean, force=force)
    return _v73_get_dukascopy(pair, count=1500)


def _fetch_yahoo_1m(pair, count=1200):
    return _v73_get_dukascopy(pair, count=count)


def _fetch_oanda_1m(pair, count=1200):
    return _v73_get_dukascopy(pair, count=count)


def _finnhub_fallback_seed_1m(pair, count=1600):
    data, _age, _symbol, info = _v73_get_dukascopy(pair, count=count)
    if data is None or getattr(data, "empty", True):
        return None, str((info or {}).get("unavailable_reason") or "Dukascopy unavailable")
    return data, "Dukascopy JForex BID"


def _finnhub_live_base_df(pair, count=1800):
    return _v73_get_dukascopy(pair, count=count)


def resolve_tracked_signal(item):
    pair = str(item.get("pair") or "").strip()
    if not pair:
        return False

    data, _age, _symbol, source_info = get_market_data(pair)
    if data is None or getattr(data, "empty", True):
        item["result_wait_reason"] = (
            (source_info or {}).get("unavailable_reason")
            or "Dukascopy result candles are not available yet."
        )
        return False

    rows = dataframe_epoch_rows(data)
    if not rows:
        return False

    entry_epoch = int(item.get("entry_epoch", 0))
    expiry_epoch = int(item.get("expiry_epoch", 0))
    entry_candidates = [
        (epoch, row)
        for epoch, row in rows
        if entry_epoch <= epoch < entry_epoch + 120
    ]
    exit_candidates = [
        (epoch, row)
        for epoch, row in rows
        if max(entry_epoch, expiry_epoch - 120) <= epoch < expiry_epoch
    ]
    if not entry_candidates or not exit_candidates:
        return False

    entry_epoch_actual, entry_row = entry_candidates[0]
    exit_epoch_actual, exit_row = exit_candidates[-1]

    try:
        entry_price = float(entry_row["Open"])
        exit_price = float(exit_row["Close"])
    except Exception:
        return False

    direction = item.get("signal")
    if exit_price == entry_price:
        result = "DRAW"
    elif direction == "CALL":
        result = "WIN" if exit_price > entry_price else "LOSS"
    elif direction == "PUT":
        result = "WIN" if exit_price < entry_price else "LOSS"
    else:
        return False

    item["entry_price"] = round(entry_price, 8)
    item["exit_price"] = round(exit_price, 8)
    item["entry_epoch_actual"] = int(entry_epoch_actual)
    item["exit_epoch_actual"] = int(exit_epoch_actual)
    item["result"] = result
    item["resolved_source"] = str((source_info or {}).get("source") or "Dukascopy JForex BID")
    item["resolved_at"] = int(time.time())
    return True


@app.route("/dukascopy-only-status", methods=["GET"])
def dukascopy_only_status():
    health = None
    err = None

    try:
        health = _dukascopy_bridge_json("/health")
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"

    try:
        pairs = sorted(_dukascopy_bridge_pairs(force=True)) if DUKASCOPY_BRIDGE_URL else []
    except Exception as exc:
        pairs = []
        if not err:
            err = f"{type(exc).__name__}: {exc}"

    return jsonify({
        "status": "success" if health else "error",
        "build": "V73",
        "market_source_policy": RAJA_MARKET_SOURCE_POLICY,
        "market_source": "BiQuote 1M primary + Dukascopy fallback",
        "configured": bool(DUKASCOPY_BRIDGE_URL),
        "health": health,
        "pair_count": len(pairs),
        "pairs": pairs,
        "yahoo_enabled": False,
        "twelve_data_enabled": False,
        "oanda_enabled": False,
        "finnhub_enabled": False,
        "fallback_enabled": False,
        "error": err,
    }), (200 if health else 503)


@app.route("/dukascopy-test/<path:pair>", methods=["GET"])
def dukascopy_test_pair(pair):
    data, age, symbol, info = _v73_get_dukascopy(pair, count=50)
    rows = 0 if data is None else len(data)
    return jsonify({
        "pair": pair,
        "normalized_pair": _v73_normalize_dukascopy_pair(pair),
        "rows": int(rows),
        "age_seconds": age,
        "provider_symbol": symbol,
        "source_info": info,
    }), (200 if rows else 503)

signal_worker_thread = threading.Thread(
    target=signal_outcome_worker,
    daemon=True,
)
signal_worker_thread.start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        threaded=True,
    )



# RAJA V74 · FOREX DUKASCOPY-ONLY + CRYPTO TWELVE FALLBACK · 2026-09-10
# FORCE RAILWAY FRESH DEPLOY · 2026-09-10

# RAJA V75 · STARTUP SIGNAL WARM-UP BYPASS · 2026-09-10

# RAJA V76 · BALANCED SIGNAL MODE + HYBRID INDICATOR FALLBACK · 2026-09-10
