"""RAJA AI native broker market-data adapters.

Purpose: obtain broker-native OTC candle/tick data without requiring the browser bridge.
The browser bridge remains a fallback in bot.py. This module never places orders.

Quotex adapter: pyquotex (persistent websocket session).
Pocket Option adapter: chema-creator/PocketOptionApi (persistent websocket session).

Both third-party clients are unofficial and can break when broker protocols change.
Secrets must be supplied through environment variables, never committed to source.
"""
from __future__ import annotations

import asyncio
import os
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Any


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() not in {"0", "false", "no", "off", ""}


def _clean_pair(pair: str) -> str:
    return str(pair or "").replace(" (OTC)", "").strip()


# Common symbols. Dynamic broker catalogs are preferred when available.
_CRYPTO_CODES = {
    "Bitcoin": "BTCUSD_otc",
    "Ethereum": "ETHUSD_otc",
    "BNB": "BNBUSD_otc",
    "Cardano": "ADAUSD_otc",
    "Polkadot": "DOTUSD_otc",
    "Polygon": "MATICUSD_otc",
    "TRON": "TRXUSD_otc",
    "Avalanche": "AVAXUSD_otc",
    "Solana": "SOLUSD_otc",
    "Chainlink": "LINKUSD_otc",
    "Litecoin": "LTCUSD_otc",
    "Dogecoin": "DOGEUSD_otc",
    "Toncoin": "TONUSD_otc",
}

_STOCK_HINTS = {
    "Apple": ["AAPL_otc", "Apple_otc"],
    "American Express": ["AXP_otc", "AmericanExpress_otc"],
    "Boeing Company": ["BA_otc", "Boeing_otc"],
    "Cisco": ["CSCO_otc", "Cisco_otc"],
    "Facebook Inc": ["META_otc", "FB_otc", "Facebook_otc"],
    "Intel": ["INTC_otc", "Intel_otc"],
    "Johnson & Johnson": ["JNJ_otc", "JohnsonJohnson_otc"],
    "McDonald's": ["MCD_otc", "McDonalds_otc"],
    "Microsoft": ["MSFT_otc", "Microsoft_otc"],
    "Pfizer Inc": ["PFE_otc", "Pfizer_otc"],
    "Tesla": ["TSLA_otc", "Tesla_otc"],
    "ExxonMobil": ["XOM_otc", "ExxonMobil_otc"],
    "Advanced Micro Devices": ["AMD_otc", "AdvancedMicroDevices_otc"],
}


def default_asset_candidates(pair: str) -> list[str]:
    """Return likely broker symbol candidates for normal LIVE or OTC assets."""
    raw = str(pair or "").strip()
    is_otc = "(otc)" in raw.casefold()
    base = _clean_pair(raw)

    # Normal Forex: broker-native symbols are normally compact without _otc.
    if "/" in base:
        compact = base.replace("/", "")
        if is_otc:
            return [compact + "_otc", compact]
        return [compact, compact.lower(), compact.upper()]

    # Crypto/stocks: keep the existing OTC hints, but prefer non-OTC form
    # when the user selected a normal LIVE market.
    if base in _CRYPTO_CODES:
        otc_code = _CRYPTO_CODES[base]
        normal_code = otc_code[:-4] if otc_code.casefold().endswith("_otc") else otc_code
        return [otc_code, normal_code] if is_otc else [normal_code, otc_code]
    if base == "Bitcoin ETF":
        vals = ["BITB_otc", "IBIT_otc", "BTCETF_otc"]
        return vals if is_otc else [x[:-4] for x in vals] + vals
    if base in _STOCK_HINTS:
        vals = list(_STOCK_HINTS[base])
        if is_otc:
            return vals
        normal = [x[:-4] if x.casefold().endswith("_otc") else x for x in vals]
        return normal + vals

    compact = re.sub(r"[^A-Za-z0-9]", "", base)
    if not compact:
        return []
    return [compact + "_otc", compact] if is_otc else [compact, compact + "_otc"]

def _normalize_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def resolve_from_catalog(pair: str, catalog: Any) -> str | None:
    """Resolve RAJA display pair to broker symbol using a broker asset catalog."""
    candidates = default_asset_candidates(pair)
    if not isinstance(catalog, dict) or not catalog:
        return candidates[0] if candidates else None

    # Exact symbol hit first.
    for candidate in candidates:
        if candidate in catalog:
            return candidate
        for key in catalog:
            if str(key).casefold() == candidate.casefold():
                return str(key)

    wanted = _normalize_name(_clean_pair(pair))
    if not wanted:
        return candidates[0] if candidates else None

    # Match key or display name. Require OTC-looking symbol for OTC requests.
    best: tuple[int, str] | None = None
    for key, row in catalog.items():
        key_s = str(key)
        row = row if isinstance(row, dict) else {}
        labels = [key_s, row.get("name"), row.get("symbol"), row.get("title")]
        score = 0
        for label in labels:
            norm = _normalize_name(label)
            if not norm:
                continue
            if norm == wanted or norm == wanted + "otc":
                score = max(score, 100)
            elif wanted in norm or norm in wanted:
                score = max(score, 60)
        wants_otc = "(otc)" in str(pair).casefold()
        is_key_otc = "otc" in key_s.casefold()
        if wants_otc and is_key_otc:
            score += 25
        elif (not wants_otc) and (not is_key_otc):
            score += 25
        elif wants_otc != is_key_otc:
            score -= 35
        if score and (best is None or score > best[0]):
            best = (score, key_s)
    if best and best[0] >= 70:
        return best[1]
    return candidates[0] if candidates else None


def _epoch(value: Any) -> float | None:
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    if x > 10_000_000_000_000:
        x /= 1_000_000.0
    elif x > 10_000_000_000:
        x /= 1000.0
    return x if x > 0 else None


def _candle_rows(candles: Any) -> list[dict[str, float]]:
    # Some broker clients return a pandas DataFrame while others return
    # lists/dicts. Normalize DataFrames without requiring pandas at import time.
    if hasattr(candles, "to_dict") and hasattr(candles, "columns"):
        try:
            frame = candles.copy()
            lower = {str(c).casefold(): c for c in frame.columns}
            if not any(k in lower for k in ("time", "timestamp", "t")):
                frame = frame.reset_index()
                lower = {str(c).casefold(): c for c in frame.columns}
            rename = {}
            for canonical, aliases in {
                "time": ("time", "timestamp", "datetime", "date", "index", "t"),
                "open": ("open", "o"),
                "high": ("high", "h"),
                "low": ("low", "l"),
                "close": ("close", "c"),
            }.items():
                for alias in aliases:
                    if alias in lower:
                        rename[lower[alias]] = canonical
                        break
            frame = frame.rename(columns=rename)
            if "time" in frame.columns:
                try:
                    # Convert datetime-like values to epoch seconds.
                    vals = frame["time"]
                    if getattr(vals, "dtype", None) is not None and not str(vals.dtype).startswith(("int", "float")):
                        import pandas as pd
                        dt = pd.to_datetime(vals, utc=True, errors="coerce")
                        frame["time"] = dt.map(lambda x: x.timestamp() if not pd.isna(x) else None)
                except Exception:
                    pass
            candles = frame.to_dict("records")
        except Exception:
            return []
    if isinstance(candles, dict):
        for key in ("candles", "data", "history"):
            if isinstance(candles.get(key), list):
                candles = candles[key]
                break
    if isinstance(candles, tuple):
        candles = list(candles)
    if not isinstance(candles, list):
        return []
    out: list[dict[str, float]] = []
    for item in candles:
        if isinstance(item, dict):
            ts = _epoch(item.get("time", item.get("timestamp", item.get("t"))))
            o = item.get("open", item.get("o"))
            h = item.get("high", item.get("h"))
            l = item.get("low", item.get("l"))
            c = item.get("close", item.get("c"))
        elif isinstance(item, (list, tuple)) and len(item) >= 5:
            ts = _epoch(item[0])
            # Common broker array layout: time, open, close, high, low.
            o, c, h, l = item[1], item[2], item[3], item[4]
        else:
            continue
        try:
            row = {"time": float(ts), "open": float(o), "high": float(h), "low": float(l), "close": float(c)}
        except (TypeError, ValueError):
            continue
        if row["time"] and min(row["open"], row["high"], row["low"], row["close"]) > 0:
            out.append(row)
    # sort and dedupe by second
    by_ts = {int(r["time"]): r for r in out}
    return [by_ts[k] for k in sorted(by_ts)]


def _tick_rows(ticks: Any) -> list[tuple[float, float]]:
    if not isinstance(ticks, (list, tuple)):
        return []
    out: list[tuple[float, float]] = []
    for item in ticks:
        ts = price = None
        if isinstance(item, dict):
            ts = _epoch(item.get("time", item.get("timestamp", item.get("t"))))
            price = item.get("price", item.get("value", item.get("p")))
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            ts = _epoch(item[0])
            price = item[1]
        try:
            if ts is not None and float(price) > 0:
                out.append((float(ts), float(price)))
        except (TypeError, ValueError):
            pass
    return out


def rows_to_dataframe(candles: Any, ticks: Any = None):
    """Normalize broker candles + live ticks to RAJA's UTC 1-minute OHLC DataFrame."""
    try:
        import pandas as pd
    except Exception:
        return None

    rows = _candle_rows(candles)
    bars: dict[int, dict[str, float]] = {}
    for row in rows:
        minute = int(row["time"] // 60) * 60
        bars[minute] = {
            "Open": row["open"], "High": row["high"], "Low": row["low"], "Close": row["close"], "Volume": 0.0
        }
    for ts, price in _tick_rows(ticks):
        minute = int(ts // 60) * 60
        bar = bars.get(minute)
        if bar is None:
            bars[minute] = {"Open": price, "High": price, "Low": price, "Close": price, "Volume": 1.0}
        else:
            bar["High"] = max(float(bar["High"]), price)
            bar["Low"] = min(float(bar["Low"]), price)
            bar["Close"] = price
            bar["Volume"] = float(bar.get("Volume", 0.0)) + 1.0
    if not bars:
        return None
    idx = pd.to_datetime(list(sorted(bars)), unit="s", utc=True)
    frame = pd.DataFrame([bars[k] for k in sorted(bars)], index=idx)
    return frame[~frame.index.duplicated(keep="last")].sort_index()


def dataframe_age_seconds(df: Any) -> float | None:
    if df is None or getattr(df, "empty", True):
        return None
    try:
        last = df.index[-1]
        ts = float(last.timestamp())
        # Candle timestamp is minute open; allow current minute its full duration.
        return max(0.0, time.time() - (ts + 60.0))
    except Exception:
        return None


@dataclass
class FeedState:
    configured: bool = False
    library_available: bool | None = None
    connected: bool = False
    last_error: str = ""
    last_success_at: float = 0.0
    last_connect_at: float = 0.0
    last_pair: str = ""
    last_asset: str = ""
    subscriptions: set[str] = field(default_factory=set)

    def public(self) -> dict[str, Any]:
        now = time.time()
        return {
            "configured": bool(self.configured),
            "library_available": self.library_available,
            "connected": bool(self.connected),
            "last_error": self.last_error[-300:],
            "last_success_age_seconds": (round(now - self.last_success_at, 1) if self.last_success_at else None),
            "last_connect_age_seconds": (round(now - self.last_connect_at, 1) if self.last_connect_at else None),
            "last_pair": self.last_pair,
            "last_asset": self.last_asset,
            "subscriptions": len(self.subscriptions),
        }


class QuotexNativeFeed:
    def __init__(self) -> None:
        self.ssid = (os.environ.get("RAJA_QUOTEX_SSID") or os.environ.get("QUOTEX_SSID") or "").strip()
        self.cookies = (os.environ.get("RAJA_QUOTEX_COOKIES") or "").strip()
        self.user_agent = (os.environ.get("RAJA_QUOTEX_USER_AGENT") or "Mozilla/5.0 RAJA-AI-NativeFeed").strip()
        self.email = (os.environ.get("RAJA_QUOTEX_EMAIL") or "").strip()
        self.password = (os.environ.get("RAJA_QUOTEX_PASSWORD") or "").strip()
        self.demo = _env_bool("RAJA_QUOTEX_DEMO", True)
        self.enabled = _env_bool("RAJA_QUOTEX_NATIVE_ENABLED", bool(self.ssid or (self.email and self.password)))
        self.history_seconds = max(7200, min(86400, int(os.environ.get("RAJA_NATIVE_HISTORY_SECONDS", "21600"))))
        self.cache_seconds = max(2, min(30, int(os.environ.get("RAJA_NATIVE_CACHE_SECONDS", "8"))))
        self.history_refresh_seconds = max(30, min(900, int(os.environ.get("RAJA_NATIVE_HISTORY_REFRESH_SECONDS", "120"))))
        self.timeout = max(4, min(30, int(os.environ.get("RAJA_NATIVE_REQUEST_TIMEOUT", "14"))))
        self.state = FeedState(configured=bool(self.enabled and (self.ssid or (self.email and self.password))))
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._client: Any = None
        self._lock = threading.RLock()
        self._cache: dict[str, tuple[float, Any, str]] = {}
        self._frames: dict[str, tuple[float, Any, str]] = {}
        self._start_guard = threading.Lock()

    def _ensure_loop(self) -> bool:
        if not self.state.configured:
            return False
        with self._start_guard:
            if self._loop and self._thread and self._thread.is_alive():
                return True
            ready = threading.Event()
            def runner() -> None:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                self._loop = loop
                ready.set()
                loop.run_forever()
            self._thread = threading.Thread(target=runner, name="raja-quotex-native", daemon=True)
            self._thread.start()
            ready.wait(2.0)
        return bool(self._loop)

    async def _ensure_connected(self) -> bool:
        try:
            from pyquotex.stable_api import Quotex
            self.state.library_available = True
        except Exception as exc:
            self.state.library_available = False
            self.state.connected = False
            self.state.last_error = f"pyquotex import failed: {exc}"
            return False
        try:
            if self._client is not None:
                try:
                    if await self._client.check_connect():
                        self.state.connected = True
                        return True
                except Exception:
                    pass
                try:
                    await self._client.close()
                except Exception:
                    pass
                self._client = None

            email = self.email or "raja-native-session@local.invalid"
            client = Quotex(email=email, password=self.password or "", lang="en", user_agent=self.user_agent)
            if self.ssid:
                client.set_session(self.user_agent, cookies=self.cookies or None, ssid=self.ssid)
            try:
                client.account_is_demo = 1 if self.demo else 0
            except Exception:
                pass
            ok, reason = await client.connect()
            self.state.last_connect_at = time.time()
            if not ok:
                self.state.connected = False
                self.state.last_error = f"Quotex connect failed: {reason}"
                return False
            self._client = client
            # A replacement websocket has no live subscriptions even if the old
            # client did. Force the next fetch to subscribe again.
            self.state.subscriptions.clear()
            self.state.connected = True
            self.state.last_error = ""
            return True
        except Exception as exc:
            self.state.connected = False
            self.state.last_error = f"Quotex native error: {type(exc).__name__}: {exc}"
            return False

    async def _fetch(self, pair: str) -> tuple[Any, str | None]:
        if not await self._ensure_connected():
            return None, None
        asset = default_asset_candidates(pair)[0] if default_asset_candidates(pair) else None
        if not asset:
            self.state.last_error = f"No Quotex asset mapping for {pair}"
            return None, None
        client = self._client
        try:
            # Resolve against live instrument list when pyquotex can do so.
            try:
                resolved, asset_data = await client.get_available_asset(asset, force_open=False)
                if resolved:
                    asset = str(resolved)
            except Exception:
                pass
            if asset not in self.state.subscriptions:
                # Current pyquotex exposes start_realtime_price(); older variants
                # used candle-stream helpers. Support both so a library update does
                # not silently break the RAJA native feed.
                if hasattr(client, "start_realtime_price"):
                    await client.start_realtime_price(asset, 60)
                elif hasattr(client, "start_candles_one_stream"):
                    await client.start_candles_one_stream(asset, 60)
                elif hasattr(client, "start_candles_stream"):
                    try:
                        await client.start_candles_stream(asset, 60)
                    except TypeError:
                        await client.start_candles_stream(asset, 60, 1000)
                else:
                    raise AttributeError("Installed pyquotex has no supported realtime subscription method")
                self.state.subscriptions.add(asset)
                # Give the first realtime tick a short chance to arrive.
                await asyncio.sleep(0.25)

            # Keep a local broker-native frame and refresh history periodically.
            # This avoids hammering history/load for every scan while realtime
            # ticks continue to update the currently forming 1m candle.
            saved = self._frames.get(pair)
            need_history = (
                not saved
                or saved[2] != asset
                or (time.time() - float(saved[0])) >= self.history_refresh_seconds
            )
            if need_history:
                try:
                    candles = await client.get_candles(
                        asset, time.time(), self.history_seconds, 60,
                        timeout=self.timeout, use_cache=True,
                    )
                except TypeError:
                    try:
                        candles = await client.get_candles(
                            asset, time.time(), self.history_seconds, 60,
                            timeout=self.timeout,
                        )
                    except TypeError:
                        candles = await client.get_candles(asset, time.time(), self.history_seconds, 60)
                history_at = time.time()
            else:
                candles = saved[1]
                history_at = float(saved[0])

            ticks = await client.get_realtime_price(asset)
            df = rows_to_dataframe(candles, ticks)
            if df is not None and not df.empty:
                # Keep a bounded rolling frame for fast next scans.
                df = df.tail(2500)
                self._frames[pair] = (history_at, df.copy(), asset)
            if df is None or df.empty:
                self.state.last_error = f"Quotex returned no usable candles for {asset}"
                return None, asset
            self.state.last_success_at = time.time()
            self.state.last_pair = pair
            self.state.last_asset = asset
            self.state.connected = True
            self.state.last_error = ""
            return df, asset
        except Exception as exc:
            self.state.last_error = f"Quotex fetch {asset}: {type(exc).__name__}: {exc}"
            try:
                self.state.connected = bool(await client.check_connect())
            except Exception:
                self.state.connected = False
            return None, asset

    def get(self, pair: str):
        if not self._ensure_loop() or not self._loop:
            return None, None, self.status()
        with self._lock:
            cached = self._cache.get(pair)
            if cached and time.time() - cached[0] <= self.cache_seconds:
                df, asset = cached[1], cached[2]
                return df.copy(), asset, self.status()
        fut = asyncio.run_coroutine_threadsafe(self._fetch(pair), self._loop)
        try:
            df, asset = fut.result(timeout=self.timeout + 4)
        except Exception as exc:
            self.state.last_error = f"Quotex request timeout/error: {type(exc).__name__}: {exc}"
            return None, None, self.status()
        if df is not None and not df.empty:
            with self._lock:
                self._cache[pair] = (time.time(), df.copy(), asset or "")
            return df, asset, self.status()
        return None, asset, self.status()

    def status(self) -> dict[str, Any]:
        out = self.state.public()
        out.update({"enabled": bool(self.enabled), "mode": "demo" if self.demo else "real", "auth_mode": "session" if self.ssid else ("credentials" if self.email and self.password else "none")})
        return out


class PocketNativeFeed:
    def __init__(self) -> None:
        # Prefer the full Pocket Option WebSocket SSID.  RAJA_POCKET_SID is
        # also accepted as an alias/fallback for deployments that keep both
        # names as separate Railway variables.
        self.ssid = (
            os.environ.get("RAJA_POCKET_SSID")
            or os.environ.get("RAJA_POCKET_SID")
            or os.environ.get("PO_SSID")
            or ""
        ).strip()
        self.auth_source = (
            "RAJA_POCKET_SSID" if os.environ.get("RAJA_POCKET_SSID")
            else ("RAJA_POCKET_SID" if os.environ.get("RAJA_POCKET_SID")
                  else ("PO_SSID" if os.environ.get("PO_SSID") else "none"))
        )
        self.enabled = _env_bool("RAJA_POCKET_NATIVE_ENABLED", bool(self.ssid))
        self.history_offset = max(9000, min(120000, int(os.environ.get("RAJA_POCKET_HISTORY_OFFSET", "45000"))))
        self.cache_seconds = max(2, min(30, int(os.environ.get("RAJA_NATIVE_CACHE_SECONDS", "8"))))
        self.history_refresh_seconds = max(30, min(900, int(os.environ.get("RAJA_NATIVE_HISTORY_REFRESH_SECONDS", "120"))))
        self.state = FeedState(configured=bool(self.enabled and self.ssid))
        self._client: Any = None
        self._lock = threading.RLock()
        self._connect_lock = threading.Lock()
        self._cache: dict[str, tuple[float, Any, str]] = {}
        self._frames: dict[str, tuple[float, Any, str]] = {}
        self._catalog: dict[str, Any] = {}

    def _ensure_connected(self) -> bool:
        if not self.state.configured:
            return False
        try:
            from pocketoptionapi import PocketOption
            self.state.library_available = True
        except Exception as exc:
            self.state.library_available = False
            self.state.connected = False
            self.state.last_error = f"PocketOptionApi import failed: {exc}"
            return False
        with self._connect_lock:
            try:
                if self._client is not None and self._client.check_connect():
                    self.state.connected = True
                    return True
            except Exception:
                pass
            try:
                if self._client is not None:
                    try:
                        self._client.disconnect_websocket()
                    except Exception:
                        pass
                client = PocketOption(self.ssid)
                ok, err = client.connect()
                self.state.last_connect_at = time.time()
                if not ok:
                    self.state.connected = False
                    self.state.last_error = f"Pocket Option connect failed: {err}"
                    return False
                deadline = time.time() + 12.0
                while time.time() < deadline:
                    try:
                        if client.check_connect() and client.is_time_synced():
                            break
                    except Exception:
                        pass
                    time.sleep(0.15)
                if not client.check_connect():
                    self.state.connected = False
                    self.state.last_error = "Pocket Option websocket did not become ready"
                    return False
                self._client = client
                self.state.subscriptions.clear()
                try:
                    self._catalog = client.get_assets() or {}
                except Exception:
                    self._catalog = {}
                self.state.connected = True
                self.state.last_error = ""
                return True
            except Exception as exc:
                self.state.connected = False
                self.state.last_error = f"Pocket Option native error: {type(exc).__name__}: {exc}"
                return False

    def _fetch(self, pair: str):
        if not self._ensure_connected():
            return None, None
        client = self._client
        try:
            if not self._catalog:
                try:
                    self._catalog = client.get_assets() or {}
                except Exception:
                    pass
            asset = resolve_from_catalog(pair, self._catalog)
            if not asset:
                self.state.last_error = f"No Pocket Option asset mapping for {pair}"
                return None, None
            if asset not in self.state.subscriptions:
                client.subscribe(asset, period=60)
                self.state.subscriptions.add(asset)
                time.sleep(0.25)

            saved = self._frames.get(pair)
            need_history = (
                not saved
                or saved[2] != asset
                or (time.time() - float(saved[0])) >= self.history_refresh_seconds
            )
            if need_history:
                candles = client.get_historical_candles(
                    asset, period=60, offset=self.history_offset, count_request=1
                )
                history_at = time.time()
            else:
                candles = saved[1]
                history_at = float(saved[0])
            ticks = client.get_realtime_ticks(asset, limit=1000)
            df = rows_to_dataframe(candles, ticks)
            if df is not None and not df.empty:
                df = df.tail(2500)
                self._frames[pair] = (history_at, df.copy(), asset)
            if df is None or df.empty:
                self.state.last_error = f"Pocket Option returned no usable candles for {asset}"
                return None, asset
            self.state.last_success_at = time.time()
            self.state.last_pair = pair
            self.state.last_asset = asset
            self.state.connected = True
            self.state.last_error = ""
            return df, asset
        except Exception as exc:
            self.state.last_error = f"Pocket Option fetch: {type(exc).__name__}: {exc}"
            try:
                self.state.connected = bool(client.check_connect())
            except Exception:
                self.state.connected = False
            return None, None

    def get(self, pair: str):
        if not self.state.configured:
            return None, None, self.status()
        with self._lock:
            cached = self._cache.get(pair)
            if cached and time.time() - cached[0] <= self.cache_seconds:
                return cached[1].copy(), cached[2], self.status()
            df, asset = self._fetch(pair)
            if df is not None and not df.empty:
                self._cache[pair] = (time.time(), df.copy(), asset or "")
                return df, asset, self.status()
            return None, asset, self.status()

    def status(self) -> dict[str, Any]:
        out = self.state.public()
        out.update({
            "enabled": bool(self.enabled),
            "auth_mode": "ssid" if self.ssid else "none",
            "auth_source": self.auth_source,
        })
        return out


_QUOTEX = QuotexNativeFeed()
_POCKET = PocketNativeFeed()


def get_native_broker_market_data(broker: str, pair: str):
    """Return broker-native 1m OHLC for normal LIVE or OTC pairs.

    This is read-only market data. It never places orders.
    """
    broker_name = str(broker or "").strip().casefold().replace(" ", "")
    is_otc = "(otc)" in str(pair).casefold()

    if broker_name == "quotex":
        df, asset, status = _QUOTEX.get(pair)
        source = "Quotex Native WebSocket"
    elif broker_name in {"pocketoption", "pocket_option", "pocket"}:
        df, asset, status = _POCKET.get(pair)
        source = "Pocket Option Native WebSocket"
    else:
        return None, None, None, {
            "exact_broker_feed": False,
            "unavailable_reason": f"Unsupported broker for native feed: {broker or 'unknown'}",
        }

    age = dataframe_age_seconds(df)
    info = {
        "source": source,
        "source_mode": "broker_native_websocket_otc" if is_otc else "broker_native_websocket_live",
        "provider_symbol": asset,
        "broker_asset": asset,
        "exact_broker_feed": bool(df is not None and not getattr(df, "empty", True)),
        "native_feed": True,
        "broker_live": not is_otc,
        "is_otc": is_otc,
        "native_status": status,
        "backup_used": False,
    }
    if df is None or getattr(df, "empty", True):
        if not status.get("configured"):
            if broker_name == "quotex":
                info["unavailable_reason"] = (
                    "Quotex native feed is not configured. Set RAJA_QUOTEX_SSID "
                    "(preferred) or RAJA_QUOTEX_EMAIL/RAJA_QUOTEX_PASSWORD in Railway."
                )
            else:
                info["unavailable_reason"] = (
                    "Pocket Option native feed is not configured. Set RAJA_POCKET_SSID in Railway."
                )
        elif status.get("library_available") is False:
            info["unavailable_reason"] = (
                f"{source} dependency is not installed. Deploy the included requirements.txt."
            )
        else:
            info["unavailable_reason"] = status.get("last_error") or f"{source} has no fresh candle data yet."
    return df, age, asset, info

def native_feed_status() -> dict[str, Any]:
    return {"quotex": _QUOTEX.status(), "pocket_option": _POCKET.status()}
