"""RAJA AI Direct OTC reference-safety layer.

This module does NOT pretend Yahoo/Twelve Data candles are exact Quotex/Pocket Option
OTC candles.  It exists to let RAJA AI operate without a browser bridge while keeping
reference-feed signals conservative and clearly labelled.

The backend owns data fetching.  This module only validates/grades a reference OHLC
frame before a broker-OTC signal is allowed to surface.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from statistics import median
from typing import Any
import math

DIRECT_OTC_VERSION = "RAJA_DIRECT_OTC_V2_TYPE36"

# Highly illiquid/exotic FX proxies often have weak or irregular 1m reference data.
# They remain configurable in the UI, but Direct OTC Safe Mode will not issue a signal
# from them unless this list is explicitly relaxed in a future build.
BLOCKED_REFERENCE_FX = {
    "USD/BDT", "USD/DZD", "USD/NGN", "USD/PKR", "USD/EGP",
    "USD/IDR", "USD/ARS", "USD/COP", "USD/PHP", "USD/MYR",
}

# Liquid FX/crosses where public underlying 1m reference data is generally more usable.
PREFERRED_REFERENCE_FX = {
    "EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD", "USD/CHF", "NZD/USD",
    "EUR/GBP", "EUR/JPY", "GBP/JPY", "AUD/JPY", "CAD/JPY", "CHF/JPY",
    "EUR/CHF", "AUD/CAD", "AUD/CHF", "CAD/CHF", "NZD/JPY", "AUD/NZD",
    "EUR/NZD", "GBP/NZD", "GBP/AUD", "NZD/CAD", "NZD/CHF", "USD/SGD",
    "USD/CNH", "USD/MXN", "USD/BRL", "EUR/TRY",
}

# Continuation/breakout families get an extra higher-timeframe trend sanity check.
TREND_SENSITIVE_PATTERNS = {9, 10, 14, 20, 21, 22, 23, 25, 36}


def _clean_pair(pair: str) -> str:
    return str(pair or "").upper().replace(" (OTC)", "").strip()


def _is_fx(pair: str) -> bool:
    p = _clean_pair(pair)
    if "/" not in p:
        return False
    a, b = p.split("/", 1)
    return len(a) == 3 and len(b) == 3 and a.isalpha() and b.isalpha()


def _rows(df: Any, limit: int = 180) -> list[dict[str, float]]:
    if df is None or getattr(df, "empty", True):
        return []
    out: list[dict[str, float]] = []
    try:
        frame = df.tail(limit)
        for _idx, row in frame.iterrows():
            o = float(row["Open"]); h = float(row["High"]); l = float(row["Low"]); c = float(row["Close"])
            if not all(math.isfinite(x) and x > 0 for x in (o, h, l, c)):
                continue
            if h < max(o, c) or l > min(o, c) or h < l:
                continue
            out.append({"o": o, "h": h, "l": l, "c": c})
    except Exception:
        return []
    return out


def _pct(a: float, b: float) -> float:
    if not b:
        return 0.0
    return (a - b) / abs(b) * 100.0


@dataclass
class DirectOtcAssessment:
    allowed: bool
    score: float
    mode: str
    reason: str
    warnings: list[str]
    pair_tier: str
    median_range_pct: float
    last_range_multiple: float
    higher_tf_trend_pct: float
    sample_size: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def assess_direct_otc_reference(
    base_df: Any,
    pair: str,
    signal: str,
    pattern_type: int,
    timeframe: str,
    data_age_seconds: float | None,
) -> dict[str, Any]:
    """Conservative gate for non-exact broker OTC reference signals.

    It intentionally prefers fewer signals over pretending a public reference feed is
    identical to a broker's synthetic OTC market.
    """
    rows = _rows(base_df, 180)
    clean = _clean_pair(pair)
    signal = str(signal or "").upper()
    try:
        pattern_type = int(pattern_type or 0)
    except Exception:
        pattern_type = 0

    warnings: list[str] = [
        "Reference-only OTC: public underlying candles can differ from Quotex/Pocket Option OTC."
    ]

    if len(rows) < 60:
        return DirectOtcAssessment(False, 0.0, DIRECT_OTC_VERSION,
            f"Direct OTC needs at least 60 clean 1m reference candles; only {len(rows)} are available.",
            warnings, "INSUFFICIENT", 0.0, 0.0, 0.0, len(rows)).to_dict()

    if data_age_seconds is not None:
        try:
            age = float(data_age_seconds)
        except Exception:
            age = 999999.0
        if age > 95:
            return DirectOtcAssessment(False, 0.0, DIRECT_OTC_VERSION,
                f"Reference feed is {age:.0f}s old; Direct OTC requires a fresh source.",
                warnings, "STALE", 0.0, 0.0, 0.0, len(rows)).to_dict()

    pair_tier = "CRYPTO/OTHER"
    score = 82.0
    if _is_fx(pair):
        if clean in BLOCKED_REFERENCE_FX:
            return DirectOtcAssessment(False, 0.0, DIRECT_OTC_VERSION,
                f"{clean} is blocked in Direct OTC Safe Mode because its public 1m proxy is too irregular for conservative binary timing.",
                warnings, "BLOCKED_FX", 0.0, 0.0, 0.0, len(rows)).to_dict()
        if clean in PREFERRED_REFERENCE_FX:
            pair_tier = "PREFERRED_FX"
            score += 4.0
        else:
            pair_tier = "OTHER_FX"
            score -= 8.0
            warnings.append("This FX pair is not in the preferred Direct OTC liquidity set.")

    ranges_pct = []
    for r in rows[-80:]:
        mid = max(1e-12, (r["o"] + r["c"]) / 2.0)
        ranges_pct.append((r["h"] - r["l"]) / mid * 100.0)
    med_range_pct = median(ranges_pct) if ranges_pct else 0.0
    last = rows[-1]
    last_mid = max(1e-12, (last["o"] + last["c"]) / 2.0)
    last_range_pct = (last["h"] - last["l"]) / last_mid * 100.0
    last_multiple = last_range_pct / max(med_range_pct, 1e-9)

    # Dead reference candles and extreme spikes are poor inputs for 1m binary timing.
    if med_range_pct <= 0.0015:
        return DirectOtcAssessment(False, 0.0, DIRECT_OTC_VERSION,
            "Reference candles are nearly flat; Direct OTC will wait for normal movement.",
            warnings, pair_tier, med_range_pct, last_multiple, 0.0, len(rows)).to_dict()
    if last_multiple >= 4.5:
        return DirectOtcAssessment(False, 0.0, DIRECT_OTC_VERSION,
            f"Latest reference candle is an abnormal {last_multiple:.1f}x range spike; wait for a fresh candle.",
            warnings, pair_tier, med_range_pct, last_multiple, 0.0, len(rows)).to_dict()
    if last_multiple >= 3.0:
        score -= 12.0
        warnings.append("Latest candle is unusually large versus recent range.")

    # Gap/jump sanity: public feeds occasionally update discontinuously.
    recent = rows[-15:]
    jumps = [abs(_pct(recent[i]["o"], recent[i-1]["c"])) for i in range(1, len(recent))]
    max_jump = max(jumps) if jumps else 0.0
    typical = max(med_range_pct, 0.001)
    if max_jump > max(typical * 5.5, 0.35):
        return DirectOtcAssessment(False, 0.0, DIRECT_OTC_VERSION,
            f"Reference feed has an abnormal price gap ({max_jump:.3f}%); Direct OTC will not trade this snapshot.",
            warnings, pair_tier, med_range_pct, last_multiple, 0.0, len(rows)).to_dict()

    # Higher-timeframe sanity from the underlying 1m reference series.  We do not
    # demand full MTF agreement for reversal patterns; only continuation/breakout
    # patterns are blocked when the broader move is strongly opposite.
    closes = [r["c"] for r in rows]
    look = 25  # ~5 five-minute candles using 1m closes
    ht_trend = _pct(closes[-1], closes[-look]) if len(closes) >= look else 0.0
    if pattern_type in TREND_SENSITIVE_PATTERNS:
        if signal == "CALL" and ht_trend < -0.18:
            return DirectOtcAssessment(False, 0.0, DIRECT_OTC_VERSION,
                f"Direct OTC trend guard blocked CALL: underlying 25m move is {ht_trend:.3f}% bearish.",
                warnings, pair_tier, med_range_pct, last_multiple, ht_trend, len(rows)).to_dict()
        if signal == "PUT" and ht_trend > 0.18:
            return DirectOtcAssessment(False, 0.0, DIRECT_OTC_VERSION,
                f"Direct OTC trend guard blocked PUT: underlying 25m move is {ht_trend:.3f}% bullish.",
                warnings, pair_tier, med_range_pct, last_multiple, ht_trend, len(rows)).to_dict()
        if (signal == "CALL" and ht_trend > 0.04) or (signal == "PUT" and ht_trend < -0.04):
            score += 5.0

    # Short-term whipsaw guard: alternating large closes means the public reference
    # series is noisy for next-candle prediction.
    last6 = rows[-6:]
    dirs = []
    for r in last6:
        dirs.append(1 if r["c"] > r["o"] else (-1 if r["c"] < r["o"] else 0))
    flips = sum(1 for i in range(1, len(dirs)) if dirs[i] and dirs[i-1] and dirs[i] != dirs[i-1])
    if flips >= 4 and med_range_pct > 0.015:
        score -= 10.0
        warnings.append("Reference market is whipsawing; quality reduced.")

    # 1m reference mode needs a higher bar than exact broker candles.
    min_score = 76.0 if str(timeframe).lower() in {"1m", "2m"} else 72.0
    score = max(0.0, min(100.0, score))
    allowed = score >= min_score
    reason = (
        f"Direct OTC reference quality {score:.1f}/100 passed ({pair_tier})."
        if allowed else
        f"Direct OTC reference quality {score:.1f}/100 is below the {min_score:.1f}/100 safety minimum."
    )
    return DirectOtcAssessment(allowed, round(score, 2), DIRECT_OTC_VERSION, reason,
        warnings, pair_tier, round(med_range_pct, 6), round(last_multiple, 3),
        round(ht_trend, 5), len(rows)).to_dict()
