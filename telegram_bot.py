from flask import request, jsonify
import os
import json
import time
import threading
import hashlib
import html
from pathlib import Path
from urllib.request import Request as UrlRequest, urlopen
from urllib.error import HTTPError, URLError
from concurrent.futures import ThreadPoolExecutor

try:
    import psycopg
except Exception:
    psycopg = None

# =========================================================
# RAJA AI TELEGRAM GATEWAY
# - Token is read ONLY from TELEGRAM_BOT_TOKEN.
# - Admin can be claimed once with TELEGRAM_ADMIN_SETUP_CODE.
# - Webhook is protected by Telegram's secret-token header.
# - Telegram users reuse the same market-analysis callbacks as the web app.
# =========================================================

BOT_TOKEN = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
BOT_USERNAME = (os.environ.get("TELEGRAM_BOT_USERNAME") or "Raja_Aii_bot").strip().lstrip("@")
SUPPORT_USERNAME = (os.environ.get("TELEGRAM_SUPPORT_USERNAME") or "RAJASIGNALAIPREMIUM").strip().lstrip("@")
PARTNER_URL = (os.environ.get("RAJA_QUOTEX_PARTNER_URL") or "https://broker-qx.pro/sign-up/?lid=2209395").strip()
PUBLIC_BASE_URL = (os.environ.get("RAJA_PUBLIC_BASE_URL") or "https://raja-ai-bot.up.railway.app").strip().rstrip("/")
ADMIN_SETUP_CODE = (os.environ.get("TELEGRAM_ADMIN_SETUP_CODE") or "").strip()
ADMIN_ID_ENV = (os.environ.get("TELEGRAM_ADMIN_ID") or "").strip()
DATABASE_URL = (os.environ.get("DATABASE_URL") or os.environ.get("RAJA_DATABASE_URL") or "").strip()
DATA_DIR = Path(os.environ.get("RAJA_DATA_DIR", str(Path(__file__).resolve().parent))).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
USERS_FILE = DATA_DIR / "telegram_users.json"
META_FILE = DATA_DIR / "telegram_meta.json"
STORE_LOCK = threading.RLock()
TELEGRAM_UPDATE_POOL = ThreadPoolExecutor(max_workers=4, thread_name_prefix='raja-tg-update')

WEBHOOK_SECRET = (os.environ.get("TELEGRAM_WEBHOOK_SECRET") or "").strip()
if not WEBHOOK_SECRET and BOT_TOKEN:
    WEBHOOK_SECRET = hashlib.sha256(("raja-telegram:" + BOT_TOKEN).encode("utf-8")).hexdigest()[:48]

TELEGRAM_API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""
TELEGRAM_WEBHOOK_PATH = "/telegram/webhook"
TELEGRAM_WEBHOOK_REPAIR_INTERVAL = max(60, int(os.environ.get("RAJA_TELEGRAM_WEBHOOK_REPAIR_INTERVAL", "300")))
TELEGRAM_AUTO_WEBHOOK = str(os.environ.get("RAJA_TELEGRAM_AUTO_WEBHOOK", "1")).strip().lower() not in {"0", "false", "no", "off"}
_TELEGRAM_RUNTIME = {
    "bot_ok": False, "bot_username": BOT_USERNAME, "webhook_ok": False,
    "webhook_url": "", "last_error": "", "last_check": 0, "last_configured": 0,
}
_TELEGRAM_RUNTIME_LOCK = threading.RLock()
_TELEGRAM_MONITOR_STARTED = False
_TELEGRAM_MONITOR_LOCK = threading.Lock()

MARKET_PAIRS = {
    "CryptoLive": ["BTC-USD", "ETH-USD", "SOL-USD", "LTC-USD", "XRP-USD", "ADA-USD", "DOGE-USD"],
    "CryptoOTC": [
        "Zcash (OTC)", "Chainlink (OTC)", "Bitcoin (OTC)", "Binance Coin (OTC)", "Ethereum (OTC)",
        "Bitcoin Cash (OTC)", "Cosmos (OTC)", "Ethereum Classic (OTC)", "Axie Infinity (OTC)",
        "Trump (OTC)", "Dash (OTC)", "Solana (OTC)", "Toncoin (OTC)", "Litecoin (OTC)",
        "Avalanche (OTC)", "Polkadot (OTC)", "Ripple (OTC)"
    ],
    "ForexLive": [
        "EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD", "USD/CHF", "NZD/USD", "EUR/GBP", "EUR/JPY",
        "GBP/JPY", "AUD/JPY", "EUR/AUD", "GBP/AUD", "CAD/JPY", "EUR/CAD", "GBP/CAD", "NZD/JPY", "AUD/NZD",
        "EUR/CHF", "GBP/CHF", "XAUUSD"
    ],
    "ForexOTC": [
        "USD/BRL (OTC)", "NZD/CHF (OTC)", "NZD/JPY (OTC)", "USD/COP (OTC)", "USD/MXN (OTC)", "AUD/NZD (OTC)",
        "USD/BDT (OTC)", "USD/DZD (OTC)", "USD/NGN (OTC)", "USD/PHP (OTC)", "USD/PKR (OTC)", "USD/ZAR (OTC)",
        "USD/INR (OTC)", "USD/EGP (OTC)", "USD/IDR (OTC)", "USD/ARS (OTC)", "GBP/NZD (OTC)", "EUR/NZD (OTC)",
        "NZD/USD (OTC)", "NZD/CAD (OTC)", "CAD/CHF (OTC)"
    ],
}

MARKET_LABELS = {
    "CryptoLive": "🪙 Crypto Live",
    "CryptoOTC": "🪙 Crypto OTC Proxy",
    "ForexLive": "💱 Forex Live",
    "ForexOTC": "⚡ Forex OTC Proxy",
}

VALID_EXPIRIES = ["1m", "2m", "5m", "15m", "30m"]


def _db_connect():
    if not DATABASE_URL or psycopg is None:
        return None
    return psycopg.connect(DATABASE_URL, connect_timeout=10)


def _read_json(path, fallback):
    try:
        if not path.exists():
            return fallback
        data = json.loads(path.read_text(encoding="utf-8"))
        return data
    except Exception:
        return fallback


def _write_json(path, data):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def init_telegram_store():
    """Create Telegram-specific persistent tables without changing the web-license schema."""
    if DATABASE_URL and psycopg is not None:
        try:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS raja_telegram_users (
                            telegram_id BIGINT PRIMARY KEY,
                            chat_id BIGINT NOT NULL,
                            username TEXT,
                            first_name TEXT,
                            last_name TEXT,
                            submitted_id TEXT,
                            license_key TEXT,
                            status TEXT NOT NULL DEFAULT 'NEW',
                            stage TEXT NOT NULL DEFAULT 'START',
                            market TEXT,
                            pair TEXT,
                            expiry TEXT,
                            created_at BIGINT,
                            updated_at BIGINT,
                            approved_at BIGINT
                        )
                    """)
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS raja_telegram_meta (
                            meta_key TEXT PRIMARY KEY,
                            meta_value TEXT
                        )
                    """)
            return
        except Exception as exc:
            print(f"Telegram DB initialization warning: {exc}")
    with STORE_LOCK:
        if not USERS_FILE.exists():
            _write_json(USERS_FILE, {})
        if not META_FILE.exists():
            _write_json(META_FILE, {})


def get_meta(key, default=None):
    if DATABASE_URL and psycopg is not None:
        try:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT meta_value FROM raja_telegram_meta WHERE meta_key=%s", (key,))
                    row = cur.fetchone()
                    return row[0] if row else default
        except Exception as exc:
            print(f"Telegram meta read warning: {exc}")
    with STORE_LOCK:
        return _read_json(META_FILE, {}).get(key, default)


def set_meta(key, value):
    value = str(value)
    if DATABASE_URL and psycopg is not None:
        try:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO raja_telegram_meta(meta_key, meta_value)
                        VALUES(%s,%s)
                        ON CONFLICT(meta_key) DO UPDATE SET meta_value=EXCLUDED.meta_value
                    """, (key, value))
            return True
        except Exception as exc:
            print(f"Telegram meta write warning: {exc}")
    with STORE_LOCK:
        data = _read_json(META_FILE, {})
        data[key] = value
        _write_json(META_FILE, data)
    return True


def _default_user(telegram_id, chat_id=None):
    now = int(time.time())
    return {
        "telegram_id": int(telegram_id),
        "chat_id": int(chat_id or telegram_id),
        "username": "",
        "first_name": "",
        "last_name": "",
        "submitted_id": "",
        "license_key": "",
        "status": "NEW",
        "stage": "START",
        "market": "",
        "pair": "",
        "expiry": "1m",
        "created_at": now,
        "updated_at": now,
        "approved_at": None,
    }


def get_user(telegram_id, chat_id=None):
    telegram_id = int(telegram_id)
    if DATABASE_URL and psycopg is not None:
        try:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT telegram_id,chat_id,username,first_name,last_name,submitted_id,license_key,
                               status,stage,market,pair,expiry,created_at,updated_at,approved_at
                        FROM raja_telegram_users WHERE telegram_id=%s
                    """, (telegram_id,))
                    row = cur.fetchone()
            if row:
                keys = ["telegram_id","chat_id","username","first_name","last_name","submitted_id","license_key",
                        "status","stage","market","pair","expiry","created_at","updated_at","approved_at"]
                return dict(zip(keys, row))
        except Exception as exc:
            print(f"Telegram user read warning: {exc}")
    else:
        with STORE_LOCK:
            data = _read_json(USERS_FILE, {})
            item = data.get(str(telegram_id))
            if isinstance(item, dict):
                return item
    return _default_user(telegram_id, chat_id)


def save_user(user):
    user = dict(user)
    user["telegram_id"] = int(user["telegram_id"])
    user["chat_id"] = int(user.get("chat_id") or user["telegram_id"])
    user["updated_at"] = int(time.time())
    user.setdefault("created_at", user["updated_at"])
    if DATABASE_URL and psycopg is not None:
        try:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO raja_telegram_users(
                            telegram_id,chat_id,username,first_name,last_name,submitted_id,license_key,
                            status,stage,market,pair,expiry,created_at,updated_at,approved_at
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT(telegram_id) DO UPDATE SET
                            chat_id=EXCLUDED.chat_id, username=EXCLUDED.username, first_name=EXCLUDED.first_name,
                            last_name=EXCLUDED.last_name, submitted_id=EXCLUDED.submitted_id,
                            license_key=EXCLUDED.license_key, status=EXCLUDED.status, stage=EXCLUDED.stage,
                            market=EXCLUDED.market, pair=EXCLUDED.pair, expiry=EXCLUDED.expiry,
                            updated_at=EXCLUDED.updated_at, approved_at=EXCLUDED.approved_at
                    """, (
                        user["telegram_id"], user["chat_id"], user.get("username"), user.get("first_name"),
                        user.get("last_name"), user.get("submitted_id"), user.get("license_key"), user.get("status", "NEW"),
                        user.get("stage", "START"), user.get("market"), user.get("pair"), user.get("expiry", "1m"),
                        user.get("created_at"), user.get("updated_at"), user.get("approved_at")
                    ))
            return user
        except Exception as exc:
            print(f"Telegram user write warning: {exc}")
    with STORE_LOCK:
        data = _read_json(USERS_FILE, {})
        data[str(user["telegram_id"])] = user
        _write_json(USERS_FILE, data)
    return user


def pending_users(limit=20):
    if DATABASE_URL and psycopg is not None:
        try:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT telegram_id,chat_id,username,first_name,last_name,submitted_id,license_key,
                               status,stage,market,pair,expiry,created_at,updated_at,approved_at
                        FROM raja_telegram_users
                        WHERE status='PENDING'
                        ORDER BY updated_at ASC LIMIT %s
                    """, (int(limit),))
                    rows = cur.fetchall()
            keys = ["telegram_id","chat_id","username","first_name","last_name","submitted_id","license_key",
                    "status","stage","market","pair","expiry","created_at","updated_at","approved_at"]
            return [dict(zip(keys, row)) for row in rows]
        except Exception as exc:
            print(f"Telegram pending read warning: {exc}")
    with STORE_LOCK:
        data = _read_json(USERS_FILE, {})
        items = [x for x in data.values() if isinstance(x, dict) and x.get("status") == "PENDING"]
        items.sort(key=lambda x: x.get("updated_at", 0))
        return items[:limit]


def approved_users(limit=100):
    """Return active Telegram users for the admin-only license list."""
    if DATABASE_URL and psycopg is not None:
        try:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT telegram_id,chat_id,username,first_name,last_name,submitted_id,license_key,
                               status,stage,market,pair,expiry,created_at,updated_at,approved_at
                        FROM raja_telegram_users
                        WHERE status='ACTIVE'
                        ORDER BY approved_at DESC NULLS LAST, updated_at DESC
                        LIMIT %s
                    """, (int(limit),))
                    rows = cur.fetchall()
            keys = ["telegram_id","chat_id","username","first_name","last_name","submitted_id","license_key",
                    "status","stage","market","pair","expiry","created_at","updated_at","approved_at"]
            return [dict(zip(keys, row)) for row in rows]
        except Exception as exc:
            print(f"Telegram approved read warning: {exc}")
    with STORE_LOCK:
        data = _read_json(USERS_FILE, {})
        items = [x for x in data.values() if isinstance(x, dict) and str(x.get("status") or "").upper() == "ACTIVE"]
        items.sort(key=lambda x: (x.get("approved_at") or 0, x.get("updated_at") or 0), reverse=True)
        return items[:limit]


def admin_id():
    if ADMIN_ID_ENV.isdigit():
        return int(ADMIN_ID_ENV)
    saved = str(get_meta("admin_telegram_id", "") or "").strip()
    return int(saved) if saved.isdigit() else None


def tg_api(method, payload=None, timeout=20):
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured")
    payload = payload or {}
    data = json.dumps(payload).encode("utf-8")
    req = UrlRequest(
        f"{TELEGRAM_API_BASE}/{method}",
        data=data,
        headers={"Content-Type": "application/json", "User-Agent": "RAJA-AI-Telegram/1.0"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
        result = json.loads(body)
        if not result.get("ok"):
            raise RuntimeError(result.get("description") or f"Telegram {method} failed")
        return result.get("result")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
        raise RuntimeError(f"Telegram {method} HTTP {exc.code}: {body[:300]}") from exc
    except URLError as exc:
        raise RuntimeError(f"Telegram {method} network error: {exc}") from exc


def _set_runtime(**updates):
    with _TELEGRAM_RUNTIME_LOCK:
        _TELEGRAM_RUNTIME.update(updates)
        _TELEGRAM_RUNTIME["last_check"] = int(time.time())


def get_webhook_info():
    if not BOT_TOKEN:
        return {}
    info = tg_api("getWebhookInfo", {}, timeout=10) or {}
    return info if isinstance(info, dict) else {}


def telegram_self_test():
    if not BOT_TOKEN:
        _set_runtime(bot_ok=False, webhook_ok=False, last_error="TELEGRAM_BOT_TOKEN is not configured")
        return False
    try:
        me = tg_api("getMe", {}, timeout=10) or {}
        actual_username = str(me.get("username") or BOT_USERNAME).lstrip("@")
        info = get_webhook_info()
        expected_url = f"{PUBLIC_BASE_URL}{TELEGRAM_WEBHOOK_PATH}" if PUBLIC_BASE_URL else ""
        actual_url = str(info.get("url") or "")
        webhook_ok = bool(expected_url and actual_url == expected_url and not info.get("last_error_message"))
        _set_runtime(bot_ok=True, bot_username=actual_username, webhook_ok=webhook_ok,
                     webhook_url=actual_url, last_error=str(info.get("last_error_message") or ""))
        return webhook_ok
    except Exception as exc:
        _set_runtime(bot_ok=False, webhook_ok=False, last_error=str(exc)[:300])
        return False


def configure_bot_commands():
    commands = [
        {"command": "start", "description": "Open RAJA AI menu"},
        {"command": "menu", "description": "Show main menu"},
        {"command": "status", "description": "Show bot/access status"},
    ]
    try:
        tg_api("setMyCommands", {"commands": commands}, timeout=10)
        return True
    except Exception as exc:
        print(f"Telegram setMyCommands warning: {exc}")
        return False


def send_message(chat_id, text, keyboard=None, parse_mode="HTML"):
    payload = {
        "chat_id": int(chat_id),
        "text": text,
        "disable_web_page_preview": True,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if keyboard:
        payload["reply_markup"] = keyboard
    return tg_api("sendMessage", payload)


def edit_message(chat_id, message_id, text, keyboard=None):
    payload = {"chat_id": int(chat_id), "message_id": int(message_id), "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if keyboard:
        payload["reply_markup"] = keyboard
    return tg_api("editMessageText", payload)


def answer_callback(callback_id, text=None, alert=False):
    payload = {"callback_query_id": callback_id, "show_alert": bool(alert)}
    if text:
        payload["text"] = text[:180]
    try:
        return tg_api("answerCallbackQuery", payload)
    except Exception as exc:
        print(f"Telegram answerCallbackQuery warning: {exc}")
        return None


def btn(text, callback_data=None, url=None):
    item = {"text": text}
    if url:
        item["url"] = url
    else:
        item["callback_data"] = callback_data
    return item


def markup(rows):
    return {"inline_keyboard": rows}


def contact_url():
    return f"https://t.me/{SUPPORT_USERNAME}"


def start_keyboard(active=False):
    if active:
        return markup([
            [btn("📊 AI MARKET SCAN", "menu:scan")],
            [btn("🔑 MY ACCESS / LICENSE", "menu:status"), btn("💬 CONTACT ADMIN", url=contact_url())],
        ])
    return markup([
        [btn("🔗 CREATE QUOTEX ACCOUNT", url=PARTNER_URL)],
        [btn("✅ SUBMIT UID / REQUEST ACCESS", "access:uid")],
        [btn("🔑 I ALREADY HAVE VIP KEY", "access:key")],
        [btn("💬 CONTACT ADMIN", url=contact_url())],
    ])


def welcome_text(user):
    status = str(user.get("status") or "NEW").upper()
    if status == "ACTIVE":
        return (
            "👑 <b>RAJA AI PREMIUM</b>\n\n"
            "✅ Your Telegram access is <b>ACTIVE</b>.\n"
            "Use the menu below to scan the same RAJA AI market engine used by the web dashboard."
        )
    status_line = ""
    if status == "PENDING":
        status_line = "\n\n⏳ <b>Status:</b> Pending admin verification."
    elif status == "REJECTED":
        status_line = "\n\n❌ <b>Status:</b> Verification was not approved. Contact admin if you need help."
    elif status == "EXPIRED":
        status_line = "\n\n⌛ <b>Status:</b> Your trial/VIP license has expired. Contact admin to renew access."
    return (
        "👑 <b>RAJA AI PREMIUM</b>\n"
        "Multi-Broker AI Service\n\n"
        "<b>Step 1:</b> Create your Quotex account using our official partner link.\n"
        f"🔗 <a href=\"{html.escape(PARTNER_URL, quote=True)}\">Quotex Partner Link</a>\n"
        "<b>Step 2:</b> Deposit minimum $50.\n"
        "<b>Step 3:</b> Submit your Telegram ID or Quotex UID for verification.\n\n"
        "🔐 After verification, admin can approve your access and issue/confirm your VIP license.\n"
        "📱 One approved Telegram account per access record.\n\n"
        f"💬 <b>Official Support:</b> @{html.escape(SUPPORT_USERNAME)}"
        + status_line
    )


def sync_identity(update_user, chat_id):
    tg_id = int(update_user.get("id"))
    user = get_user(tg_id, chat_id)
    user["chat_id"] = int(chat_id)
    user["username"] = str(update_user.get("username") or "")[:120]
    user["first_name"] = str(update_user.get("first_name") or "")[:120]
    user["last_name"] = str(update_user.get("last_name") or "")[:120]
    return save_user(user)


def notify_admin_pending(user):
    aid = admin_id()
    if not aid:
        return False
    uname = f"@{user.get('username')}" if user.get("username") else "(no username)"
    text = (
        "🛡️ <b>NEW TELEGRAM ACCESS REQUEST</b>\n\n"
        f"Name: <b>{html.escape((user.get('first_name') or '') + ' ' + (user.get('last_name') or ''))}</b>\n"
        f"Username: {html.escape(uname)}\n"
        f"Telegram ID: <code>{user.get('telegram_id')}</code>\n"
        f"Submitted UID/ID: <code>{html.escape(user.get('submitted_id') or 'Not supplied')}</code>\n"
        f"VIP Key: <code>{html.escape(user.get('license_key') or 'Not supplied / issue on approval')}</code>\n\n"
        "Verify the user's Quotex/referral details, then approve or reject."
    )
    kb = markup([[btn("✅ APPROVE", f"admin:approve:{user['telegram_id']}"), btn("❌ REJECT", f"admin:reject:{user['telegram_id']}")]])
    send_message(aid, text, kb)
    return True


def show_pending_to_admin(chat_id):
    items = pending_users(20)
    if not items:
        send_message(chat_id, "✅ No pending Telegram access requests.")
        return
    send_message(chat_id, f"🛡️ <b>Pending requests:</b> {len(items)}")
    for user in items:
        notify_text = (
            f"👤 <b>{html.escape(user.get('first_name') or 'User')}</b> "
            f"@{html.escape(user.get('username') or 'no_username')}\n"
            f"Telegram ID: <code>{user['telegram_id']}</code>\n"
            f"Submitted: <code>{html.escape(user.get('submitted_id') or '--')}</code>\n"
            f"Key: <code>{html.escape(user.get('license_key') or 'Issue on approval')}</code>"
        )
        kb = markup([[btn("✅ APPROVE", f"admin:approve:{user['telegram_id']}"), btn("❌ REJECT", f"admin:reject:{user['telegram_id']}")]])
        send_message(chat_id, notify_text, kb)


def show_approved_to_admin(chat_id):
    items = approved_users(100)
    if not items:
        send_message(chat_id, "🔐 No approved Telegram licenses found.")
        return

    send_message(chat_id, f"🔐 <b>APPROVED TELEGRAM LICENSES</b> · {len(items)}")
    for i, user in enumerate(items, 1):
        username = f"@{user.get('username')}" if user.get("username") else "(no username)"
        name = ((user.get('first_name') or '') + ' ' + (user.get('last_name') or '')).strip() or 'User'
        text = (
            f"<b>{i}. {html.escape(name)}</b> · {html.escape(username)}\n"
            f"Telegram ID: <code>{user.get('telegram_id')}</code>\n"
            f"UID/ID: <code>{html.escape(user.get('submitted_id') or '--')}</code>\n"
            f"VIP Key: <code>{html.escape(user.get('license_key') or '--')}</code>\n"
            f"Status: <b>{html.escape(str(user.get('status') or '--'))}</b>"
        )
        kb = markup([
            [btn("❌ REMOVE ACCESS", f"admin:revoke:{user['telegram_id']}")]
        ])
        send_message(chat_id, text, kb)


def market_keyboard():
    return markup([
        [btn(MARKET_LABELS["CryptoLive"], "mkt:CryptoLive"), btn(MARKET_LABELS["CryptoOTC"], "mkt:CryptoOTC")],
        [btn(MARKET_LABELS["ForexLive"], "mkt:ForexLive"), btn(MARKET_LABELS["ForexOTC"], "mkt:ForexOTC")],
        [btn("⬅️ MAIN MENU", "menu:home")],
    ])


def pair_keyboard(market, page=0):
    pairs = MARKET_PAIRS.get(market, [])
    per_page = 8
    pages = max(1, (len(pairs) + per_page - 1) // per_page)
    page = max(0, min(int(page), pages - 1))
    start = page * per_page
    rows = [[btn("✨ AUTO SCAN BEST PAIR", f"pairauto:{market}")]]
    chunk = pairs[start:start+per_page]
    for offset in range(0, len(chunk), 2):
        row = []
        for j in range(offset, min(offset+2, len(chunk))):
            index = start + j
            row.append(btn(chunk[j], f"pair:{market}:{index}"))
        rows.append(row)
    nav = []
    if page > 0:
        nav.append(btn("◀️", f"pairs:{market}:{page-1}"))
    nav.append(btn(f"{page+1}/{pages}", f"pairs:{market}:{page}"))
    if page + 1 < pages:
        nav.append(btn("▶️", f"pairs:{market}:{page+1}"))
    rows.append(nav)
    rows.append([btn("⬅️ MARKETS", "menu:scan")])
    return markup(rows)


def expiry_keyboard():
    return markup([
        [btn("1m", "exp:1m"), btn("2m", "exp:2m"), btn("5m", "exp:5m"), btn("15m", "exp:15m"), btn("30m", "exp:30m")],
        [btn("⬅️ CHANGE PAIR", "pair:back")],
    ])


def ready_to_scan_keyboard():
    return markup([
        [btn("🚀 START AI MARKET SCAN", "scan:run")],
        [btn("⬅️ CHANGE EXPIRY", "scan:expiry"), btn("🏠 MAIN MENU", "menu:home")],
    ])


def format_signal(result, expiry):
    signal = str(result.get("signal") or "")
    pair = result.get("pair") or "--"
    pattern = result.get("selected_pattern") or "SK Pattern"
    ptype = int(result.get("pattern_type") or 0)
    next_color = result.get("next_candle_color") or ("GREEN" if signal == "CALL" else "RED")
    direction = "🟢 ⬆️ UP (BUY)" if signal == "CALL" else "🔴 ⬇️ DOWN (SELL)"
    rules = result.get("rules") or []
    matched = int(result.get("rules_matched") or sum(1 for r in rules if isinstance(r, dict) and r.get("ok")))
    total = int(result.get("rules_total") or len(rules))
    recovery = "\n♻️ <b>RECOVERY TRADE</b>" if result.get("recovery_trade") else ""
    return (
        "🎯 <b>RAJA AI · SK25 EXACT SIGNAL</b>\n\n"
        f"Asset: <b>{html.escape(str(pair))}</b>\n"
        f"Signal: <b>{direction}</b>\n"
        f"Pattern: <b>{html.escape(str(pattern))}</b>" + (f" (Type {ptype})" if ptype else "") + "\n"
        f"Setup Match: <b>100%</b> · Rules {matched}/{total}\n"
        f"Selected Timeframe: <b>{html.escape(str(expiry))}</b>\n"
        f"Next Candle: <b>{html.escape(str(next_color))}</b>{recovery}\n\n"
        "⏳ <b>Entry:</b> Next candle open after the confirmed closed-candle setup.\n"
        "<i>Pattern Type 1-25 only · No technical indicators</i>"
    )



def _run_scan(user, services):
    chat_id = user["chat_id"]
    expiry = user.get("expiry") or "1m"
    pair = user.get("pair") or ""
    market = user.get("market") or ""
    try:
        if pair == "__AUTO__":
            pairs = MARKET_PAIRS.get(market, [])
            outcome = services["scan_auto"](pairs, expiry)
            result = (outcome or {}).get("best") if isinstance(outcome, dict) else None
            diagnostics = (outcome or {}).get("diagnostics", {}) if isinstance(outcome, dict) else {}
        else:
            result = services["scan_pair"](pair, expiry)
            diagnostics = {}
        if result and result.get("signal") in {"CALL", "PUT"}:
            send_message(chat_id, format_signal(result, expiry), markup([
                [btn("🔁 SCAN AGAIN", "scan:run")],
                [btn("🔄 CHANGE PAIR", "menu:scan"), btn("🏠 MAIN MENU", "menu:home")],
            ]))
        else:
            extra = ""
            if diagnostics:
                extra = f"\nData available: {diagnostics.get('data_available','--')}/{diagnostics.get('total_pairs','--')}"
            send_message(chat_id,
                "⚠️ <b>NO VALID LIVE SIGNAL FOUND</b>\n\n"
                f"No asset passed the strict multi-timeframe + {html.escape(expiry)} confirmation on the current closed candles.{extra}\n\n"
                "Use Scan Again or wait for a fresh market setup.",
                markup([[btn("🔁 SCAN AGAIN", "scan:run")], [btn("🏠 MAIN MENU", "menu:home")]])
            )
    except Exception as exc:
        print(f"Telegram scan error: {exc}")
        send_message(chat_id, "⚠️ Scan could not complete right now. Please try again shortly.", markup([[btn("🔁 TRY AGAIN", "scan:run")], [btn("🏠 MAIN MENU", "menu:home")]]))



def _telegram_user_ref(user):
    """Use the same user reference that was used when the license was approved/issued."""
    submitted = str(user.get("submitted_id") or "").strip()
    if submitted:
        return submitted
    username = str(user.get("username") or "").strip()
    if username:
        return "@" + username
    return str(user.get("telegram_id") or "").strip()


def refresh_license_status(user, services):
    """Turn ACTIVE into EXPIRED when the underlying web/VIP license is no longer valid."""
    if str(user.get("status") or "").upper() != "ACTIVE":
        return user
    key = str(user.get("license_key") or "").strip()
    valid = False
    if key:
        try:
            valid = bool(services["validate_license"](key, _telegram_user_ref(user)))
        except Exception as exc:
            print(f"Telegram license validation warning: {exc}")
    if not valid:
        user["status"] = "EXPIRED"
        user["stage"] = "EXPIRED"
        save_user(user)
    return user



def _format_remaining(seconds):
    seconds = max(0, int(seconds or 0))
    hours, rem = divmod(seconds, 3600)
    minutes = rem // 60
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def trial_expiry_reminder_worker(services):
    """
    Background Telegram reminder:
    - one reminder when a FREE TRIAL has <= 2 hours remaining
    - one expiry notification when the trial expires
    Persistent meta markers stop repeated messages after restarts.
    """
    interval = max(30, int(os.environ.get("RAJA_TRIAL_REMINDER_INTERVAL", "60")))
    while True:
        try:
            info_fn = services.get("license_info")
            if not callable(info_fn):
                time.sleep(interval)
                continue

            now = int(time.time())
            for user in approved_users(500):
                if str(user.get("status") or "").upper() != "ACTIVE":
                    continue
                key = str(user.get("license_key") or "").strip()
                if not key:
                    continue

                try:
                    info = info_fn(key, _telegram_user_ref(user)) or {}
                except Exception as exc:
                    print(f"Trial reminder license-info warning: {exc}")
                    continue

                plan = str(info.get("plan") or "").strip().upper()
                expires_at = int(info.get("expires_at") or 0)
                if plan != "FREE TRIAL" or not expires_at:
                    continue

                remaining = expires_at - now
                marker_base = f"{user.get('telegram_id')}:{key}"

                # Send once when <= 2 hours remain.
                if 0 < remaining <= 7200:
                    reminder_key = "trial_reminder_2h:" + marker_base
                    if not get_meta(reminder_key, ""):
                        try:
                            send_message(
                                user["chat_id"],
                                "⏳ <b>FREE TRIAL EXPIRY REMINDER</b>\n\n"
                                f"Your RAJA AI free trial has approximately <b>{_format_remaining(remaining)}</b> remaining.\n\n"
                                "If you want to continue after expiry, contact admin for VIP access.",
                                markup([[btn("💬 CONTACT ADMIN", url=contact_url())]])
                            )
                            set_meta(reminder_key, str(now))
                        except Exception as exc:
                            print(f"Trial reminder send warning: {exc}")

                # Expire Telegram access automatically and notify once.
                if remaining <= 0:
                    expired_key = "trial_expired_notice:" + marker_base
                    if str(user.get("status") or "").upper() == "ACTIVE":
                        user["status"] = "EXPIRED"
                        user["stage"] = "EXPIRED"
                        save_user(user)
                    if not get_meta(expired_key, ""):
                        try:
                            send_message(
                                user["chat_id"],
                                "⌛ <b>FREE TRIAL EXPIRED</b>\n\n"
                                "Your RAJA AI free trial has ended. Market scanning is now locked for this trial.\n\n"
                                "Contact admin if you want to activate VIP access.",
                                markup([[btn("💬 CONTACT ADMIN", url=contact_url())]])
                            )
                            set_meta(expired_key, str(now))
                        except Exception as exc:
                            print(f"Trial expiry notice warning: {exc}")
        except Exception as exc:
            print(f"Trial expiry worker warning: {exc}")

        time.sleep(interval)


def handle_message(message, services):
    chat = message.get("chat") or {}
    sender = message.get("from") or {}
    if chat.get("type") != "private" or not sender.get("id"):
        return
    chat_id = int(chat.get("id"))
    user = sync_identity(sender, chat_id)
    user = refresh_license_status(user, services)
    text = str(message.get("text") or "").strip()

    if text.split(maxsplit=1)[0].lower() in {"/licenses", "/keys", "/approved"}:
        current_admin = admin_id()
        if current_admin and int(sender["id"]) == current_admin:
            show_approved_to_admin(chat_id)
        else:
            send_message(chat_id, "⛔ Admin only command.")
        return

    if text.split(maxsplit=1)[0].lower() in {"/status", "/health"}:
        runtime = dict(_TELEGRAM_RUNTIME)
        access = str(user.get("status") or "NEW").upper()
        send_message(
            chat_id,
            "🩺 <b>RAJA AI BOT STATUS</b>\n\n"
            f"Bot: <b>{'ONLINE' if runtime.get('bot_ok') else 'CHECKING'}</b>\n"
            f"Webhook: <b>{'OK' if runtime.get('webhook_ok') else 'REPAIRING'}</b>\n"
            f"Your access: <b>{html.escape(access)}</b>\n"
            f"Public server: <code>{html.escape(PUBLIC_BASE_URL)}</code>",
            start_keyboard(access == "ACTIVE"),
        )
        return

    if text.startswith("/admin"):
        parts = text.split(maxsplit=1)
        current_admin = admin_id()
        if current_admin and int(sender["id"]) == current_admin:
            show_pending_to_admin(chat_id)
            return
        code = parts[1].strip() if len(parts) > 1 else ""
        if not current_admin and ADMIN_SETUP_CODE and code and code == ADMIN_SETUP_CODE:
            set_meta("admin_telegram_id", str(sender["id"]))
            send_message(chat_id, "✅ <b>ADMIN TELEGRAM ACCOUNT BOUND</b>\n\nThis Telegram account can now approve/reject RAJA AI access requests.")
            show_pending_to_admin(chat_id)
            return
        if not current_admin:
            send_message(chat_id, "🔐 Admin is not bound yet. Set TELEGRAM_ADMIN_SETUP_CODE in Render, then send <code>/admin YOUR_CODE</code> from the admin Telegram account.")
        else:
            send_message(chat_id, "⛔ This Telegram account is not the configured admin.")
        return

    if text.startswith("/start") or text.startswith("/menu"):
        send_message(chat_id, welcome_text(user), start_keyboard(user.get("status") == "ACTIVE"))
        return

    if user.get("status") == "ACTIVE":
        send_message(chat_id, "Use the RAJA AI menu below.", start_keyboard(True))
        return

    stage = user.get("stage")
    if stage == "AWAITING_UID" and text:
        user["submitted_id"] = text[:160]
        user["stage"] = "AWAITING_LICENSE"
        save_user(user)
        send_message(chat_id,
            "✅ UID / ID received.\n\n"
            "🔑 <b>Now send your VIP License Key</b> if admin has already issued one.\n"
            "If you do not have a key yet, tap <b>REQUEST ADMIN APPROVAL</b>; a VIP key can be issued when admin approves you.",
            markup([[btn("🛡️ REQUEST ADMIN APPROVAL", "access:submit")], [btn("💬 CONTACT ADMIN", url=contact_url())]])
        )
        return

    if stage == "AWAITING_LICENSE" and text:
        user["license_key"] = text[:160]
        user["status"] = "PENDING"
        user["stage"] = "PENDING"
        save_user(user)
        notified = notify_admin_pending(user)
        send_message(chat_id,
            "⏳ <b>ACCESS REQUEST SUBMITTED</b>\n\n"
            "Your Telegram ID / Quotex UID and VIP key have been sent for admin verification.\n"
            f"Support: @{html.escape(SUPPORT_USERNAME)}" + ("" if notified else "\n\n⚠️ Admin Telegram binding is not configured yet; contact support."),
            start_keyboard(False)
        )
        return

    send_message(chat_id, welcome_text(user), start_keyboard(False))


def handle_callback(query, services):
    callback_id = query.get("id")
    sender = query.get("from") or {}
    message = query.get("message") or {}
    chat = message.get("chat") or {}
    if not sender.get("id") or not chat.get("id"):
        answer_callback(callback_id)
        return
    chat_id = int(chat["id"])
    message_id = int(message.get("message_id") or 0)
    user = sync_identity(sender, chat_id)
    user = refresh_license_status(user, services)
    data = str(query.get("data") or "")

    # Admin actions are always checked against the numeric admin Telegram ID.
    if data.startswith("admin:"):
        if admin_id() != int(sender["id"]):
            answer_callback(callback_id, "Admin only", True)
            return
        parts = data.split(":")
        if len(parts) != 3 or not parts[2].isdigit():
            answer_callback(callback_id, "Invalid request", True)
            return
        action, target_id = parts[1], int(parts[2])
        target = get_user(target_id)

        if action == "revoke":
            if str(target.get("status") or "").upper() != "ACTIVE":
                answer_callback(callback_id, f"User is {target.get('status','not active')}", True)
                return
            target["status"] = "REVOKED"
            target["stage"] = "REVOKED"
            save_user(target)
            send_message(
                target["chat_id"],
                "⛔ <b>RAJA AI TELEGRAM ACCESS REMOVED</b>\n\n"
                "Your Telegram bot access has been revoked by the admin.\n"
                f"If you believe this is a mistake, contact @{html.escape(SUPPORT_USERNAME)}.",
                markup([[btn("💬 CONTACT ADMIN", url=contact_url())]])
            )
            answer_callback(callback_id, "Access removed")
            try:
                edit_message(
                    chat_id,
                    message_id,
                    "⛔ <b>ACCESS REMOVED</b>\n\n"
                    f"Telegram ID: <code>{target_id}</code>\n"
                    f"Status: <b>REVOKED</b>"
                )
            except Exception:
                send_message(
                    chat_id,
                    f"⛔ Removed access for Telegram user <code>{target_id}</code>."
                )
            return

        if target.get("status") != "PENDING":
            answer_callback(callback_id, f"Already {target.get('status','processed')}", True)
            return
        if action == "approve":
            user_ref = (target.get("submitted_id") or ("@" + target.get("username") if target.get("username") else str(target_id))).strip()
            submitted_key = (target.get("license_key") or "").strip()
            key = None
            if submitted_key:
                try:
                    if services["validate_license"](submitted_key, user_ref):
                        key = submitted_key
                except Exception:
                    key = None
            if not key:
                key = services["issue_license"](user_ref)
            target["license_key"] = key
            target["status"] = "ACTIVE"
            target["stage"] = "ACTIVE"
            target["approved_at"] = int(time.time())
            save_user(target)
            send_message(target["chat_id"],
                "✅ <b>RAJA AI TELEGRAM ACCESS APPROVED</b>\n\n"
                "Your bot access is now active.\n"
                f"VIP License Key: <code>{html.escape(key)}</code>\n\n"
                "Keep your key private. Use AI Market Scan below to continue.",
                start_keyboard(True)
            )
            answer_callback(callback_id, "Approved")
            send_message(chat_id, f"✅ Approved Telegram user <code>{target_id}</code>. License: <code>{html.escape(key)}</code>")
            return
        if action == "reject":
            target["status"] = "REJECTED"
            target["stage"] = "REJECTED"
            save_user(target)
            send_message(target["chat_id"],
                "❌ <b>ACCESS NOT APPROVED</b>\n\nPlease contact the admin if your Quotex UID/referral details need to be checked again.",
                markup([[btn("💬 CONTACT ADMIN", url=contact_url())]])
            )
            answer_callback(callback_id, "Rejected")
            send_message(chat_id, f"❌ Rejected Telegram user <code>{target_id}</code>.")
            return

    # All market/scanning callbacks require a currently valid license.
    protected = (
        data == "menu:scan"
        or data.startswith("mkt:")
        or data.startswith("pairs:")
        or data == "pair:back"
        or data.startswith("pairauto:")
        or (data.startswith("pair:") and data != "pair:back")
        or data.startswith("exp:")
        or data == "scan:expiry"
        or data == "scan:run"
    )
    if protected and str(user.get("status") or "").upper() != "ACTIVE":
        answer_callback(callback_id, "Trial/VIP access expired or inactive", True)
        send_message(
            chat_id,
            "⌛ <b>RAJA AI ACCESS EXPIRED / INACTIVE</b>\n\n"
            "Your trial or VIP license is no longer active. Contact admin to renew access.",
            markup([[btn("💬 CONTACT ADMIN", url=contact_url())]])
        )
        return

    if data == "access:uid":
        user["stage"] = "AWAITING_UID"
        user["status"] = "NEW"
        save_user(user)
        answer_callback(callback_id)
        send_message(chat_id,
            "🆔 <b>SUBMIT TELEGRAM ID OR QUOTEX UID</b>\n\n"
            "Send the ID/UID in your next message. Your numeric Telegram ID is captured automatically as well.\n\n"
            f"Need help? @{html.escape(SUPPORT_USERNAME)}"
        )
        return

    if data == "access:key":
        user["stage"] = "AWAITING_LICENSE"
        save_user(user)
        answer_callback(callback_id)
        send_message(chat_id,
            "🔑 <b>ENTER VIP LICENSE KEY</b>\n\n"
            "Send your RAJA VIP license key in the next message. If your UID has not been submitted yet, use Request Access first."
        )
        return

    if data == "access:submit":
        if not user.get("submitted_id"):
            answer_callback(callback_id, "Submit UID first", True)
            return
        user["status"] = "PENDING"
        user["stage"] = "PENDING"
        save_user(user)
        notified = notify_admin_pending(user)
        answer_callback(callback_id, "Request sent")
        send_message(chat_id,
            "⏳ <b>ACCESS REQUEST SUBMITTED</b>\n\nAdmin will verify your UID and approve/reject your request." +
            ("" if notified else f"\n\n⚠️ Contact @{html.escape(SUPPORT_USERNAME)} because admin Telegram binding is not configured yet."),
            start_keyboard(False)
        )
        return

    if data == "menu:home":
        answer_callback(callback_id)
        send_message(chat_id, welcome_text(user), start_keyboard(user.get("status") == "ACTIVE"))
        return

    if data == "menu:status":
        answer_callback(callback_id)
        key = html.escape(user.get("license_key") or "Not issued")
        submitted = html.escape(user.get("submitted_id") or "--")
        send_message(chat_id,
            "🔐 <b>MY RAJA AI ACCESS</b>\n\n"
            f"Status: <b>{html.escape(str(user.get('status') or 'NEW'))}</b>\n"
            f"Telegram ID: <code>{user['telegram_id']}</code>\n"
            f"Submitted UID/ID: <code>{submitted}</code>\n"
            f"VIP Key: <code>{key}</code>",
            start_keyboard(user.get("status") == "ACTIVE")
        )
        return

    if data == "menu:scan":
        if user.get("status") != "ACTIVE":
            answer_callback(callback_id, "Access not active", True)
            return
        answer_callback(callback_id)
        send_message(chat_id, "📊 <b>SELECT MARKET TYPE</b>", market_keyboard())
        return

    if data.startswith("mkt:"):
        market = data.split(":",1)[1]
        if market not in MARKET_PAIRS:
            answer_callback(callback_id, "Unknown market", True)
            return
        user["market"] = market
        user["pair"] = ""
        save_user(user)
        answer_callback(callback_id)
        send_message(chat_id, f"{MARKET_LABELS[market]}\n\n<b>Select Pair / Asset</b>", pair_keyboard(market, 0))
        return

    if data.startswith("pairs:"):
        parts = data.split(":")
        if len(parts) == 3:
            market, page = parts[1], int(parts[2]) if parts[2].isdigit() else 0
            answer_callback(callback_id)
            try:
                edit_message(chat_id, message_id, f"{MARKET_LABELS.get(market, market)}\n\n<b>Select Pair / Asset</b>", pair_keyboard(market, page))
            except Exception:
                send_message(chat_id, f"{MARKET_LABELS.get(market, market)}\n\n<b>Select Pair / Asset</b>", pair_keyboard(market, page))
        return

    if data == "pair:back":
        market = user.get("market") or "CryptoLive"
        answer_callback(callback_id)
        send_message(chat_id, f"{MARKET_LABELS.get(market, market)}\n\n<b>Select Pair / Asset</b>", pair_keyboard(market, 0))
        return

    if data.startswith("pairauto:"):
        market = data.split(":",1)[1]
        user["market"] = market
        user["pair"] = "__AUTO__"
        save_user(user)
        answer_callback(callback_id)
        send_message(chat_id, f"✨ Auto Scan Best Pair · {MARKET_LABELS.get(market, market)}\n\n⏱️ <b>Select Trade Expiry</b>", expiry_keyboard())
        return

    if data.startswith("pair:") and data != "pair:back":
        parts = data.split(":")
        if len(parts) == 3 and parts[2].isdigit():
            market, index = parts[1], int(parts[2])
            pairs = MARKET_PAIRS.get(market, [])
            if 0 <= index < len(pairs):
                user["market"] = market
                user["pair"] = pairs[index]
                save_user(user)
                answer_callback(callback_id)
                send_message(chat_id, f"Asset: <b>{html.escape(pairs[index])}</b>\n\n⏱️ <b>Select Trade Expiry</b>", expiry_keyboard())
                return
        answer_callback(callback_id, "Invalid pair", True)
        return

    if data.startswith("exp:"):
        expiry = data.split(":",1)[1]
        if expiry not in VALID_EXPIRIES:
            answer_callback(callback_id, "Unsupported expiry", True)
            return
        user["expiry"] = expiry
        save_user(user)
        pair_label = "Auto Scan Best Pair" if user.get("pair") == "__AUTO__" else user.get("pair")
        answer_callback(callback_id)
        send_message(chat_id,
            "✅ <b>SCAN CONFIGURATION READY</b>\n\n"
            f"Market: {html.escape(MARKET_LABELS.get(user.get('market'), user.get('market') or '--'))}\n"
            f"Pair: <b>{html.escape(pair_label or '--')}</b>\n"
            f"Expiry: <b>{html.escape(expiry)}</b>",
            ready_to_scan_keyboard()
        )
        return

    if data == "scan:expiry":
        answer_callback(callback_id)
        send_message(chat_id, "⏱️ <b>Select Trade Expiry</b>", expiry_keyboard())
        return

    if data == "scan:run":
        if user.get("status") != "ACTIVE":
            answer_callback(callback_id, "Access not active", True)
            return
        if not user.get("market") or not user.get("pair") or not user.get("expiry"):
            answer_callback(callback_id, "Choose market, pair and expiry first", True)
            send_message(chat_id, "📊 <b>Select Market</b>", market_keyboard())
            return
        answer_callback(callback_id, "Scan started")
        pair_label = "best pair" if user.get("pair") == "__AUTO__" else user.get("pair")
        send_message(chat_id,
            "🔎 <b>AI MARKET SCAN STARTED</b>\n\n"
            f"Scanning {html.escape(str(pair_label))} with {html.escape(user.get('expiry') or '1m')} confirmation.\n"
            "I will send the result here when the scan finishes."
        )
        threading.Thread(target=_run_scan, args=(dict(user), services), daemon=True).start()
        return

    answer_callback(callback_id)


def handle_update(update, services):
    try:
        if update.get("callback_query"):
            handle_callback(update["callback_query"], services)
        elif update.get("message"):
            handle_message(update["message"], services)
    except Exception as exc:
        print(f"Telegram update handler error: {exc}")


def configure_webhook():
    if not BOT_TOKEN or not PUBLIC_BASE_URL:
        _set_runtime(webhook_ok=False, last_error="Bot token or RAJA_PUBLIC_BASE_URL is missing")
        return False
    if not PUBLIC_BASE_URL.lower().startswith("https://"):
        _set_runtime(webhook_ok=False, last_error="Telegram webhook requires an HTTPS RAJA_PUBLIC_BASE_URL")
        return False
    try:
        me = tg_api("getMe", {}, timeout=12) or {}
        actual_username = str(me.get("username") or BOT_USERNAME).lstrip("@")
    except Exception as exc:
        _set_runtime(bot_ok=False, webhook_ok=False, last_error=str(exc)[:300])
        raise

    webhook_url = f"{PUBLIC_BASE_URL}{TELEGRAM_WEBHOOK_PATH}"
    payload = {
        "url": webhook_url,
        "allowed_updates": ["message", "callback_query"],
        "drop_pending_updates": False,
        "max_connections": 40,
    }
    if WEBHOOK_SECRET:
        payload["secret_token"] = WEBHOOK_SECRET

    result = tg_api("setWebhook", payload, timeout=20)
    configure_bot_commands()
    info = get_webhook_info()
    actual_url = str(info.get("url") or "")
    last_error = str(info.get("last_error_message") or "")
    ok = bool(result and actual_url == webhook_url)
    _set_runtime(bot_ok=True, bot_username=actual_username, webhook_ok=ok, webhook_url=actual_url,
                 last_error=last_error, last_configured=int(time.time()))
    print(f"Telegram webhook configured for @{actual_username}: {ok} -> {actual_url}")
    return ok


def telegram_webhook_monitor():
    # Railway may start the process before the public URL is fully reachable.
    # Retry automatically so one failed cold-start does not leave the bot dead.
    for delay in (3, 8, 20):
        time.sleep(delay)
        try:
            if telegram_self_test():
                break
            configure_webhook()
            if telegram_self_test():
                break
        except Exception as exc:
            print(f"Telegram webhook startup retry warning: {exc}")

    while TELEGRAM_AUTO_WEBHOOK and BOT_TOKEN:
        time.sleep(TELEGRAM_WEBHOOK_REPAIR_INTERVAL)
        try:
            if not telegram_self_test():
                configure_webhook()
        except Exception as exc:
            print(f"Telegram webhook monitor warning: {exc}")


def _start_webhook_monitor_once():
    global _TELEGRAM_MONITOR_STARTED
    if not TELEGRAM_AUTO_WEBHOOK or not BOT_TOKEN:
        return
    with _TELEGRAM_MONITOR_LOCK:
        if _TELEGRAM_MONITOR_STARTED:
            return
        _TELEGRAM_MONITOR_STARTED = True
        threading.Thread(target=telegram_webhook_monitor, daemon=True,
                         name="raja-telegram-webhook-monitor").start()


def register_telegram_routes(app, services):
    init_telegram_store()

    @app.route("/telegram/webhook", methods=["POST"])
    def telegram_webhook():
        if not BOT_TOKEN:
            return jsonify({"ok": False, "message": "Telegram bot is not configured."}), 503
        if WEBHOOK_SECRET:
            provided = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
            if provided != WEBHOOK_SECRET:
                return jsonify({"ok": False, "message": "Invalid webhook secret."}), 403
        update = request.get_json(silent=True) or {}
        # Telegram expects a fast webhook acknowledgement. Process message/callback work
        # in a bounded worker pool so network calls back to Telegram cannot block the webhook.
        try:
            TELEGRAM_UPDATE_POOL.submit(handle_update, update, services)
        except Exception as exc:
            print(f"Telegram update queue warning: {exc}")
            threading.Thread(target=handle_update, args=(update, services), daemon=True).start()
        return jsonify({"ok": True})

    @app.route("/telegram/health", methods=["GET"])
    def telegram_health():
        runtime = dict(_TELEGRAM_RUNTIME)
        return jsonify({
            "status": "ok" if BOT_TOKEN else "not_configured",
            "telegram_enabled": bool(BOT_TOKEN),
            "bot_ok": bool(runtime.get("bot_ok")),
            "bot_username": runtime.get("bot_username") or BOT_USERNAME,
            "support_username": SUPPORT_USERNAME,
            "admin_bound": bool(admin_id()),
            "public_base_url": PUBLIC_BASE_URL,
            "expected_webhook_url": f"{PUBLIC_BASE_URL}{TELEGRAM_WEBHOOK_PATH}" if PUBLIC_BASE_URL else "",
            "webhook_ok": bool(runtime.get("webhook_ok")),
            "webhook_url": runtime.get("webhook_url") or "",
            "last_error": runtime.get("last_error") or "",
            "last_check": runtime.get("last_check") or 0,
            "last_configured": runtime.get("last_configured") or 0,
            "auto_repair": bool(TELEGRAM_AUTO_WEBHOOK),
        })

    @app.route("/telegram/repair", methods=["POST"])
    def telegram_repair():
        data = request.get_json(silent=True) or {}
        if not ADMIN_SETUP_CODE:
            return jsonify({"ok": False, "message": "TELEGRAM_ADMIN_SETUP_CODE is not configured."}), 503
        if str(data.get("setup_code") or "") != ADMIN_SETUP_CODE:
            return jsonify({"ok": False, "message": "Invalid setup code."}), 403
        try:
            ok = configure_webhook()
            return jsonify({"ok": bool(ok), "runtime": dict(_TELEGRAM_RUNTIME)})
        except Exception as exc:
            return jsonify({"ok": False, "message": str(exc)[:300], "runtime": dict(_TELEGRAM_RUNTIME)}), 503

    if BOT_TOKEN:
        _start_webhook_monitor_once()
        threading.Thread(
            target=trial_expiry_reminder_worker, args=(services,), daemon=True,
            name="raja-trial-expiry-reminder",
        ).start()
    else:
        _set_runtime(last_error="TELEGRAM_BOT_TOKEN is not set")
        print("Telegram integration loaded but TELEGRAM_BOT_TOKEN is not set.")
