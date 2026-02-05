# kissclicker-bot (FULL)
# python-telegram-bot==20.7
# psycopg2-binary==2.9.9

import os
import logging
import time
import asyncio
import html
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict

import psycopg2
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.error import TimedOut, Conflict
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# =========================
# ===== НАСТРОЙКИ =========
# =========================
TOKEN = os.getenv("TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# ✅ если надо несколько админов — добавляй сюда
ADMIN_IDS = {1924971257}

CHANNEL_ID = "@kisspromochannel"

# Экономика кликов
BASE_CLICKS_LIMIT = 2000  # ✅ базовый лимит (без VIP, без улучшений)
CLICK_RESET_HOURS = 3
MIN_WITHDRAW = 1000

# Рефералка / бонусы
REF_REWARD = 150
DAILY_BONUS_AMOUNT = 500
DAILY_BONUS_HOURS = 24
REF_MILESTONES = [(10, 1000), (50, 5000), (100, 10000)]

# VIP
VIP_LIMITS = {"VIP": 2500, "MVP": 3500, "PREMIUM": 4000}
VIP_ICONS = {"VIP": "🏆", "MVP": "💎", "PREMIUM": "💲"}
VIP_ORDER = {"VIP": 1, "MVP": 2, "PREMIUM": 3}

# =========================
# ===== КОСМЕТИКА =========
# =========================
COSMETIC_CHANGE_COOLDOWN_SEC = 10

# Титулы (коды)
TITLE_LABELS = {
    "ROOKIE": "Rookie",
    "LEGEND": "Legend",
    "MASTER": "Master",
    "BETA_TESTER": "Beta Tester",
    "OLD": "Old",
    "ADMIN": "Admin",
    "MODER": "Moder",
    "GRAND_MASTER": "Grand Master",
    "RICH": "Rich",
    "ELITE": "Elite",
    "KING": "KING",
    "ETERNITY": "Eternity",
    "STINGER": "Stinger",
    "DEV": "DEV",
    "OWNER": "OWNER",

    # авто за клики
    "MASTER_CLICK": "Master Click",
    "ELITE_CLICKER": "Elite Clicker",
    "ULTRA_CLICKER": "Ultra Clicker",
    "IMPOSSIBLE_CLICKER": "Impossible Clicker",
}

# Авто-титулы по total_clicks
AUTO_TITLES = [
    (0, "ROOKIE"),
    (5000, "MASTER_CLICK"),
    (8000, "ELITE_CLICKER"),
    (13000, "ULTRA_CLICKER"),
    (20000, "IMPOSSIBLE_CLICKER"),
]

# Фоны (коды + цены)
THEMES = [
    ("FIRE", "🔥 Огненный", 1200),
    ("DARK", "🌑 Тёмный", 1700),
    ("CRYSTAL", "💎 Кристальный", 2300),
    ("ICE", "❄️ Ледяной", 2300),
    ("NEWYEAR", "🎄 Новогодний", 2700),
    ("CHOC", "🍫 Шоколадный", 3000),
    ("TOP", "⭐️ Топовый", 4000),
]
THEME_BY_CODE = {c: (label, price) for c, label, price in THEMES}

# =========================
# ===== УЛУЧШЕНИЯ =========
# =========================
UPGRADE_MAX_LEVEL = 10
UPGRADE_BONUS = {
    0: 0,
    1: 200,
    2: 250,
    3: 300,
    4: 350,
    5: 400,
    6: 450,
    7: 500,
    8: 600,
    9: 800,
    10: 1000,
}
UPGRADE_COST = {
    0: 3500,   # 0->1
    1: 5000,   # 1->2
    2: 7000,   # 2->3
    3: 9000,   # 3->4
    4: 12000,  # 4->5
    5: 14500,  # 5->6
    6: 17000,  # 6->7
    7: 19500,  # 7->8
    8: 22000,  # 8->9
    9: 25000,  # 9->10
}

def click_reward_for_level(lvl: int) -> int:
    if lvl >= 10:
        return 3
    if lvl >= 5:
        return 2
    return 1

# =========================
# ===== КЕЙСЫ =============
# =========================
CASE_RESET_HOURS = 12
CASE_LIMITS = {"COMMON": 7, "RARE": 4, "LEGENDARY": 2}

CASE_PRICES = {"COMMON": 500, "RARE": 1000, "LEGENDARY": 3000}
CASE_LABELS = {"COMMON": "📦 Обычный", "RARE": "🎁 Редкий", "LEGENDARY": "💎 Легендарный"}

# (reward_type, value, chance%)
# reward_type: "gold" / "vip"
CASE_DROPS = {
    "COMMON": [
        ("gold", 100, 45),
        ("gold", 250, 25),
        ("gold", 700, 15),
        ("gold", 1000, 8),
        ("vip", ("VIP", 1, "дн"), 3),
        ("vip", ("MVP", 1, "дн"), 2),
        ("gold", 2000, 2),
    ],
    "RARE": [
        ("gold", 400, 45),
        ("gold", 700, 25),
        ("gold", 1400, 15),
        ("gold", 1700, 8),
        ("vip", ("MVP", 3, "дн"), 4),
        ("vip", ("PREMIUM", 1, "дн"), 2),
        ("gold", 4000, 1),
    ],
    "LEGENDARY": [
        ("gold", 1000, 35),
        ("gold", 1500, 25),
        ("gold", 3300, 18),
        ("gold", 3900, 10),
        ("vip", ("MVP", 5, "дн"), 6),
        ("vip", ("PREMIUM", 3, "дн"), 4),
        ("gold", 6500, 2),
    ],
}

CASE_ANIM_SECONDS = 9  # 7-10 сек интрига
CASE_SPAM_COOLDOWN_SEC = 3

# =========================
# ===== КАЗИНО ============
# =========================
CASINO_MIN_BET = 100
CASINO_MAX_BET = 500000
CASINO_COOLDOWN_SEC = 5
CASINO_COEFF = {
    "bm": 1.8,   # больше/меньше
    "pn": 1.8,   # чет/нечет
    "num": 2.5,  # угадай число
}

# =========================
# ===== ЛОГИ ==============
# =========================
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("kissclicker-bot")

# =========================
# ===== POSTGRES DB =======
# =========================
conn = None

def _parse_db_url(db_url: str) -> dict:
    from urllib.parse import urlparse, unquote
    u = urlparse(db_url)
    if u.scheme not in ("postgres", "postgresql"):
        raise RuntimeError("DATABASE_URL должен начинаться с postgres:// или postgresql://")
    user = unquote(u.username) if u.username else None
    password = unquote(u.password) if u.password else None
    host = u.hostname
    port = u.port or 5432
    dbname = (u.path or "").lstrip("/") or "railway"
    if not host:
        raise RuntimeError("DATABASE_URL без host")
    return {
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": user,
        "password": password,
        "sslmode": "require",
    }

def db_connect():
    global conn
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL не найден. Railway -> Variables -> DATABASE_URL")
    cfg = _parse_db_url(DATABASE_URL)
    last_err = None
    for attempt in range(1, 11):
        try:
            conn = psycopg2.connect(**cfg)
            conn.autocommit = True
            logger.info("✅ Postgres connected")
            return
        except Exception as e:
            last_err = e
            logger.warning(f"DB connect failed ({attempt}/10): {e}")
            time.sleep(1.2)
    raise RuntimeError(f"Не удалось подключиться к Postgres: {last_err}")

def db_exec(query: str, params: tuple = ()):
    with conn.cursor() as cur:
        cur.execute(query, params)

def db_fetchone(query: str, params: tuple = ()):
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone()

def db_fetchall(query: str, params: tuple = ()):
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()

def init_db():
    db_exec(
        """
        CREATE TABLE IF NOT EXISTS users (
            id BIGINT PRIMARY KEY,
            balance DOUBLE PRECISION DEFAULT 0,
            banned INTEGER DEFAULT 0,
            clicks_used INTEGER DEFAULT 0,
            last_click_reset TEXT,
            subscribed INTEGER DEFAULT 0,
            total_clicks BIGINT DEFAULT 0,
            username TEXT DEFAULT NULL,

            vip_type TEXT DEFAULT NULL,
            vip_until TEXT DEFAULT NULL,

            last_daily_bonus TEXT DEFAULT NULL,

            ref_bonus_10 INTEGER DEFAULT 0,
            ref_bonus_50 INTEGER DEFAULT 0,
            ref_bonus_100 INTEGER DEFAULT 0,

            -- кейсы (инвентарь)
            case_common INTEGER DEFAULT 0,
            case_rare INTEGER DEFAULT 0,
            case_legendary INTEGER DEFAULT 0,

            -- лимиты кейсов за 12 часов
            case_window_start TEXT DEFAULT NULL,
            case_open_common INTEGER DEFAULT 0,
            case_open_rare INTEGER DEFAULT 0,
            case_open_legendary INTEGER DEFAULT 0,

            -- косметика
            active_title TEXT DEFAULT 'ROOKIE',
            active_theme TEXT DEFAULT NULL,
            last_cosmetic_change TEXT DEFAULT NULL,

            -- улучшения
            upgrade_level INTEGER DEFAULT 0,

            -- антиспам
            last_case_action TEXT DEFAULT NULL,
            last_casino_action TEXT DEFAULT NULL
        )
        """
    )

    db_exec(
        """
        CREATE TABLE IF NOT EXISTS referrals (
            user_id BIGINT PRIMARY KEY,
            referrer_id BIGINT,
            rewarded INTEGER DEFAULT 0
        )
        """
    )

    db_exec(
        """
        CREATE TABLE IF NOT EXISTS withdrawals (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            amount DOUBLE PRECISION,
            requisites TEXT,
            status TEXT DEFAULT 'pending',
            admin_note TEXT DEFAULT NULL,
            decided_at TEXT DEFAULT NULL
        )
        """
    )

    db_exec(
        """
        CREATE TABLE IF NOT EXISTS promocodes (
            code TEXT PRIMARY KEY,
            amount DOUBLE PRECISION,
            uses_left INTEGER DEFAULT 1
        )
        """
    )

    db_exec(
        """
        CREATE TABLE IF NOT EXISTS used_promocodes (
            user_id BIGINT,
            code TEXT,
            PRIMARY KEY(user_id, code)
        )
        """
    )

    # Владение титулами/фонами (сроки)
    db_exec(
        """
        CREATE TABLE IF NOT EXISTS user_titles (
            user_id BIGINT,
            title_code TEXT,
            expires_at TEXT DEFAULT NULL,
            PRIMARY KEY(user_id, title_code)
        )
        """
    )
    db_exec(
        """
        CREATE TABLE IF NOT EXISTS user_themes (
            user_id BIGINT,
            theme_code TEXT,
            expires_at TEXT DEFAULT NULL,
            PRIMARY KEY(user_id, theme_code)
        )
        """
    )

def now_iso():
    return datetime.now().isoformat(timespec="seconds")

def now_human():
    return datetime.now().strftime("%d.%m.%Y %H:%M")

def fmt_td(td: timedelta) -> str:
    sec = int(td.total_seconds())
    if sec < 0:
        return "0м"
    d = sec // 86400
    h = (sec % 86400) // 3600
    m = (sec % 3600) // 60
    if d > 0:
        return f"{d}д {h}ч {m}м"
    if h > 0:
        return f"{h}ч {m}м"
    return f"{m}м"

def ensure_user(user_id: int, username: Optional[str] = None):
    db_exec("INSERT INTO users (id) VALUES (%s) ON CONFLICT (id) DO NOTHING", (user_id,))
    if username:
        db_exec("UPDATE users SET username=%s WHERE id=%s", (username, user_id))
    # гарантируем rookie в владении
    db_exec(
        "INSERT INTO user_titles (user_id, title_code, expires_at) VALUES (%s,%s,NULL) "
        "ON CONFLICT (user_id, title_code) DO NOTHING",
        (user_id, "ROOKIE"),
    )
    # если active_title пустой — ставим rookie
    r = db_fetchone("SELECT active_title FROM users WHERE id=%s", (user_id,))
    if r and (r[0] is None or str(r[0]).strip() == ""):
        db_exec("UPDATE users SET active_title='ROOKIE' WHERE id=%s", (user_id,))

async def safe_reply(update: Update, text: str, reply_markup=None, parse_mode: Optional[str] = None):
    try:
        if update.message:
            return await update.message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=True)
    except TimedOut:
        try:
            if update.message:
                return await update.message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=True)
        except Exception as e:
            logger.warning(f"safe_reply second try failed: {e}")
    except Exception as e:
        logger.warning(f"safe_reply failed: {e}")

async def is_subscribed(bot, user_id: int):
    try:
        member = await bot.get_chat_member(CHANNEL_ID, user_id)
        return member.status in ["member", "administrator", "creator"]
    except Exception:
        return False

# =========================
# ===== МЕНЮ ==============
# =========================
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def main_menu(user_id: int):
    buttons = [
        ["👤 Профиль", "💰 Заработать"],
        ["👥 Рефералка", "💸 Вывод"],
        ["🎁 Ввести промокод"],
    ]
    if is_admin(user_id):
        buttons.append(["🛠 Админка"])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def earn_menu():
    return ReplyKeyboardMarkup([["👆 КЛИК"], ["🔙 Назад"]], resize_keyboard=True)

def admin_menu():
    return ReplyKeyboardMarkup(
        [
            ["Создать промокод", "Выдать баланс"],
            ["Забрать баланс", "Бан/Разбан"],
            ["🎖 Выдать привилегию", "🏷 Выдать титул"],
            ["🌌 Выдать фон", "Рассылка"],
            ["📋 Заявки на вывод", "Все промокоды"],
            ["🔙 Назад"],
        ],
        resize_keyboard=True,
    )

def cancel_menu():
    return ReplyKeyboardMarkup([["❌ Отмена"], ["🔙 Назад"]], resize_keyboard=True)

def subscribe_menu():
    return ReplyKeyboardMarkup([["🔔 Подписаться"], ["✅ Я подписался"]], resize_keyboard=True)

def profile_inline_menu():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🎁 Ежедневный бонус", callback_data="daily_bonus")],
            [InlineKeyboardButton("🏆 ТОПЫ", callback_data="tops")],
            [InlineKeyboardButton("🎯 Бонусы за рефералов", callback_data="ref_bonuses")],
            [InlineKeyboardButton("📦 Кейсы", callback_data="cases")],
            [InlineKeyboardButton("🎨 Косметика", callback_data="cosmetics")],
            [InlineKeyboardButton("⚡ Улучшения", callback_data="upgrades")],
            [InlineKeyboardButton("🎲 КАЗИНО", callback_data="casino")],
        ]
    )

def tops_inline_menu():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📊 Топ по кликам", callback_data="top_clicks")],
            [InlineKeyboardButton("💰 Топ по балансу", callback_data="top_balance")],
            [InlineKeyboardButton("👥 Топ рефоводов", callback_data="top_refs")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_profile")],
        ]
    )

def ref_bonuses_inline_menu(claimed10: int, claimed50: int, claimed100: int):
    buttons = []
    buttons.append([InlineKeyboardButton("✅ 10 рефов — получено" if claimed10 else "🎁 Забрать за 10 рефов", callback_data="noop" if claimed10 else "claim_ref_10")])
    buttons.append([InlineKeyboardButton("✅ 50 рефов — получено" if claimed50 else "🎁 Забрать за 50 рефов", callback_data="noop" if claimed50 else "claim_ref_50")])
    buttons.append([InlineKeyboardButton("✅ 100 рефов — получено" if claimed100 else "🎁 Забрать за 100 рефов", callback_data="noop" if claimed100 else "claim_ref_100")])
    buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_profile")])
    return InlineKeyboardMarkup(buttons)

# ===== Кейсы UI
def cases_inline_menu(common: int, rare: int, leg: int):
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"📦 Обычный (x{common}) — Открыть", callback_data="case_open_COMMON")],
            [InlineKeyboardButton(f"🎁 Редкий (x{rare}) — Открыть", callback_data="case_open_RARE")],
            [InlineKeyboardButton(f"💎 Легендарный (x{leg}) — Открыть", callback_data="case_open_LEGENDARY")],
            [InlineKeyboardButton("🛒 Магазин кейсов", callback_data="case_shop")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_profile")],
        ]
    )

def cases_shop_menu():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"📦 Обычный — купить за {CASE_PRICES['COMMON']}G", callback_data="case_buy_COMMON")],
            [InlineKeyboardButton(f"🎁 Редкий — купить за {CASE_PRICES['RARE']}G", callback_data="case_buy_RARE")],
            [InlineKeyboardButton(f"💎 Легендарный — купить за {CASE_PRICES['LEGENDARY']}G", callback_data="case_buy_LEGENDARY")],
            [InlineKeyboardButton("📜 Что может выпасть", callback_data="case_info")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="cases")],
        ]
    )

# ===== Косметика UI
def cosmetics_menu():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🏷 Титул", callback_data="cos_title")],
            [InlineKeyboardButton("🌌 Фон", callback_data="cos_theme")],
            [InlineKeyboardButton("🧱 Рамка", callback_data="cos_frame_info")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_profile")],
        ]
    )

def upgrades_menu():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("⬆️ Улучшить", callback_data="upgrade_buy")],
            [InlineKeyboardButton("📜 Инфо уровней", callback_data="upgrade_info")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_profile")],
        ]
    )

def casino_menu():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📉 Куб: Больше / Меньше", callback_data="casino_game_bm")],
            [InlineKeyboardButton("⚫ Куб: Чёт / Нечёт", callback_data="casino_game_pn")],
            [InlineKeyboardButton("🎯 Куб: Угадай число (1–6)", callback_data="casino_game_num")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_profile")],
        ]
    )

def casino_choice_menu(game: str):
    if game == "bm":
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📈 Больше (4–6)", callback_data="casino_pick_bigger"),
             InlineKeyboardButton("📉 Меньше (1–3)", callback_data="casino_pick_smaller")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="casino")],
        ])
    if game == "pn":
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("⚫ Чёт", callback_data="casino_pick_even"),
             InlineKeyboardButton("⚪ Нечёт", callback_data="casino_pick_odd")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="casino")],
        ])
    # num
    rows = [
        [InlineKeyboardButton("1", callback_data="casino_pick_num_1"),
         InlineKeyboardButton("2", callback_data="casino_pick_num_2"),
         InlineKeyboardButton("3", callback_data="casino_pick_num_3")],
        [InlineKeyboardButton("4", callback_data="casino_pick_num_4"),
         InlineKeyboardButton("5", callback_data="casino_pick_num_5"),
         InlineKeyboardButton("6", callback_data="casino_pick_num_6")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="casino")],
    ]
    return InlineKeyboardMarkup(rows)

# =========================
# ===== ЛИМИТЫ / VIP ======
# =========================
def parse_duration(value: str, unit: str) -> Optional[timedelta]:
    try:
        v = int(value)
    except Exception:
        return None
    u = unit.lower()
    if u.startswith("мин") or u.startswith("m"):
        return timedelta(minutes=v)
    if u.startswith("час") or u.startswith("h"):
        return timedelta(hours=v)
    if u.startswith("дн") or u.startswith("d"):
        return timedelta(days=v)
    return None

def parse_duration_short(s: str) -> Optional[timedelta]:
    s = (s or "").strip().lower()
    if s == "infinity":
        return None
    try:
        if s.endswith("м"):
            return timedelta(minutes=int(s[:-1]))
        if s.endswith("ч"):
            return timedelta(hours=int(s[:-1]))
        if s.endswith("д"):
            return timedelta(days=int(s[:-1]))
    except Exception:
        return None
    return None

def vip_is_active(vip_type: Optional[str], vip_until: Optional[str]) -> Tuple[Optional[str], Optional[datetime]]:
    if not vip_type or not vip_until:
        return None, None
    try:
        until_dt = datetime.fromisoformat(vip_until)
    except Exception:
        return None, None
    if datetime.now() >= until_dt:
        return None, None
    return vip_type, until_dt

def compute_current_limit(user_id: int) -> int:
    row = db_fetchone("SELECT vip_type, vip_until, upgrade_level FROM users WHERE id=%s", (user_id,))
    vip_type, vip_until, lvl = row if row else (None, None, 0)
    lvl = int(lvl or 0)
    bonus = UPGRADE_BONUS.get(lvl, 0)
    active_vip, _until = vip_is_active(vip_type, vip_until)
    base = VIP_LIMITS.get(active_vip, BASE_CLICKS_LIMIT) if active_vip else BASE_CLICKS_LIMIT
    return int(base + bonus)

def compute_click_reward(user_id: int) -> int:
    row = db_fetchone("SELECT upgrade_level FROM users WHERE id=%s", (user_id,))
    lvl = int(row[0] or 0) if row else 0
    return click_reward_for_level(lvl)

def check_click_reset(user_id: int) -> Tuple[int, datetime, int]:
    row = db_fetchone("SELECT last_click_reset, clicks_used FROM users WHERE id=%s", (user_id,))
    now = datetime.now()
    limit = compute_current_limit(user_id)

    if not row or row[0] is None:
        db_exec("UPDATE users SET last_click_reset=%s, clicks_used=0 WHERE id=%s", (now.strftime("%Y-%m-%d %H:%M:%S"), user_id))
        return 0, now + timedelta(hours=CLICK_RESET_HOURS), limit

    last_reset = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
    next_reset = last_reset + timedelta(hours=CLICK_RESET_HOURS)
    if now >= next_reset:
        db_exec("UPDATE users SET last_click_reset=%s, clicks_used=0 WHERE id=%s", (now.strftime("%Y-%m-%d %H:%M:%S"), user_id))
        return 0, now + timedelta(hours=CLICK_RESET_HOURS), limit

    used = int(row[1] or 0)
    return used, next_reset, limit

# =========================
# ===== ВЫВОД НИКА / ТОПЫ ==
# =========================
def esc(s: str) -> str:
    return html.escape(s or "", quote=False)

def title_label(code: Optional[str]) -> str:
    if not code:
        return "Rookie"
    return TITLE_LABELS.get(code, code)

def format_user_link(username: Optional[str], user_id: int) -> str:
    # username -> @user
    if username:
        return f"@{esc(username)}"
    # id -> clickable tg://
    return f'<a href="tg://user?id={user_id}">{user_id}</a>'

def get_active_title(user_id: int) -> str:
    row = db_fetchone("SELECT active_title FROM users WHERE id=%s", (user_id,))
    code = row[0] if row else "ROOKIE"
    return code or "ROOKIE"

def display_in_top(user_id: int, username: Optional[str]) -> str:
    tcode = get_active_title(user_id)
    tname = title_label(tcode)
    return f"[{esc(tname)}] {format_user_link(username, user_id)}"

# =========================
# ===== АВТО-ТИТУЛЫ =======
# =========================
def best_auto_title(total_clicks: int) -> str:
    best = "ROOKIE"
    for need, code in AUTO_TITLES:
        if total_clicks >= need:
            best = code
    return best

def grant_title(user_id: int, code: str, expires_at: Optional[str]):
    db_exec(
        "INSERT INTO user_titles (user_id, title_code, expires_at) VALUES (%s,%s,%s) "
        "ON CONFLICT (user_id, title_code) DO UPDATE SET expires_at=EXCLUDED.expires_at",
        (user_id, code, expires_at),
    )

def cleanup_expired_cosmetics(user_id: int):
    now = datetime.now()
    # titles
    rows = db_fetchall("SELECT title_code, expires_at FROM user_titles WHERE user_id=%s", (user_id,))
    for code, exp in rows:
        if exp:
            try:
                dt = datetime.fromisoformat(exp)
                if now >= dt and code != "ROOKIE":
                    db_exec("DELETE FROM user_titles WHERE user_id=%s AND title_code=%s", (user_id, code))
            except Exception:
                pass

    # themes
    rows = db_fetchall("SELECT theme_code, expires_at FROM user_themes WHERE user_id=%s", (user_id,))
    for code, exp in rows:
        if exp:
            try:
                dt = datetime.fromisoformat(exp)
                if now >= dt:
                    db_exec("DELETE FROM user_themes WHERE user_id=%s AND theme_code=%s", (user_id, code))
                    # если активный — снять
                    r = db_fetchone("SELECT active_theme FROM users WHERE id=%s", (user_id,))
                    if r and r[0] == code:
                        db_exec("UPDATE users SET active_theme=NULL WHERE id=%s", (user_id,))
            except Exception:
                pass

def can_change_cosmetic(user_id: int) -> Tuple[bool, int]:
    r = db_fetchone("SELECT last_cosmetic_change FROM users WHERE id=%s", (user_id,))
    if not r or not r[0]:
        return True, 0
    try:
        last = datetime.fromisoformat(r[0])
    except Exception:
        return True, 0
    left = (last + timedelta(seconds=COSMETIC_CHANGE_COOLDOWN_SEC)) - datetime.now()
    if left.total_seconds() <= 0:
        return True, 0
    return False, int(left.total_seconds())

def mark_cosmetic_change(user_id: int):
    db_exec("UPDATE users SET last_cosmetic_change=%s WHERE id=%s", (now_iso(), user_id))

# =========================
# ===== КЕЙСЫ: ЛИМИТЫ =====
# =========================
def case_reset_if_needed(user_id: int):
    r = db_fetchone("SELECT case_window_start FROM users WHERE id=%s", (user_id,))
    start = r[0] if r else None
    now = datetime.now()
    if not start:
        db_exec(
            "UPDATE users SET case_window_start=%s, case_open_common=0, case_open_rare=0, case_open_legendary=0 WHERE id=%s",
            (now_iso(), user_id),
        )
        return
    try:
        dt = datetime.fromisoformat(start)
    except Exception:
        dt = now
    if now >= dt + timedelta(hours=CASE_RESET_HOURS):
        db_exec(
            "UPDATE users SET case_window_start=%s, case_open_common=0, case_open_rare=0, case_open_legendary=0 WHERE id=%s",
            (now_iso(), user_id),
        )

def get_case_counts(user_id: int) -> Tuple[int, int, int]:
    r = db_fetchone("SELECT case_common, case_rare, case_legendary FROM users WHERE id=%s", (user_id,))
    if not r:
        return 0, 0, 0
    return int(r[0] or 0), int(r[1] or 0), int(r[2] or 0)

def get_case_opens(user_id: int) -> Dict[str, int]:
    case_reset_if_needed(user_id)
    r = db_fetchone("SELECT case_open_common, case_open_rare, case_open_legendary FROM users WHERE id=%s", (user_id,))
    if not r:
        return {"COMMON": 0, "RARE": 0, "LEGENDARY": 0}
    return {"COMMON": int(r[0] or 0), "RARE": int(r[1] or 0), "LEGENDARY": int(r[2] or 0)}

def case_can_open(user_id: int, ctype: str) -> Tuple[bool, str]:
    inv = get_case_counts(user_id)
    inv_map = {"COMMON": inv[0], "RARE": inv[1], "LEGENDARY": inv[2]}
    if inv_map.get(ctype, 0) <= 0:
        return False, "❌ У тебя нет этого кейса."
    opens = get_case_opens(user_id)[ctype]
    if opens >= CASE_LIMITS[ctype]:
        return False, f"⏳ Лимит на {CASE_LABELS[ctype]} исчерпан (раз в {CASE_RESET_HOURS}ч)."
    # спам-кулдаун
    r = db_fetchone("SELECT last_case_action FROM users WHERE id=%s", (user_id,))
    if r and r[0]:
        try:
            last = datetime.fromisoformat(r[0])
            if (datetime.now() - last).total_seconds() < CASE_SPAM_COOLDOWN_SEC:
                return False, "⏳ Не так быстро 🙂"
        except Exception:
            pass
    return True, ""

def mark_case_action(user_id: int):
    db_exec("UPDATE users SET last_case_action=%s WHERE id=%s", (now_iso(), user_id))

def weighted_choice(items):
    # items: list of (type, value, chance_int)
    total = sum(int(x[2]) for x in items)
    import random
    r = random.randint(1, total)
    acc = 0
    for t, v, p in items:
        acc += int(p)
        if r <= acc:
            return t, v
    return items[-1][0], items[-1][1]

def vip_apply_reward(user_id: int, vip_type: str, dur: timedelta) -> Tuple[bool, str]:
    row = db_fetchone("SELECT vip_type, vip_until FROM users WHERE id=%s", (user_id,))
    cur_type, cur_until = row if row else (None, None)

    # если текущий активный VIP выше — не трогаем
    cur_active, cur_active_until = vip_is_active(cur_type, cur_until)
    if cur_active and VIP_ORDER.get(cur_active, 0) > VIP_ORDER.get(vip_type, 0):
        return False, "⚠️ У вас уже есть привилегия выше!"

    now = datetime.now()
    if cur_active and cur_active == vip_type and cur_active_until:
        # продлеваем
        new_until = cur_active_until + dur
    else:
        new_until = now + dur

    db_exec("UPDATE users SET vip_type=%s, vip_until=%s WHERE id=%s", (vip_type, new_until.isoformat(), user_id))
    return True, f"🎖 Выдано: {vip_type} (+{dur.days}д {dur.seconds//3600}ч)"

# =========================
# ===== СТАРТ =============
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username

    ensure_user(user_id, username=username)

    args = context.args
    if args:
        try:
            ref_id = int(args[0])
            if ref_id != user_id:
                db_exec(
                    "INSERT INTO referrals (user_id, referrer_id) VALUES (%s,%s) ON CONFLICT (user_id) DO NOTHING",
                    (user_id, ref_id),
                )
        except Exception:
            pass

    subscribed = await is_subscribed(context.bot, user_id)
    db_exec("UPDATE users SET subscribed=%s WHERE id=%s", (1 if subscribed else 0, user_id))

    if not subscribed:
        await safe_reply(
            update,
            f"🔔 Подпишись на канал:\n{CHANNEL_ID}\n\nПосле подписки нажми «✅ Я подписался»",
            reply_markup=subscribe_menu(),
        )
        return

    check_click_reset(user_id)
    context.user_data.clear()
    context.user_data["menu"] = "main"
    await safe_reply(update, "✨ Добро пожаловать!", reply_markup=main_menu(user_id))

# =========================
# ===== ПРОФИЛЬ ===========
# =========================
def get_theme_emoji(active_theme: Optional[str]) -> str:
    if not active_theme:
        return ""
    label, _price = THEME_BY_CODE.get(active_theme, ("", 0))
    # label starts with emoji
    return label.split()[0] if label else ""

def get_frame_emoji(active_vip: Optional[str]) -> str:
    if active_vip == "VIP":
        return "💎"
    if active_vip == "MVP":
        return "🏆"
    if active_vip == "PREMIUM":
        return "🔥"
    return "🔸"

async def send_profile(q, context, user_id: int):
    cleanup_expired_cosmetics(user_id)

    row = db_fetchone(
        "SELECT balance, COALESCE(total_clicks,0), username, vip_type, vip_until, active_title, active_theme, upgrade_level FROM users WHERE id=%s",
        (user_id,),
    )
    if row:
        bal, total_clicks, stored_username, vip_type, vip_until, active_title, active_theme, upgrade_level = row
    else:
        bal, total_clicks, stored_username, vip_type, vip_until, active_title, active_theme, upgrade_level = (0, 0, None, None, None, "ROOKIE", None, 0)

    upgrade_level = int(upgrade_level or 0)

    active_vip, vip_until_dt = vip_is_active(vip_type, vip_until)
    frame = get_frame_emoji(active_vip)
    theme_emoji = get_theme_emoji(active_theme)

    # ✅ короткая шапка, не криво на телефоне
    header = f"{frame}{theme_emoji} • ПРОФИЛЬ • {theme_emoji}{frame}"

    used, next_reset, limit = check_click_reset(user_id)
    reward = compute_click_reward(user_id)

    tname = title_label(active_title or "ROOKIE")
    nick = format_user_link(stored_username, user_id)

    vip_status_text = active_vip if active_vip else "нет"
    vip_left_text = fmt_td(vip_until_dt - datetime.now()) if vip_until_dt else "нет VIP статуса"

    text = (
        f"{header}\n\n"
        f"🏷 Титул: {esc(tname)}\n"
        f"👤 Ник: {nick}\n"
        f"🎖 VIP: {esc(vip_status_text)}\n"
        f"⏳ VIP срок: {esc(vip_left_text)}\n\n"
        f"💰 Баланс: {round(float(bal), 2)} GOLD\n"
        f"💸 За клик: +{reward} GOLD\n"
        f"📊 Клики (за период): {used}/{limit}\n"
        f"🏁 Клики (всего): {int(total_clicks)}\n"
        f"⚡ Уровень улучшения: {upgrade_level}/{UPGRADE_MAX_LEVEL}\n"
        f"⏳ До обновления кликов: {fmt_td(next_reset - datetime.now())}"
    )
    await q.message.reply_text(text, reply_markup=profile_inline_menu(), parse_mode="HTML", disable_web_page_preview=True)

# =========================
# ===== РЕФ БОНУСЫ =========
# =========================
def get_subscribed_ref_count(referrer_id: int) -> int:
    row = db_fetchone(
        """
        SELECT COUNT(*)
        FROM referrals r
        JOIN users u ON u.id = r.user_id
        WHERE r.referrer_id=%s AND u.subscribed=1
        """,
        (referrer_id,),
    )
    return int(row[0]) if row else 0

def can_take_daily(last_daily_bonus: Optional[str]) -> Tuple[bool, Optional[timedelta]]:
    if not last_daily_bonus:
        return True, None
    try:
        last_dt = datetime.fromisoformat(last_daily_bonus)
    except Exception:
        return True, None
    next_dt = last_dt + timedelta(hours=DAILY_BONUS_HOURS)
    now = datetime.now()
    if now >= next_dt:
        return True, None
    return False, (next_dt - now)

async def send_ref_bonus_menu(q, context, user_id: int):
    ref_count = get_subscribed_ref_count(user_id)
    row = db_fetchone("SELECT ref_bonus_10, ref_bonus_50, ref_bonus_100 FROM users WHERE id=%s", (user_id,))
    claimed10, claimed50, claimed100 = row if row else (0, 0, 0)

    text = (
        "🎯 Бонусы за рефералов\n\n"
        f"👥 Подписанные рефералы: {ref_count}\n\n"
        "Награды:\n"
        "• 10 рефов → +1000G\n"
        "• 50 рефов → +5000G\n"
        "• 100 рефов → +10000G\n\n"
        "Нажми кнопку, чтобы забрать (если выполнено)."
    )
    await q.message.reply_text(text, reply_markup=ref_bonuses_inline_menu(claimed10, claimed50, claimed100))

async def process_claim_ref_bonus(q, context, user_id: int, data: str):
    ref_count = get_subscribed_ref_count(user_id)
    if data == "claim_ref_10":
        need, reward, col = 10, 1000, "ref_bonus_10"
    elif data == "claim_ref_50":
        need, reward, col = 50, 5000, "ref_bonus_50"
    elif data == "claim_ref_100":
        need, reward, col = 100, 10000, "ref_bonus_100"
    else:
        return

    row = db_fetchone(f"SELECT {col} FROM users WHERE id=%s", (user_id,))
    already = int(row[0]) if row else 0
    if already:
        await q.message.reply_text("✅ Ты уже забрал эту награду.")
        return
    if ref_count < need:
        await q.message.reply_text(f"❌ Нужно {need} подписанных рефералов. Сейчас: {ref_count}")
        return

    db_exec(f"UPDATE users SET balance=balance+%s, {col}=1 WHERE id=%s", (reward, user_id))
    await q.message.reply_text(f"🎉 Награда получена: +{reward} GOLD ✅")
    await send_ref_bonus_menu(q, context, user_id)

# =========================
# ===== КЕЙСЫ =============
# =========================
def case_info_text() -> str:
    def fmt_case(ctype: str) -> str:
        lines = [f"{CASE_LABELS[ctype]} (цена {CASE_PRICES[ctype]}G)"]
        for t, v, p in CASE_DROPS[ctype]:
            if t == "gold":
                lines.append(f"• {v}G — {p}%")
            else:
                vip, val, unit = v
                lines.append(f"• {vip} на {val} {unit} — {p}%")
        return "\n".join(lines)
    return "📜 Что может выпасть:\n\n" + "\n\n".join([fmt_case("COMMON"), fmt_case("RARE"), fmt_case("LEGENDARY")])

async def show_cases(q, user_id: int):
    c, r, l = get_case_counts(user_id)
    opens = get_case_opens(user_id)
    text = (
        "📦 Кейсы\n\n"
        f"{CASE_LABELS['COMMON']}: x{c} (открыто {opens['COMMON']}/{CASE_LIMITS['COMMON']} за {CASE_RESET_HOURS}ч)\n"
        f"{CASE_LABELS['RARE']}: x{r} (открыто {opens['RARE']}/{CASE_LIMITS['RARE']} за {CASE_RESET_HOURS}ч)\n"
        f"{CASE_LABELS['LEGENDARY']}: x{l} (открыто {opens['LEGENDARY']}/{CASE_LIMITS['LEGENDARY']} за {CASE_RESET_HOURS}ч)\n\n"
        "Открывай кейсы или покупай в магазине 👇"
    )
    await q.message.reply_text(text, reply_markup=cases_inline_menu(c, r, l))

async def case_buy(user_id: int, ctype: str) -> Tuple[bool, str]:
    price = CASE_PRICES[ctype]
    bal = db_fetchone("SELECT balance FROM users WHERE id=%s", (user_id,))
    bal = float(bal[0]) if bal else 0.0
    if bal < price:
        return False, "❌ Недостаточно GOLD."
    col = "case_common" if ctype == "COMMON" else ("case_rare" if ctype == "RARE" else "case_legendary")
    db_exec("UPDATE users SET balance=balance-%s, " + col + "=" + col + "+1 WHERE id=%s", (price, user_id))
    return True, f"✅ Куплено: {CASE_LABELS[ctype]} (+1)."

async def case_open(q, context, user_id: int, ctype: str):
    ok, reason = case_can_open(user_id, ctype)
    if not ok:
        await q.message.reply_text(reason)
        return

    mark_case_action(user_id)

    # списываем кейс и увеличиваем счетчик открытия
    if ctype == "COMMON":
        db_exec("UPDATE users SET case_common=case_common-1, case_open_common=case_open_common+1 WHERE id=%s", (user_id,))
    elif ctype == "RARE":
        db_exec("UPDATE users SET case_rare=case_rare-1, case_open_rare=case_open_rare+1 WHERE id=%s", (user_id,))
    else:
        db_exec("UPDATE users SET case_legendary=case_legendary-1, case_open_legendary=case_open_legendary+1 WHERE id=%s", (user_id,))

    # анимация
    msg = await q.message.reply_text("📦 Открываю кейс…\n\n🔄 Кручу… ░░░░░")
    steps = [
        ("🔄 Кручу… ░░░░░", 2.0),
        ("🔄 Кручу… █░░░░", 2.0),
        ("🔄 Кручу… ██░░░", 2.0),
        ("🔄 Кручу… ███░░", 1.5),
        ("🔄 Кручу… ████░", 1.5),
    ]
    total = sum(s[1] for s in steps)
    extra = max(0.0, CASE_ANIM_SECONDS - total)
    for text, delay in steps:
        try:
            await msg.edit_text(f"📦 Открываю кейс…\n\n{text}")
        except Exception:
            pass
        await asyncio.sleep(delay)
    if extra > 0:
        await asyncio.sleep(extra)

    # дроп
    rtype, val = weighted_choice(CASE_DROPS[ctype])

    if rtype == "gold":
        amount = int(val)
        db_exec("UPDATE users SET balance=balance+%s WHERE id=%s", (amount, user_id))
        try:
            await msg.edit_text(f"🎉 Выпало: +{amount} GOLD ✅")
        except Exception:
            await q.message.reply_text(f"🎉 Выпало: +{amount} GOLD ✅")
        return

    vip_type, num, unit = val
    # unit "дн"
    dur = timedelta(days=int(num))
    applied, txt = vip_apply_reward(user_id, vip_type, dur)
    if applied:
        try:
            await msg.edit_text(f"🎉 Выпало: {vip_type} на {num} {unit} ✅")
        except Exception:
            await q.message.reply_text(f"🎉 Выпало: {vip_type} на {num} {unit} ✅")
    else:
        try:
            await msg.edit_text(f"🎉 Выпало: {vip_type} на {num} {unit}\n{txt}")
        except Exception:
            await q.message.reply_text(f"🎉 Выпало: {vip_type} на {num} {unit}\n{txt}")

# =========================
# ===== КОСМЕТИКА =========
# =========================
async def show_cosmetics(q, user_id: int):
    cleanup_expired_cosmetics(user_id)
    await q.message.reply_text("🎨 Косметика\n\nВыбери раздел 👇", reply_markup=cosmetics_menu())

async def show_titles(q, user_id: int):
    cleanup_expired_cosmetics(user_id)
    ok, left = can_change_cosmetic(user_id)
    active = get_active_title(user_id)

    rows = db_fetchall("SELECT title_code, expires_at FROM user_titles WHERE user_id=%s ORDER BY title_code ASC", (user_id,))
    if not rows:
        rows = [("ROOKIE", None)]

    text = "🏷 Титулы\n\n"
    if not ok:
        text += f"⏳ Кулдаун смены: {left} сек\n\n"

    buttons = []
    for code, exp in rows[:30]:
        name = title_label(code)
        is_active = (code == active)
        label = f"{'✅ ' if is_active else ''}{name}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"title_set_{code}")])

    buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="cosmetics")])
    await q.message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))

async def show_themes(q, user_id: int):
    cleanup_expired_cosmetics(user_id)
    ok, left = can_change_cosmetic(user_id)
    r = db_fetchone("SELECT active_theme FROM users WHERE id=%s", (user_id,))
    active = r[0] if r else None

    rows = db_fetchall("SELECT theme_code, expires_at FROM user_themes WHERE user_id=%s ORDER BY theme_code ASC", (user_id,))
    owned = {c for c, _exp in rows}

    text = "🌌 Фоны\n\n"
    if not ok:
        text += f"⏳ Кулдаун смены: {left} сек\n\n"

    # список owned
    btns = []
    if owned:
        for code in sorted(list(owned)):
            label, _p = THEME_BY_CODE.get(code, (code, 0))
            is_active = (code == active)
            btns.append([InlineKeyboardButton(f"{'✅ ' if is_active else ''}{label}", callback_data=f"theme_set_{code}")])
        btns.append([InlineKeyboardButton("❌ Снять фон", callback_data="theme_clear")])
    else:
        text += "У тебя пока нет фонов.\n"

    btns.append([InlineKeyboardButton("🛒 Магазин фонов", callback_data="theme_shop")])
    btns.append([InlineKeyboardButton("⬅️ Назад", callback_data="cosmetics")])
    await q.message.reply_text(text, reply_markup=InlineKeyboardMarkup(btns))

async def show_theme_shop(q, user_id: int):
    rows = db_fetchall("SELECT theme_code FROM user_themes WHERE user_id=%s", (user_id,))
    owned = {r[0] for r in rows}

    bal = db_fetchone("SELECT balance FROM users WHERE id=%s", (user_id,))
    bal = float(bal[0]) if bal else 0.0

    text = "🛒 Магазин фонов\n\n" + f"💰 Баланс: {int(bal)}G\n\n"
    buttons = []
    for code, label, price in THEMES:
        if code in owned:
            buttons.append([InlineKeyboardButton(f"✅ {label} — куплено", callback_data="noop")])
        else:
            buttons.append([InlineKeyboardButton(f"{label} — {price}G", callback_data=f"theme_buy_{code}")])
    buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="cos_theme")])
    await q.message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))

async def theme_buy(user_id: int, code: str) -> Tuple[bool, str]:
    if code not in THEME_BY_CODE:
        return False, "❌ Не найдено."
    label, price = THEME_BY_CODE[code]
    bal = db_fetchone("SELECT balance FROM users WHERE id=%s", (user_id,))
    bal = float(bal[0]) if bal else 0.0
    if bal < price:
        return False, "❌ Недостаточно GOLD."
    db_exec("UPDATE users SET balance=balance-%s WHERE id=%s", (price, user_id))
    db_exec(
        "INSERT INTO user_themes (user_id, theme_code, expires_at) VALUES (%s,%s,NULL) "
        "ON CONFLICT (user_id, theme_code) DO NOTHING",
        (user_id, code),
    )
    return True, f"✅ Куплено: {label}"

async def set_title(q, user_id: int, code: str):
    ok, left = can_change_cosmetic(user_id)
    if not ok:
        await q.message.reply_text(f"⏳ Подожди {left} сек перед сменой.")
        return

    # проверка владения
    r = db_fetchone("SELECT 1 FROM user_titles WHERE user_id=%s AND title_code=%s", (user_id, code))
    if not r:
        await q.message.reply_text("❌ У тебя нет этого титула.")
        return

    db_exec("UPDATE users SET active_title=%s WHERE id=%s", (code, user_id))
    mark_cosmetic_change(user_id)
    await q.message.reply_text(f"✅ Титул выбран: {title_label(code)}")

async def set_theme(q, user_id: int, code: Optional[str]):
    ok, left = can_change_cosmetic(user_id)
    if not ok:
        await q.message.reply_text(f"⏳ Подожди {left} сек перед сменой.")
        return

    if code is None:
        db_exec("UPDATE users SET active_theme=NULL WHERE id=%s", (user_id,))
        mark_cosmetic_change(user_id)
        await q.message.reply_text("✅ Фон снят.")
        return

    r = db_fetchone("SELECT 1 FROM user_themes WHERE user_id=%s AND theme_code=%s", (user_id, code))
    if not r:
        await q.message.reply_text("❌ У тебя нет этого фона.")
        return

    db_exec("UPDATE users SET active_theme=%s WHERE id=%s", (code, user_id))
    mark_cosmetic_change(user_id)
    label, _p = THEME_BY_CODE.get(code, (code, 0))
    await q.message.reply_text(f"✅ Фон выбран: {label}")

# =========================
# ===== УЛУЧШЕНИЯ =========
# =========================
def upgrade_status_text(user_id: int) -> str:
    row = db_fetchone("SELECT upgrade_level, vip_type, vip_until FROM users WHERE id=%s", (user_id,))
    lvl, vip_type, vip_until = row if row else (0, None, None)
    lvl = int(lvl or 0)
    active_vip, _ = vip_is_active(vip_type, vip_until)
    base = VIP_LIMITS.get(active_vip, BASE_CLICKS_LIMIT) if active_vip else BASE_CLICKS_LIMIT
    limit = compute_current_limit(user_id)
    reward = compute_click_reward(user_id)
    text = (
        "⚡ Улучшения\n\n"
        f"Уровень: {lvl}/{UPGRADE_MAX_LEVEL}\n"
        f"Бонус к лимиту: +{UPGRADE_BONUS.get(lvl,0)}\n"
        f"База лимита: {base} (VIP {'есть' if active_vip else 'нет'})\n"
        f"Итог лимита: {limit}\n"
        f"За клик: +{reward} GOLD\n"
    )
    if lvl < UPGRADE_MAX_LEVEL:
        cost = UPGRADE_COST.get(lvl, None)
        nb = UPGRADE_BONUS.get(lvl+1, 0)
        nreward = click_reward_for_level(lvl+1)
        text += (
            f"\nСледующий уровень: {lvl+1}\n"
            f"Цена: {cost}G\n"
            f"Новый бонус к лимиту: +{nb}\n"
            f"Новая награда за клик: +{nreward}G\n"
        )
    else:
        text += "\n✅ Максимальный уровень."
    return text

def upgrade_info_text() -> str:
    lines = ["📜 Инфо уровней\n"]
    for lvl in range(0, UPGRADE_MAX_LEVEL + 1):
        bonus = UPGRADE_BONUS.get(lvl, 0)
        reward = click_reward_for_level(lvl)
        lines.append(f"• Уровень {lvl}: +{bonus} к лимиту | +{reward}G за клик")
    lines.append("\nЦены:")
    for lvl in range(0, UPGRADE_MAX_LEVEL):
        lines.append(f"• {lvl}→{lvl+1}: {UPGRADE_COST[lvl]}G")
    return "\n".join(lines)

async def upgrade_buy(q, user_id: int):
    row = db_fetchone("SELECT upgrade_level, balance FROM users WHERE id=%s", (user_id,))
    lvl, bal = row if row else (0, 0)
    lvl = int(lvl or 0)
    bal = float(bal or 0)
    if lvl >= UPGRADE_MAX_LEVEL:
        await q.message.reply_text("✅ У тебя уже максимальный уровень.")
        return
    cost = UPGRADE_COST.get(lvl, None)
    if cost is None:
        await q.message.reply_text("❌ Ошибка цены уровня.")
        return
    if bal < cost:
        await q.message.reply_text("❌ Недостаточно GOLD.")
        return
    db_exec("UPDATE users SET balance=balance-%s, upgrade_level=upgrade_level+1 WHERE id=%s", (cost, user_id))
    await q.message.reply_text(f"✅ Улучшение куплено! Уровень теперь: {lvl+1}")

# =========================
# ===== КАЗИНО ============
# =========================
def casino_can_play(user_id: int) -> Tuple[bool, str]:
    r = db_fetchone("SELECT last_casino_action FROM users WHERE id=%s", (user_id,))
    if r and r[0]:
        try:
            last = datetime.fromisoformat(r[0])
            left = (last + timedelta(seconds=CASINO_COOLDOWN_SEC)) - datetime.now()
            if left.total_seconds() > 0:
                return False, f"⏳ Подожди {int(left.total_seconds())} сек."
        except Exception:
            pass
    return True, ""

def mark_casino_action(user_id: int):
    db_exec("UPDATE users SET last_casino_action=%s WHERE id=%s", (now_iso(), user_id))

async def casino_start_game(q, context, user_id: int, game: str):
    ok, reason = casino_can_play(user_id)
    if not ok:
        await q.message.reply_text(reason)
        return
    context.user_data["casino"] = {"step": "bet", "game": game}
    await q.message.reply_text("🎲 Казино\n\nВведите сумму ставки (100–500000):", reply_markup=cancel_menu())

async def casino_set_bet(update: Update, context, user_id: int, text: str):
    st = context.user_data.get("casino", {})
    if st.get("step") != "bet":
        return False
    try:
        bet = int(text.strip())
    except Exception:
        await safe_reply(update, "❌ Введите число.", reply_markup=cancel_menu())
        return True
    if bet < CASINO_MIN_BET or bet > CASINO_MAX_BET:
        await safe_reply(update, "❌ Ставка должна быть от 100 до 500000.", reply_markup=cancel_menu())
        return True
    row = db_fetchone("SELECT balance FROM users WHERE id=%s", (user_id,))
    bal = float(row[0]) if row else 0.0
    if bal < bet:
        await safe_reply(update, "❌ Недостаточно GOLD.", reply_markup=cancel_menu())
        return True

    st["bet"] = bet
    st["step"] = "pick"
    context.user_data["casino"] = st

    game = st["game"]
    text_game = "Больше/Меньше" if game == "bm" else ("Чёт/Нечёт" if game == "pn" else "Угадай число (1–6)")
    await safe_reply(
        update,
        f"✅ Ставка принята: {bet} GOLD\n🎮 Игра: {text_game}\n\nВыберите вариант:",
        reply_markup=None,
    )
    # отправляем выбор инлайном отдельным сообщением
    await update.message.reply_text("👇 Выбор:", reply_markup=casino_choice_menu(game))
    return True

async def casino_resolve(q, context, user_id: int, pick: str):
    ok, reason = casino_can_play(user_id)
    if not ok:
        await q.message.reply_text(reason)
        return
    st = context.user_data.get("casino", {})
    if st.get("step") != "pick":
        await q.message.reply_text("❌ Сначала выбери игру и введи ставку.")
        return
    bet = int(st.get("bet", 0))
    game = st.get("game")

    # защита: только 1 активная ставка
    if st.get("resolving"):
        await q.message.reply_text("⏳ Подожди, ставка уже крутится…")
        return
    st["resolving"] = True
    context.user_data["casino"] = st

    # проверим баланс ещё раз
    row = db_fetchone("SELECT balance FROM users WHERE id=%s", (user_id,))
    bal = float(row[0]) if row else 0.0
    if bal < bet:
        st["resolving"] = False
        context.user_data["casino"] = st
        await q.message.reply_text("❌ Недостаточно GOLD.")
        return

    # списываем ставку
    db_exec("UPDATE users SET balance=balance-%s WHERE id=%s", (bet, user_id))

    # кидаем куб
    mark_casino_action(user_id)
    await q.message.reply_text("🎲 Ставка принята! Крутим…")
    dice_msg = await context.bot.send_dice(chat_id=user_id, emoji="🎲")
    value = getattr(dice_msg.dice, "value", None) or 1

    # даём анимации “пожить”
    await asyncio.sleep(5.5)

    win = False
    result_text = ""
    if game == "bm":
        # 1-3 меньше, 4-6 больше
        outcome = "bigger" if value >= 4 else "smaller"
        win = (pick == outcome)
        result_text = f"🎲 Выпало: {value} → {'БОЛЬШЕ' if outcome=='bigger' else 'МЕНЬШЕ'}"
    elif game == "pn":
        outcome = "even" if (value % 2 == 0) else "odd"
        win = (pick == outcome)
        result_text = f"🎲 Выпало: {value} → {'ЧЁТ' if outcome=='even' else 'НЕЧЁТ'}"
    else:
        # num_1..num_6
        try:
            chosen = int(pick.split("_")[-1])
        except Exception:
            chosen = 1
        win = (value == chosen)
        result_text = f"🎲 Выпало: {value} → {'УГАДАЛ' if win else 'НЕ УГАДАЛ'} (ты выбрал {chosen})"

    coef = CASINO_COEFF[game]
    if win:
        payout = int(bet * coef)
        db_exec("UPDATE users SET balance=balance+%s WHERE id=%s", (payout, user_id))
        await q.message.reply_text(
            f"✅ ВЫ ВЫИГРАЛИ!\n"
            f"💰 Ставка: {bet} GOLD\n"
            f"📈 Коэф: {coef}\n"
            f"{result_text}\n"
            f"🎉 Начислено: {payout} GOLD"
        )
    else:
        await q.message.reply_text(
            f"❌ ВЫ ПРОИГРАЛИ\n"
            f"💰 Ставка: {bet} GOLD\n"
            f"{result_text}\n"
            f"↩️ Попробуй ещё раз"
        )

    # сбрасываем состояние
    context.user_data.pop("casino", None)

# =========================
# ===== ТОПЫ / INLINE =====
# =========================
async def inline_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    try:
        await q.answer()
    except Exception:
        pass

    user_id = q.from_user.id
    username = q.from_user.username
    ensure_user(user_id, username=username)

    data = q.data or ""

    if data == "back_profile":
        await send_profile(q, context, user_id)
        return

    if data == "noop":
        return

    # проф меню
    if data == "tops":
        await q.message.reply_text("🏆 Выберите ТОП:", reply_markup=tops_inline_menu())
        return

    if data == "daily_bonus":
        row = db_fetchone("SELECT last_daily_bonus FROM users WHERE id=%s", (user_id,))
        last_daily = row[0] if row else None
        ok, left = can_take_daily(last_daily)
        if not ok and left is not None:
            await q.message.reply_text(f"⏳ Ежедневный бонус уже был.\nСледующий через: {fmt_td(left)}")
            return
        db_exec("UPDATE users SET balance=balance+%s, last_daily_bonus=%s WHERE id=%s", (DAILY_BONUS_AMOUNT, now_iso(), user_id))
        await q.message.reply_text(f"✅ Ежедневный бонус получен: +{DAILY_BONUS_AMOUNT} GOLD 🎁")
        return

    if data == "top_clicks":
        rows = db_fetchall("SELECT id, username, COALESCE(total_clicks,0) AS tc FROM users ORDER BY tc DESC, id ASC LIMIT 10")
        msg = "📊 ТОП по кликам (всего)\n\n"
        if not rows:
            msg += "Пока пусто."
        else:
            for i, (uid, uname, tc) in enumerate(rows, start=1):
                msg += f"{i}) {display_in_top(uid, uname)} — {int(tc)} кликов\n"
        await q.message.reply_text(msg, reply_markup=tops_inline_menu(), parse_mode="HTML", disable_web_page_preview=True)
        return

    if data == "top_balance":
        rows = db_fetchall("SELECT id, username, balance FROM users ORDER BY balance DESC, id ASC LIMIT 10")
        msg = "💰 ТОП по балансу\n\n"
        if not rows:
            msg += "Пока пусто."
        else:
            for i, (uid, uname, bal) in enumerate(rows, start=1):
                msg += f"{i}) {display_in_top(uid, uname)} — {round(float(bal), 2)} GOLD\n"
        await q.message.reply_text(msg, reply_markup=tops_inline_menu(), parse_mode="HTML", disable_web_page_preview=True)
        return

    if data == "top_refs":
        rows = db_fetchall(
            """
            SELECT r.referrer_id, u.username, COUNT(*) AS c
            FROM referrals r
            JOIN users uref ON uref.id = r.user_id
            LEFT JOIN users u ON u.id = r.referrer_id
            WHERE uref.subscribed=1
            GROUP BY r.referrer_id, u.username
            ORDER BY c DESC, r.referrer_id ASC
            LIMIT 10
            """
        )
        msg = "👥 ТОП рефоводов (подписанные рефы)\n\n"
        if not rows:
            msg += "Пока пусто."
        else:
            for i, (ref_uid, ref_uname, c) in enumerate(rows, start=1):
                msg += f"{i}) {display_in_top(ref_uid, ref_uname)} — {int(c)} рефералов\n"
        await q.message.reply_text(msg, reply_markup=tops_inline_menu(), parse_mode="HTML", disable_web_page_preview=True)
        return

    if data == "ref_bonuses":
        await send_ref_bonus_menu(q, context, user_id)
        return

    if data.startswith("claim_ref_"):
        await process_claim_ref_bonus(q, context, user_id, data)
        return

    # кейсы
    if data == "cases":
        await show_cases(q, user_id)
        return
    if data == "case_shop":
        await q.message.reply_text("🛒 Магазин кейсов", reply_markup=cases_shop_menu())
        return
    if data == "case_info":
        await q.message.reply_text(case_info_text())
        return
    if data.startswith("case_buy_"):
        ctype = data.split("_")[-1]
        ok, txt = await case_buy(user_id, ctype)
        await q.message.reply_text(txt)
        return
    if data.startswith("case_open_"):
        ctype = data.split("_")[-1]
        await case_open(q, context, user_id, ctype)
        return

    # косметика
    if data == "cosmetics":
        await show_cosmetics(q, user_id)
        return
    if data == "cos_title":
        await show_titles(q, user_id)
        return
    if data.startswith("title_set_"):
        code = data.replace("title_set_", "", 1)
        await set_title(q, user_id, code)
        return

    if data == "cos_theme":
        await show_themes(q, user_id)
        return
    if data == "theme_shop":
        await show_theme_shop(q, user_id)
        return
    if data.startswith("theme_buy_"):
        code = data.replace("theme_buy_", "", 1)
        ok, txt = await theme_buy(user_id, code)
        await q.message.reply_text(txt)
        return
    if data.startswith("theme_set_"):
        code = data.replace("theme_set_", "", 1)
        await set_theme(q, user_id, code)
        return
    if data == "theme_clear":
        await set_theme(q, user_id, None)
        return
    if data == "cos_frame_info":
        await q.message.reply_text("🧱 Рамка\n\nРамка зависит от VIP:\nVIP → 💎\nMVP → 🏆\nPREMIUM → 🔥\nБез VIP → 🔸\n\nVIP рамка всегда главнее ✅")
        return

    # улучшения
    if data == "upgrades":
        await q.message.reply_text(upgrade_status_text(user_id), reply_markup=upgrades_menu())
        return
    if data == "upgrade_info":
        await q.message.reply_text(upgrade_info_text())
        return
    if data == "upgrade_buy":
        await upgrade_buy(q, user_id)
        return

    # казино
    if data == "casino":
        await q.message.reply_text("🎲 Казино — выбери игру:", reply_markup=casino_menu())
        return
    if data.startswith("casino_game_"):
        game = data.split("_")[-1]
        await casino_start_game(q, context, user_id, game)
        return
    if data.startswith("casino_pick_"):
        st = context.user_data.get("casino", {})
        game = st.get("game")
        if not game:
            await q.message.reply_text("❌ Сначала выбери игру.")
            return
        pick = data.replace("casino_pick_", "", 1)
        await casino_resolve(q, context, user_id, pick)
        return

# =========================
# ===== WITHDRAW done/cancel
# =========================
async def admin_process_withdraw_decision(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    lower = text.strip().lower()
    if not (lower.startswith("done ") or lower.startswith("cancel ")):
        return False

    parts = text.strip().split(maxsplit=2)
    if len(parts) < 2:
        await safe_reply(update, "❌ Формат:\n done 3 текст\n cancel 3 причина", reply_markup=admin_menu())
        return True

    cmd = parts[0].lower()
    try:
        wid = int(parts[1].lstrip("#"))
    except Exception:
        await safe_reply(update, "❌ ID заявки должен быть числом. Пример: done 3", reply_markup=admin_menu())
        return True

    admin_note = parts[2] if len(parts) >= 3 else ""
    row = db_fetchone("SELECT user_id, amount, requisites, status FROM withdrawals WHERE id=%s", (wid,))
    if not row:
        await safe_reply(update, "❌ Заявка не найдена.", reply_markup=admin_menu())
        return True

    target_uid, amount, requisites, status = row
    if status != "pending":
        await safe_reply(update, "❌ Эта заявка уже обработана.", reply_markup=admin_menu())
        return True

    decided_at = now_iso()

    if cmd == "done":
        db_exec("UPDATE withdrawals SET status='approved', admin_note=%s, decided_at=%s WHERE id=%s", (admin_note, decided_at, wid))
        try:
            msg_user = f"✅ Ваша заявка на вывод подтверждена\n💰 Сумма: {amount} GOLD\n🕒 Ожидайте зачисление\n"
            if admin_note.strip():
                msg_user += f"\n💬 Сообщение: {admin_note.strip()}"
            await context.bot.send_message(chat_id=target_uid, text=msg_user)
        except Exception:
            pass
        await safe_reply(update, f"✅ Готово. Заявка #{wid} подтверждена.", reply_markup=admin_menu())
        return True

    if cmd == "cancel":
        db_exec("UPDATE users SET balance=balance+%s WHERE id=%s", (amount, target_uid))
        db_exec("UPDATE withdrawals SET status='declined', admin_note=%s, decided_at=%s WHERE id=%s", (admin_note, decided_at, wid))
        try:
            msg_user = f"❌ Ваша заявка на вывод отклонена\n💰 Сумма: {amount} GOLD\n↩️ Средства возвращены на баланс.\n"
            if admin_note.strip():
                msg_user += f"\n💬 Причина: {admin_note.strip()}"
            await context.bot.send_message(chat_id=target_uid, text=msg_user)
        except Exception:
            pass
        await safe_reply(update, f"✅ Отклонено. Заявка #{wid} закрыта.", reply_markup=admin_menu())
        return True

    return False

# =========================
# ===== ОБРАБОТКА TEXT =====
# =========================
async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    text = update.message.text
    user_id = update.effective_user.id
    username = update.effective_user.username

    ensure_user(user_id, username=username)

    # бан (кроме админа)
    if not is_admin(user_id):
        r = db_fetchone("SELECT banned FROM users WHERE id=%s", (user_id,))
        if r and int(r[0]) == 1:
            await safe_reply(update, "⛔ Вы заблокированы.")
            return

    # НАЗАД / ОТМЕНА
    if text in ["🔙 Назад", "❌ Отмена"]:
        # сброс состояний
        context.user_data.pop("admin_action", None)
        context.user_data.pop("menu", None)
        context.user_data.pop("earning", None)
        context.user_data.pop("withdraw_step", None)
        context.user_data.pop("withdraw_amount", None)
        context.user_data.pop("casino", None)
        await safe_reply(update, "Главное меню", reply_markup=main_menu(user_id))
        return

    # ПОДПИСКА
    if text == "✅ Я подписался":
        subscribed = await is_subscribed(context.bot, user_id)
        db_exec("UPDATE users SET subscribed=%s WHERE id=%s", (1 if subscribed else 0, user_id))
        if subscribed:
            await safe_reply(update, "✅ Подписка подтверждена!", reply_markup=main_menu(user_id))
        else:
            await safe_reply(update, "❌ Ты ещё не подписался!", reply_markup=subscribe_menu())
        return

    # ПРОФИЛЬ
    if text == "👤 Профиль":
        await safe_reply(update, "Открываю профиль 👇", reply_markup=main_menu(user_id))
        fake_q = type("Q", (), {})()
        fake_q.message = update.message
        fake_q.from_user = update.effective_user
        await send_profile(fake_q, context, user_id)
        return

    # ЗАРАБОТАТЬ
    if text == "💰 Заработать":
        used, _, limit = check_click_reset(user_id)
        if used >= limit:
            await safe_reply(update, "❌ У вас закончились клики", reply_markup=main_menu(user_id))
            return
        context.user_data["earning"] = True
        await safe_reply(update, "👆 Нажимай «КЛИК»", reply_markup=earn_menu())
        return

    # КЛИК
    if text == "👆 КЛИК" and context.user_data.get("earning"):
        used, _, limit = check_click_reset(user_id)
        if used >= limit:
            await safe_reply(update, "❌ У вас закончились клики", reply_markup=main_menu(user_id))
            return

        reward = compute_click_reward(user_id)
        db_exec(
            """
            UPDATE users
            SET balance=balance+%s,
                clicks_used=clicks_used+1,
                total_clicks=COALESCE(total_clicks,0)+1
            WHERE id=%s
            """,
            (reward, user_id),
        )

        # авто-титул за клики
        row = db_fetchone("SELECT COALESCE(total_clicks,0) FROM users WHERE id=%s", (user_id,))
        total_clicks = int(row[0] or 0) if row else 0
        auto = best_auto_title(total_clicks)
        grant_title(user_id, auto, None)  # навсегда

        used += 1
        await safe_reply(update, f"✅ Заработано {reward} GOLD ({used}/{limit})", reply_markup=earn_menu())
        return

    # РЕФЕРАЛКА
    if text == "👥 Рефералка":
        refs = db_fetchall("SELECT user_id, rewarded FROM referrals WHERE referrer_id=%s", (user_id,))
        total = len(refs)
        earned = 0

        for ref_id, rewarded in refs:
            row = db_fetchone("SELECT subscribed FROM users WHERE id=%s", (ref_id,))
            sub = int(row[0]) if row else 0
            if sub and int(rewarded) == 0:
                db_exec("UPDATE users SET balance=balance+%s WHERE id=%s", (REF_REWARD, user_id))
                db_exec("UPDATE referrals SET rewarded=1 WHERE user_id=%s", (ref_id,))
                earned += REF_REWARD

        link = f"https://t.me/topclickerkisspromobot?start={user_id}"
        await safe_reply(
            update,
            f"👥 Ваша ссылка:\n{link}\n"
            f"💰 За подписанного: {REF_REWARD} GOLD\n"
            f"👥 Всего: {total}\n"
            f"💵 Получено сейчас: {earned} GOLD\n"
            f"✅ Подписанные рефы: {get_subscribed_ref_count(user_id)}",
            reply_markup=main_menu(user_id),
        )
        return

    # ПРОМО
    if text == "🎁 Ввести промокод":
        context.user_data["menu"] = "promo"
        await safe_reply(update, "Введите промокод:", reply_markup=cancel_menu())
        return

    if context.user_data.get("menu") == "promo":
        res = db_fetchone("SELECT amount, uses_left FROM promocodes WHERE code=%s", (text,))
        if not res:
            await safe_reply(update, "❌ Неверный промокод", reply_markup=main_menu(user_id))
        else:
            amount, uses_left = res
            used_row = db_fetchone("SELECT 1 FROM used_promocodes WHERE user_id=%s AND code=%s", (user_id, text))
            if used_row:
                await safe_reply(update, "❌ Уже использован", reply_markup=main_menu(user_id))
            elif int(uses_left) <= 0:
                await safe_reply(update, "❌ Промокод недействителен", reply_markup=main_menu(user_id))
            else:
                db_exec("UPDATE users SET balance=balance+%s WHERE id=%s", (amount, user_id))
                db_exec("UPDATE promocodes SET uses_left=uses_left-1 WHERE code=%s", (text,))
                db_exec(
                    "INSERT INTO used_promocodes (user_id, code) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                    (user_id, text),
                )
                await safe_reply(update, f"🎉 ПРОМО АКТИВИРОВАН\n💰 +{amount} GOLD", reply_markup=main_menu(user_id))
        context.user_data.pop("menu", None)
        return

    # ВЫВОД
    if text == "💸 Вывод":
        row = db_fetchone("SELECT balance FROM users WHERE id=%s", (user_id,))
        bal = float(row[0]) if row else 0
        if bal < MIN_WITHDRAW:
            await safe_reply(update, f"❌ Минимум {MIN_WITHDRAW} GOLD", reply_markup=main_menu(user_id))
            return

        context.user_data["withdraw_step"] = "amount"
        await safe_reply(
            update,
            "Введите сумму:\n\n"
            "📌 Примечание для вывода:\n"
            "• Указывайте только целую сумму от 1000\n"
            "• Примеры: 1000 / 2000 / 3000 / 4000\n"
            "❌ Не нужно: 1100, 1500, 1780 и т.д.",
            reply_markup=cancel_menu(),
        )
        return

    if context.user_data.get("withdraw_step") == "amount":
        try:
            amount = float(text)
            row = db_fetchone("SELECT balance FROM users WHERE id=%s", (user_id,))
            bal = float(row[0]) if row else 0
            if amount < MIN_WITHDRAW or amount > bal:
                await safe_reply(update, "❌ Неверная сумма", reply_markup=cancel_menu())
                return
            context.user_data["withdraw_amount"] = amount
            context.user_data["withdraw_step"] = "requisites"
            await safe_reply(update, "Введите свои реквизиты:\nTelegram Username / ID", reply_markup=cancel_menu())
        except Exception:
            await safe_reply(update, "❌ Введите число", reply_markup=cancel_menu())
        return

    if context.user_data.get("withdraw_step") == "requisites":
        amount = float(context.user_data.get("withdraw_amount", 0))
        requisites = text.strip()

        db_exec("INSERT INTO withdrawals (user_id, amount, requisites, status) VALUES (%s,%s,%s,'pending')", (user_id, amount, requisites))
        db_exec("UPDATE users SET balance=balance-%s WHERE id=%s", (amount, user_id))

        await safe_reply(
            update,
            "✅ Заявка отправлена!\n"
            f"💰 {amount} GOLD\n"
            f"✍️ {requisites}\n"
            f"🕒 {now_human()}\n\n"
            "⏳ Регламент вывода: в течение 24 часов. Ожидайте ✅",
            reply_markup=main_menu(user_id),
        )
        context.user_data.pop("withdraw_step", None)
        context.user_data.pop("withdraw_amount", None)
        return

    # =======================
    # ======= КАЗИНО BET =====
    # =======================
    if await casino_set_bet(update, context, user_id, text):
        return

    # =======================
    # ======= АДМИНКА =======
    # =======================
    menu = context.user_data.get("menu")
    admin_action = context.user_data.get("admin_action")

    if text == "🛠 Админка":
        if not is_admin(user_id):
            await safe_reply(update, "❌ Нет доступа", reply_markup=main_menu(user_id))
            return
        context.user_data["menu"] = "admin"
        context.user_data.pop("admin_action", None)
        await safe_reply(
            update,
            "🛠 Админ панель\n\n"
            "Команды для вывода:\n"
            "✅ done 3 текст\n"
            "❌ cancel 3 причина",
            reply_markup=admin_menu(),
        )
        return

    if is_admin(user_id):
        handled = await admin_process_withdraw_decision(update, context, text)
        if handled:
            return

    if is_admin(user_id) and menu == "admin" and admin_action is None:
        if text == "Создать промокод":
            context.user_data["admin_action"] = "create_promocode"
            await safe_reply(update, "Код Сумма Кол-во\nПример: KISS 10 5", reply_markup=cancel_menu())
            return

        if text == "Выдать баланс":
            context.user_data["admin_action"] = "give_balance"
            await safe_reply(update, "ID Сумма\nПример: 123456789 100", reply_markup=cancel_menu())
            return

        if text == "Забрать баланс":
            context.user_data["admin_action"] = "take_balance"
            await safe_reply(update, "ID Сумма\nПример: 123456789 50", reply_markup=cancel_menu())
            return

        if text == "Бан/Разбан":
            context.user_data["admin_action"] = "ban_user"
            await safe_reply(update, "ID пользователя\nПример: 123456789", reply_markup=cancel_menu())
            return

        if text == "🎖 Выдать привилегию":
            context.user_data["admin_action"] = "give_vip"
            await safe_reply(update, "Формат:\nID VIP 1 час\nID MVP 300 минут\nID PREMIUM 2 дня\nили: ID VIP 12ч / 300м / 2д / Infinity", reply_markup=cancel_menu())
            return

        if text == "🏷 Выдать титул":
            context.user_data["admin_action"] = "give_title"
            await safe_reply(update, "Формат:\nID TITLE_CODE 7д\nID TITLE_CODE 12ч\nID TITLE_CODE 300м\nID TITLE_CODE Infinity\nПример: 123 LEGEND Infinity", reply_markup=cancel_menu())
            return

        if text == "🌌 Выдать фон":
            context.user_data["admin_action"] = "give_theme"
            await safe_reply(update, "Формат:\nID THEME_CODE 7д/12ч/300м/Infinity\nПример: 123 ICE 7д", reply_markup=cancel_menu())
            return

        if text == "Рассылка":
            context.user_data["admin_action"] = "broadcast"
            await safe_reply(update, "Текст рассылки:", reply_markup=cancel_menu())
            return

        if text == "📋 Заявки на вывод":
            rows = db_fetchall(
                """
                SELECT id, user_id, amount, requisites
                FROM withdrawals
                WHERE status='pending'
                ORDER BY id DESC
                """
            )
            if not rows:
                await safe_reply(update, "Нет заявок ✅", reply_markup=admin_menu())
                return
            msg = "📋 Заявки (pending):\n\n"
            for wid, uid, amount, req in rows[:50]:
                msg += f"#{wid} | {uid} | {amount} GOLD\n✍️ {req}\n\n"
            msg += "Команды:\n✅ done 3 текст\n❌ cancel 3 причина"
            await safe_reply(update, msg, reply_markup=admin_menu())
            return

        if text == "Все промокоды":
            rows = db_fetchall("SELECT code, amount, uses_left FROM promocodes")
            if not rows:
                await safe_reply(update, "Промокодов пока нет", reply_markup=admin_menu())
            else:
                msg = "🎁 Все промокоды:\n\n"
                for code, amount, uses_left in rows:
                    msg += f"🔑 {code} — 💰 {amount} GOLD — 🕹️ {uses_left} активаций\n"
                await safe_reply(update, msg, reply_markup=admin_menu())
            return

    if is_admin(user_id) and admin_action:
        parts = text.split()
        try:
            if admin_action == "create_promocode":
                if len(parts) != 3:
                    await safe_reply(update, "❌ Формат: КОД СУММА КОЛ-ВО", reply_markup=cancel_menu())
                    return
                code, amount, uses = parts[0], float(parts[1]), int(parts[2])
                db_exec(
                    "INSERT INTO promocodes (code, amount, uses_left) VALUES (%s,%s,%s) "
                    "ON CONFLICT (code) DO UPDATE SET amount=EXCLUDED.amount, uses_left=EXCLUDED.uses_left",
                    (code, amount, uses),
                )
                await safe_reply(update, f"✅ Промокод создан: {code} | {amount} | {uses}", reply_markup=admin_menu())

            elif admin_action == "give_balance":
                if len(parts) != 2:
                    await safe_reply(update, "❌ Формат: ID СУММА", reply_markup=cancel_menu())
                    return
                uid, amount = int(parts[0]), float(parts[1])
                ensure_user(uid)
                db_exec("UPDATE users SET balance=balance+%s WHERE id=%s", (amount, uid))
                await safe_reply(update, f"✅ Выдано {amount} GOLD пользователю {uid}", reply_markup=admin_menu())

            elif admin_action == "take_balance":
                if len(parts) != 2:
                    await safe_reply(update, "❌ Формат: ID СУММА", reply_markup=cancel_menu())
                    return
                uid, amount = int(parts[0]), float(parts[1])
                ensure_user(uid)
                db_exec("UPDATE users SET balance=balance-%s WHERE id=%s", (amount, uid))
                await safe_reply(update, f"✅ Снято {amount} GOLD у пользователя {uid}", reply_markup=admin_menu())

            elif admin_action == "ban_user":
                if len(parts) != 1:
                    await safe_reply(update, "❌ Формат: ID", reply_markup=cancel_menu())
                    return
                uid = int(parts[0])
                ensure_user(uid)
                row = db_fetchone("SELECT banned FROM users WHERE id=%s", (uid,))
                banned = int(row[0]) if row else 0
                new_status = 0 if banned else 1
                db_exec("UPDATE users SET banned=%s WHERE id=%s", (new_status, uid))
                await safe_reply(update, f"✅ Пользователь {uid} {'разбанен' if banned else 'забанен'}", reply_markup=admin_menu())

            elif admin_action == "give_vip":
                # формат: ID VIP 1 час / ID VIP 12ч / Infinity
                if len(parts) < 3:
                    await safe_reply(update, "❌ Формат: ID VIP 1 час / 300 минут / 2 дня / 12ч / 300м / 2д / Infinity", reply_markup=cancel_menu())
                    return
                uid = int(parts[0])
                vip = parts[1].upper()
                if vip not in VIP_LIMITS:
                    await safe_reply(update, "❌ Привилегия только: VIP / MVP / PREMIUM", reply_markup=cancel_menu())
                    return

                # короткий формат
                if len(parts) == 3:
                    dur = parse_duration_short(parts[2])
                    if parts[2].strip().lower() == "infinity":
                        # навсегда
                        db_exec("UPDATE users SET vip_type=%s, vip_until=%s WHERE id=%s", (vip, "9999-12-31T23:59:59", uid))
                        await safe_reply(update, f"✅ VIP выдан {uid}: {vip} (Infinity)", reply_markup=admin_menu())
                        return
                    if not dur:
                        await safe_reply(update, "❌ Время: 300м / 12ч / 2д / Infinity", reply_markup=cancel_menu())
                        return
                else:
                    value = parts[2]
                    unit = parts[3]
                    dur = parse_duration(value, unit)
                    if not dur:
                        await safe_reply(update, "❌ Время: минут/час/дня (пример: 300 минут / 1 час / 2 дня)", reply_markup=cancel_menu())
                        return

                ensure_user(uid)
                # продление если есть такой же, иначе установка
                applied, txt = vip_apply_reward(uid, vip, dur)
                if applied:
                    await safe_reply(update, f"✅ VIP выдан {uid}: {vip}", reply_markup=admin_menu())
                else:
                    await safe_reply(update, f"⚠️ Не применено: {txt}", reply_markup=admin_menu())

            elif admin_action == "give_title":
                # ID TITLE_CODE duration/Infinity
                if len(parts) != 3:
                    await safe_reply(update, "❌ Формат: ID TITLE_CODE 7д/12ч/300м/Infinity", reply_markup=cancel_menu())
                    return
                uid = int(parts[0])
                code = parts[1].upper()
                dur_s = parts[2]
                if code not in TITLE_LABELS:
                    await safe_reply(update, "❌ Неизвестный TITLE_CODE", reply_markup=cancel_menu())
                    return
                ensure_user(uid)
                if dur_s.lower() == "infinity":
                    grant_title(uid, code, None)
                    await safe_reply(update, f"✅ Титул выдан: {uid} → {code} (Infinity)", reply_markup=admin_menu())
                else:
                    dur = parse_duration_short(dur_s)
                    if not dur:
                        await safe_reply(update, "❌ Время: 300м / 12ч / 7д / Infinity", reply_markup=cancel_menu())
                        return
                    exp = (datetime.now() + dur).isoformat(timespec="seconds")
                    grant_title(uid, code, exp)
                    await safe_reply(update, f"✅ Титул выдан: {uid} → {code} ({dur_s})", reply_markup=admin_menu())

            elif admin_action == "give_theme":
                if len(parts) != 3:
                    await safe_reply(update, "❌ Формат: ID THEME_CODE 7д/12ч/300м/Infinity", reply_markup=cancel_menu())
                    return
                uid = int(parts[0])
                code = parts[1].upper()
                dur_s = parts[2]
                if code not in THEME_BY_CODE:
                    await safe_reply(update, "❌ Неизвестный THEME_CODE", reply_markup=cancel_menu())
                    return
                ensure_user(uid)
                if dur_s.lower() == "infinity":
                    db_exec(
                        "INSERT INTO user_themes (user_id, theme_code, expires_at) VALUES (%s,%s,NULL) "
                        "ON CONFLICT (user_id, theme_code) DO NOTHING",
                        (uid, code),
                    )
                    await safe_reply(update, f"✅ Фон выдан: {uid} → {code} (Infinity)", reply_markup=admin_menu())
                else:
                    dur = parse_duration_short(dur_s)
                    if not dur:
                        await safe_reply(update, "❌ Время: 300м / 12ч / 7д / Infinity", reply_markup=cancel_menu())
                        return
                    exp = (datetime.now() + dur).isoformat(timespec="seconds")
                    db_exec(
                        "INSERT INTO user_themes (user_id, theme_code, expires_at) VALUES (%s,%s,%s) "
                        "ON CONFLICT (user_id, theme_code) DO UPDATE SET expires_at=EXCLUDED.expires_at",
                        (uid, code, exp),
                    )
                    await safe_reply(update, f"✅ Фон выдан: {uid} → {code} ({dur_s})", reply_markup=admin_menu())

            elif admin_action == "broadcast":
                msg = text
                users = db_fetchall("SELECT id FROM users")
                sent = 0
                for (uid,) in users:
                    try:
                        await context.bot.send_message(chat_id=uid, text=msg)
                        sent += 1
                    except Exception:
                        pass
                await safe_reply(update, f"✅ Рассылка завершена. Отправлено: {sent}", reply_markup=admin_menu())

        except Exception as e:
            await safe_reply(update, f"❌ Ошибка: {e}", reply_markup=admin_menu())
        finally:
            context.user_data.pop("admin_action", None)
            context.user_data["menu"] = "admin"
        return

    await safe_reply(update, "Выберите пункт меню 👇", reply_markup=main_menu(user_id))

# =========================
# ===== ERROR HANDLER =====
# =========================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    err = context.error
    if isinstance(err, Conflict):
        logger.warning("Conflict: запущено 2 getUpdates. Бот может молчать пока конфликт не исчезнет.")
        return
    logger.exception("Unhandled error:", exc_info=err)

# =========================
# ===== MAIN ==============
# =========================
def main():
    if not TOKEN:
        raise RuntimeError("TOKEN не найден. Railway -> Variables -> TOKEN")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL не найден. Railway -> Variables -> DATABASE_URL")

    db_connect()
    init_db()

    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(inline_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handler))
    app.add_error_handler(error_handler)

    print("✅ Бот запущен")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
