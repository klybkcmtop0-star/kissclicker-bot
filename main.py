import os
import logging
import time
import asyncio
import random
import html
from datetime import datetime, timedelta
from typing import Optional, Tuple, List

import psycopg2
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.constants import ParseMode
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
DATABASE_URL = os.getenv("DATABASE_URL")  # Railway -> Variables -> DATABASE_URL

ADMIN_IDS = {1924971257}  # добавляй через запятую: {192..., 503...}
CHANNEL_ID = "@kisspromochannel"

# Экономика
MIN_WITHDRAW = 2000

# Клики / лимиты
BASE_CLICK_LIMIT_DEFAULT = 2000
CLICK_RESET_HOURS = 4
REF_REWARD = 150

DAILY_BONUS_AMOUNT = 500
DAILY_BONUS_HOURS = 24

# VIP
VIP_LIMITS = {"VIP": 2500, "MVP": 3500, "PREMIUM": 4500}
VIP_RANK = {"VIP": 1, "MVP": 2, "PREMIUM": 3}

# Косметика: титулы
TITLE_NAMES = {
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
    "GOD": "God",
    "HACKER": "Hacker",
    "BETA_CREATOR": "Beta Creator",
    "GOJO": "GOJO",
    "CREATOR": "Creator",
    "MASTER_CLICK": "Master Click",
    "ELITE_CLICKER": "Elite Clicker",
    "ULTRA_CLICKER": "Ultra Clicker",
    "IMPOSSIBLE_CLICKER": "Impossible Clicker",
}
TITLE_PROGRESS = [
    (0, "ROOKIE"),
    (5000, "MASTER_CLICK"),
    (8000, "ELITE_CLICKER"),
    (13000, "ULTRA_CLICKER"),
    (20000, "IMPOSSIBLE_CLICKER"),
]

# Косметика: фоны (магазин)
THEME_NAMES = {
    "FIRE": "🔥 Огненный",
    "DARK": "🌑 Тёмный",
    "CRYSTAL": "💎 Кристальный",
    "ICE": "❄️ Ледяной",
    "NEWYEAR": "🎄 Новогодний",
    "CHOC": "🍫 Шоколадный",
    "TOP": "⭐️ Топовый",
}
THEME_ICON = {
    "FIRE": "🔥",
    "DARK": "🌑",
    "CRYSTAL": "💎",
    "ICE": "❄️",
    "NEWYEAR": "🎄",
    "CHOC": "🍫",
    "TOP": "⭐️",
}
THEME_PRICES = {
    "FIRE": 1200,
    "DARK": 1700,
    "CRYSTAL": 2300,
    "ICE": 2300,
    "NEWYEAR": 2700,
    "CHOC": 3000,
    "TOP": 4000,
}

COSMETIC_CHANGE_COOLDOWN_SEC = 10

# Улучшения (0..10)
UPGRADE_MAX = 10
UPGRADE_BONUS_CLICKS = {
    0: 0, 1: 200, 2: 250, 3: 300, 4: 350,
    5: 400, 6: 450, 7: 500, 8: 600, 9: 800, 10: 1000,
}
UPGRADE_PRICES = {
    0: 3500, 1: 5000, 2: 7000, 3: 9000, 4: 12000,
    5: 14500, 6: 17000, 7: 19500, 8: 22000, 9: 25000,
}

def click_reward_by_level(lvl: int) -> int:
    if lvl >= 10:
        return 3
    if lvl >= 5:
        return 2
    return 1

# Кейсы
CASE_PRICES = {"common": 500, "rare": 1000, "legend": 3000}
CASE_LIMITS_12H = {"common": 7, "rare": 4, "legend": 2}
CASE_RESET_HOURS = 12
CASE_OPEN_COOLDOWN_SEC = 8  # “интрига”

CASE_DROPS = {
    "common": [
        ("GOLD", 100, 45),
        ("GOLD", 250, 25),
        ("GOLD", 700, 15),
        ("GOLD", 1000, 8),
        ("VIP", ("VIP", 1, "day"), 3),
        ("VIP", ("MVP", 1, "day"), 2),
        ("GOLD", 2000, 2),
    ],
    "rare": [
        ("GOLD", 400, 45),
        ("GOLD", 700, 25),
        ("GOLD", 1400, 15),
        ("GOLD", 1700, 8),
        ("VIP", ("MVP", 3, "day"), 4),
        ("VIP", ("PREMIUM", 1, "day"), 2),
        ("GOLD", 4000, 1),
    ],
    "legend": [
        ("GOLD", 1000, 35),
        ("GOLD", 1500, 25),
        ("GOLD", 3300, 18),
        ("GOLD", 3900, 10),
        ("VIP", ("MVP", 5, "day"), 6),
        ("VIP", ("PREMIUM", 3, "day"), 4),
        ("GOLD", 6500, 2),
    ],
}

# Казино
CASINO_COOLDOWN_SEC = 5
CASINO_MIN_BET = 100
CASINO_MAX_BET = 500000
CASINO_COEF = {
    "bigsmall": 1.5,
    "evenodd": 1.5,
    "number": 2.2,
}

# =========================
# ===== АНТИСПАМ / АНТИКЛИК
# =========================
CLICK_MIN_INTERVAL_SEC = 0.55

HUMAN_CHECK_K = 12         # окно K
HUMAN_CHECK_EPS = 0.07     # порог ровности (сек)
HUMAN_CHECK_TRIGGER = 40   # после N "ровных" кликов — проверка

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
    return {"host": host, "port": port, "dbname": dbname, "user": user, "password": password, "sslmode": "require"}

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

def migrate_add_column(sql: str):
    try:
        db_exec(sql)
    except Exception as e:
        logger.warning(f"Migration skipped/failed: {e}")

def init_db():
    # базовые таблицы
    db_exec(
        """
        CREATE TABLE IF NOT EXISTS users (
            id BIGINT PRIMARY KEY,
            balance DOUBLE PRECISION DEFAULT 0,
            banned INTEGER DEFAULT 0,
            clicks_used INTEGER DEFAULT 0,
            last_click_reset TEXT,
            subscribed INTEGER DEFAULT 0
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
            status TEXT DEFAULT 'pending'
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

    # ====== БЕЗ DO $$ : безопасные ALTER TABLE ... IF NOT EXISTS ======
    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS vip_type TEXT DEFAULT NULL")
    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS vip_until TEXT DEFAULT NULL")
    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS vip_base_limit INTEGER DEFAULT NULL")

    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS total_clicks BIGINT DEFAULT 0")
    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT DEFAULT NULL")
    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_daily_bonus TEXT DEFAULT NULL")

    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS ref_bonus_10 INTEGER DEFAULT 0")
    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS ref_bonus_50 INTEGER DEFAULT 0")
    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS ref_bonus_100 INTEGER DEFAULT 0")

    migrate_add_column("ALTER TABLE withdrawals ADD COLUMN IF NOT EXISTS admin_note TEXT DEFAULT NULL")
    migrate_add_column("ALTER TABLE withdrawals ADD COLUMN IF NOT EXISTS decided_at TEXT DEFAULT NULL")

    migrate_add_column(f"ALTER TABLE users ADD COLUMN IF NOT EXISTS base_click_limit INTEGER DEFAULT {BASE_CLICK_LIMIT_DEFAULT}")
    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS upgrade_level INTEGER DEFAULT 0")

    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS active_title TEXT DEFAULT 'ROOKIE'")
    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS active_theme TEXT DEFAULT NULL")
    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_cosmetic_change TEXT DEFAULT NULL")

    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS cases_common INTEGER DEFAULT 0")
    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS cases_rare INTEGER DEFAULT 0")
    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS cases_legend INTEGER DEFAULT 0")

    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS case_open_common INTEGER DEFAULT 0")
    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS case_open_rare INTEGER DEFAULT 0")
    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS case_open_legend INTEGER DEFAULT 0")
    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS case_reset_at TEXT DEFAULT NULL")

    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_case_open TEXT DEFAULT NULL")
    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_casino_play TEXT DEFAULT NULL")

    # ====== АНТИСПАМ/АНТИКЛИКЕР: колонки ======
    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_click_at TEXT DEFAULT NULL")
    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS click_intv_buf TEXT DEFAULT NULL")          # CSV float
    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS suspicious_clicks INTEGER DEFAULT 0")      # счётчик
    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS human_lock INTEGER DEFAULT 0")             # 1 = заблокирован до подтверждения
    migrate_add_column("ALTER TABLE users ADD COLUMN IF NOT EXISTS human_lock_sent_at TEXT DEFAULT NULL")     # чтобы не спамить сообщениями

    # Владение титулами/фонами (сроки)
    db_exec(
        """
        CREATE TABLE IF NOT EXISTS user_titles (
            user_id BIGINT,
            title_code TEXT,
            until TEXT DEFAULT NULL,
            PRIMARY KEY(user_id, title_code)
        )
        """
    )
    db_exec(
        """
        CREATE TABLE IF NOT EXISTS user_themes (
            user_id BIGINT,
            theme_code TEXT,
            until TEXT DEFAULT NULL,
            PRIMARY KEY(user_id, theme_code)
        )
        """
    )

    # фиксы старых схем
    migrate_add_column("ALTER TABLE user_titles ADD COLUMN IF NOT EXISTS until TEXT DEFAULT NULL")
    migrate_add_column("ALTER TABLE user_themes ADD COLUMN IF NOT EXISTS until TEXT DEFAULT NULL")
    try:
        db_exec("CREATE UNIQUE INDEX IF NOT EXISTS user_titles_uq ON user_titles (user_id, title_code)")
    except Exception:
        pass
    try:
        db_exec("CREATE UNIQUE INDEX IF NOT EXISTS user_themes_uq ON user_themes (user_id, theme_code)")
    except Exception:
        pass

# =========================
# ===== МЕНЮ ==============
# =========================
def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

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
            ["⚙ Выдать лимит кликов", "🎖 Выдать привилегию"],
            ["🏷 Выдать титул", "🌌 Выдать фон"],
            ["📚 Список кодов", "📋 Заявки на вывод"],
            ["Рассылка", "Все промокоды"],
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

def cases_inline_menu(common: int, rare: int, legend: int):
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"📦 Обычный (x{common}) — Открыть", callback_data="case_open_common")],
            [InlineKeyboardButton(f"🎁 Редкий (x{rare}) — Открыть", callback_data="case_open_rare")],
            [InlineKeyboardButton(f"💎 Легендарный (x{legend}) — Открыть", callback_data="case_open_legend")],
            [InlineKeyboardButton("🛒 Магазин кейсов", callback_data="case_shop")],
            [InlineKeyboardButton("ℹ️ Что может выпасть", callback_data="case_info")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_profile")],
        ]
    )

def case_shop_menu():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"📦 Купить Обычный — {CASE_PRICES['common']}G", callback_data="case_buy_common")],
            [InlineKeyboardButton(f"🎁 Купить Редкий — {CASE_PRICES['rare']}G", callback_data="case_buy_rare")],
            [InlineKeyboardButton(f"💎 Купить Легендарный — {CASE_PRICES['legend']}G", callback_data="case_buy_legend")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="cases")],
        ]
    )

def cosmetics_menu():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🏷 Титул", callback_data="cos_title")],
            [InlineKeyboardButton("🌌 Фон", callback_data="cos_theme")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_profile")],
        ]
    )

def title_select_menu(user_titles: list, active: str):
    rows = []
    for code in user_titles:
        name = TITLE_NAMES.get(code, code)
        mark = "✅ " if code == active else ""
        rows.append([InlineKeyboardButton(f"{mark}{name}", callback_data=f"title_set:{code}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="cosmetics")])
    return InlineKeyboardMarkup(rows)

def theme_menu(owned: list, active: Optional[str]):
    rows = []
    for code in owned:
        icon = THEME_ICON.get(code, "")
        name = THEME_NAMES.get(code, code)
        mark = "✅ " if active == code else ""
        rows.append([InlineKeyboardButton(f"{mark}{icon} {name}", callback_data=f"theme_set:{code}")])
    rows.append([InlineKeyboardButton("🛒 Магазин фонов", callback_data="theme_shop")])
    if active:
        rows.append([InlineKeyboardButton("🧹 Снять фон", callback_data="theme_clear")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="cosmetics")])
    return InlineKeyboardMarkup(rows)

def theme_shop_menu():
    rows = []
    for code in ["FIRE", "DARK", "CRYSTAL", "ICE", "NEWYEAR", "CHOC", "TOP"]:
        rows.append([InlineKeyboardButton(f"{THEME_NAMES[code]} — {THEME_PRICES[code]}G", callback_data=f"theme_buy:{code}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="cos_theme")])
    return InlineKeyboardMarkup(rows)

def upgrades_menu(level: int):
    if level >= UPGRADE_MAX:
        return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_profile")]])
    price = UPGRADE_PRICES.get(level, None)
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"⬆️ Улучшить до {level+1} (за {price}G)", callback_data="upgrade_buy")],
            [InlineKeyboardButton("ℹ️ Инфо", callback_data="upgrade_info")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_profile")],
        ]
    )

def casino_menu():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📉 Куб: Больше / Меньше", callback_data="casino_game:bigsmall")],
            [InlineKeyboardButton("⚫ Куб: Чёт / Нечёт", callback_data="casino_game:evenodd")],
            [InlineKeyboardButton("🎯 Куб: Угадай число (1–6)", callback_data="casino_game:number")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_profile")],
        ]
    )

def casino_choice_menu(game: str):
    if game == "bigsmall":
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("⬆️ Больше (4–6)", callback_data="casino_pick:big")],
                [InlineKeyboardButton("⬇️ Меньше (1–3)", callback_data="casino_pick:small")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="casino")],
            ]
        )
    if game == "evenodd":
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("⚫ Чёт", callback_data="casino_pick:even")],
                [InlineKeyboardButton("⚪ Нечёт", callback_data="casino_pick:odd")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="casino")],
            ]
        )
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("1", callback_data="casino_pick:1"),
                InlineKeyboardButton("2", callback_data="casino_pick:2"),
                InlineKeyboardButton("3", callback_data="casino_pick:3"),
            ],
            [
                InlineKeyboardButton("4", callback_data="casino_pick:4"),
                InlineKeyboardButton("5", callback_data="casino_pick:5"),
                InlineKeyboardButton("6", callback_data="casino_pick:6"),
            ],
            [InlineKeyboardButton("⬅️ Назад", callback_data="casino")],
        ]
    )

def human_check_markup():
    return InlineKeyboardMarkup([[InlineKeyboardButton("✅ Я ЧЕЛОВЕК", callback_data="human_ok")]])

# =========================
# ===== СТИЛИ ПРОФИЛЯ =====
# =========================
STYLE_PRESETS = {
    None: {  # базовый, если фона нет
        "header": "• ПРОФИЛЬ •",
        "divider": "--------------------",
        "icons": {"bal": "💰", "period": "📊", "total": "🏁", "upg": "⚡", "rw": "💎", "vip": "🎖"},
        "order": ["vip", "divider", "bal", "period", "total", "upg", "rw", "divider", "reset"],
    },
    "DARK": {
        "header": "░░░ 🌑 ТЁМНЫЙ ПРОФИЛЬ ░░░",
        "divider": "░░░░░░░░░░",
        "icons": {"bal": "⛓", "period": "⛓", "total": "⛓", "upg": "⛓", "rw": "⛓", "vip": "⛓"},
        "order": ["vip", "divider", "bal", "period", "total", "upg", "rw", "divider", "reset"],
    },
    "FIRE": {
        "header": "🔥🔥🔥 ОГНЕННЫЙ ПРОФИЛЬ 🔥🔥🔥",
        "divider": "━━━━━━━━━━━━",
        "icons": {"bal": "🔥", "period": "🔥", "total": "🔥", "upg": "🔥", "rw": "🔥", "vip": "🔥"},
        "order": ["vip", "divider", "bal", "rw", "period", "upg", "total", "divider", "reset"],
    },
    "CRYSTAL": {
        "header": "✦✧✦ КРИСТАЛЬНЫЙ ПРОФИЛЬ ✦✧✦",
        "divider": "✧✧✧✧✧✧✧",
        "icons": {"bal": "💎", "period": "✧", "total": "✧", "upg": "✦", "rw": "✦", "vip": "👑"},
        "order": ["vip", "divider", "bal", "upg", "rw", "period", "total", "divider", "reset"],
    },
    "ICE": {
        "header": "❄❄❄ ЛЕДЯНОЙ ПРОФИЛЬ ❄❄❄",
        "divider": "═══════",
        "icons": {"bal": "❄", "period": "❄", "total": "❄", "upg": "❄", "rw": "❄", "vip": "❄"},
        "order": ["vip", "divider", "period", "total", "bal", "upg", "rw", "divider", "reset"],
    },
    "NEWYEAR": {
        "header": "🎄✨ НОВОГОДНИЙ ПРОФИЛЬ ✨🎄",
        "divider": "✨✨✨✨✨✨",
        "icons": {"bal": "🎁", "period": "❄", "total": "❄", "upg": "⭐", "rw": "⭐", "vip": "🎄"},
        "order": ["vip", "divider", "bal", "period", "upg", "total", "rw", "divider", "reset"],
    },
    "CHOC": {
        "header": "🍫 ШОКОЛАДНЫЙ ПРОФИЛЬ 🍫",
        "divider": "▬▬▬▬▬▬▬",
        "icons": {"bal": "🍫", "period": "☕", "total": "☕", "upg": "🍩", "rw": "🍫", "vip": "🍪"},
        "order": ["vip", "divider", "bal", "period", "total", "upg", "rw", "divider", "reset"],
    },
    "TOP": {
        "header": "⭐️⭐️⭐️ ТОП ПРОФИЛЬ ⭐️⭐️⭐️",
        "divider": "★★★★★★★",
        "icons": {"bal": "⭐", "period": "🏆", "total": "🏆", "upg": "⚜", "rw": "⭐", "vip": "👑"},
        "order": ["vip", "divider", "total", "bal", "upg", "period", "rw", "divider", "reset"],
    },
}

# =========================
# ===== ВСПОМОГАТЕЛЬНОЕ ===
# =========================
def now_iso():
    return datetime.now().isoformat(timespec="seconds")

def now_human():
    return datetime.now().strftime("%d.%m.%Y %H:%M")

def format_time_left(td: timedelta) -> str:
    seconds = int(td.total_seconds())
    if seconds < 0:
        return "0м"
    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    minutes = (seconds % 3600) // 60
    if days > 0:
        return f"{days}д {hours}ч {minutes}м"
    if hours > 0:
        return f"{hours}ч {minutes}м"
    return f"{minutes}м"

async def safe_reply(update: Update, text: str, reply_markup=None, parse_mode: Optional[str] = None):
    try:
        if update.message:
            return await update.message.reply_text(
                text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=True,
            )
    except TimedOut:
        try:
            if update.message:
                return await update.message.reply_text(
                    text,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                    disable_web_page_preview=True,
                )
        except Exception as e:
            logger.warning(f"safe_reply second try failed: {e}")
    except Exception as e:
        logger.warning(f"safe_reply failed: {e}")

async def is_subscribed(bot, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(CHANNEL_ID, user_id)
        return member.status in ["member", "administrator", "creator"]
    except Exception:
        return False

async def require_subscribed(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    # ключевой гейт: если не подписан — не даём играть
    sub = await is_subscribed(context.bot, user_id)
    db_exec("UPDATE users SET subscribed=%s WHERE id=%s", (1 if sub else 0, user_id))
    if not sub:
        await safe_reply(
            update,
            f"🔔 Подпишись на канал:\n{CHANNEL_ID}\n\nПосле подписки нажми «✅ Я подписался»",
            reply_markup=subscribe_menu(),
        )
        return False
    return True

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

def parse_duration(value: str, unit: str) -> Optional[timedelta]:
    try:
        v = int(value)
    except Exception:
        return None
    u = (unit or "").strip().lower()
    if u in ("м", "мин", "минут", "минуты", "minute", "minutes", "min"):
        return timedelta(minutes=v)
    if u in ("ч", "час", "часа", "часов", "hour", "hours", "h"):
        return timedelta(hours=v)
    if u in ("д", "дн", "день", "дня", "дней", "day", "days", "d"):
        return timedelta(days=v)

    raw = (value + unit).strip().lower()
    if raw.endswith("м"):
        try:
            return timedelta(minutes=int(raw[:-1]))
        except Exception:
            return None
    if raw.endswith("ч"):
        try:
            return timedelta(hours=int(raw[:-1]))
        except Exception:
            return None
    if raw.endswith("д"):
        try:
            return timedelta(days=int(raw[:-1]))
        except Exception:
            return None
    return None

def is_infinity(s: str) -> bool:
    return s.strip().lower() in ("infinity", "inf", "♾️", "♾", "navsegda", "навсегда")

def ensure_user(user_id: int, username: Optional[str] = None):
    db_exec("INSERT INTO users (id) VALUES (%s) ON CONFLICT (id) DO NOTHING", (user_id,))
    if username is not None:
        db_exec("UPDATE users SET username=%s WHERE id=%s", (username, user_id))
    db_exec(
        "INSERT INTO user_titles (user_id, title_code, until) VALUES (%s,%s,NULL) "
        "ON CONFLICT (user_id, title_code) DO NOTHING",
        (user_id, "ROOKIE"),
    )

def get_active_title(user_id: int) -> str:
    row = db_fetchone("SELECT active_title FROM users WHERE id=%s", (user_id,))
    code = (row[0] if row and row[0] else "ROOKIE")
    if code not in TITLE_NAMES:
        return "ROOKIE"
    return code

def user_link_html(user_id: int, username: Optional[str]) -> str:
    if username:
        safe_u = html.escape(username)
        return f"@{safe_u}"
    return f'<a href="tg://user?id={user_id}">{user_id}</a>'

def check_and_update_vip(user_id: int) -> Tuple[Optional[str], Optional[datetime]]:
    row = db_fetchone("SELECT vip_type, vip_until FROM users WHERE id=%s", (user_id,))
    if not row:
        return None, None
    vip_type, vip_until = row
    if not vip_type or not vip_until:
        return None, None
    try:
        until_dt = datetime.fromisoformat(vip_until)
    except Exception:
        db_exec("UPDATE users SET vip_type=NULL, vip_until=NULL, vip_base_limit=NULL WHERE id=%s", (user_id,))
        return None, None
    now = datetime.now()
    if now >= until_dt:
        db_exec("UPDATE users SET vip_type=NULL, vip_until=NULL, vip_base_limit=NULL WHERE id=%s", (user_id,))
        return None, None
    return vip_type, until_dt

def vip_frame_icon(vip_type: Optional[str]) -> str:
    if vip_type == "VIP":
        return "💎"
    if vip_type == "MVP":
        return "🏆"
    if vip_type == "PREMIUM":
        return "🔥"
    return ""

def ensure_progress_titles(user_id: int):
    row = db_fetchone("SELECT COALESCE(total_clicks,0) FROM users WHERE id=%s", (user_id,))
    tc = int(row[0]) if row else 0
    for need, code in TITLE_PROGRESS:
        if tc >= need:
            db_exec(
                "INSERT INTO user_titles (user_id, title_code, until) VALUES (%s,%s,NULL) "
                "ON CONFLICT (user_id, title_code) DO NOTHING",
                (user_id, code),
            )

def get_effective_limits_and_reward(user_id: int) -> Tuple[int, int]:
    vip_type, _vip_until = check_and_update_vip(user_id)
    row = db_fetchone("SELECT base_click_limit, upgrade_level FROM users WHERE id=%s", (user_id,))
    base_limit = int(row[0]) if row and row[0] else BASE_CLICK_LIMIT_DEFAULT
    lvl = int(row[1]) if row and row[1] is not None else 0
    lvl = max(0, min(UPGRADE_MAX, lvl))
    bonus = UPGRADE_BONUS_CLICKS.get(lvl, 0)

    if vip_type in VIP_LIMITS:
        effective = VIP_LIMITS[vip_type] + bonus
    else:
        effective = base_limit + bonus

    reward = click_reward_by_level(lvl)
    return effective, reward

def check_click_reset(user_id: int) -> Tuple[int, datetime]:
    row = db_fetchone("SELECT last_click_reset, clicks_used FROM users WHERE id=%s", (user_id,))
    now = datetime.now()
    if not row or row[0] is None:
        db_exec("UPDATE users SET last_click_reset=%s, clicks_used=0 WHERE id=%s",
                (now.strftime("%Y-%m-%d %H:%M:%S"), user_id))
        return 0, now + timedelta(hours=CLICK_RESET_HOURS)

    last_reset = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
    next_reset = last_reset + timedelta(hours=CLICK_RESET_HOURS)

    if now >= next_reset:
        db_exec("UPDATE users SET last_click_reset=%s, clicks_used=0 WHERE id=%s",
                (now.strftime("%Y-%m-%d %H:%M:%S"), user_id))
        return 0, now + timedelta(hours=CLICK_RESET_HOURS)

    return int(row[1]), next_reset

def case_reset_if_needed(user_id: int):
    row = db_fetchone("SELECT case_reset_at FROM users WHERE id=%s", (user_id,))
    now = datetime.now()
    if not row or not row[0]:
        db_exec(
            "UPDATE users SET case_reset_at=%s, case_open_common=0, case_open_rare=0, case_open_legend=0 WHERE id=%s",
            (now_iso(), user_id),
        )
        return
    try:
        last = datetime.fromisoformat(row[0])
    except Exception:
        db_exec(
            "UPDATE users SET case_reset_at=%s, case_open_common=0, case_open_rare=0, case_open_legend=0 WHERE id=%s",
            (now_iso(), user_id),
        )
        return
    if now >= last + timedelta(hours=CASE_RESET_HOURS):
        db_exec(
            "UPDATE users SET case_reset_at=%s, case_open_common=0, case_open_rare=0, case_open_legend=0 WHERE id=%s",
            (now_iso(), user_id),
        )

def case_cooldown_left(user_id: int) -> int:
    row = db_fetchone("SELECT last_case_open FROM users WHERE id=%s", (user_id,))
    if not row or not row[0]:
        return 0
    try:
        last = datetime.fromisoformat(row[0])
    except Exception:
        return 0
    left = CASE_OPEN_COOLDOWN_SEC - int((datetime.now() - last).total_seconds())
    return max(0, left)

def set_case_touch(user_id: int):
    db_exec("UPDATE users SET last_case_open=%s WHERE id=%s", (now_iso(), user_id))

def casino_cooldown_left(user_id: int) -> int:
    row = db_fetchone("SELECT last_casino_play FROM users WHERE id=%s", (user_id,))
    if not row or not row[0]:
        return 0
    try:
        last = datetime.fromisoformat(row[0])
    except Exception:
        return 0
    left = CASINO_COOLDOWN_SEC - int((datetime.now() - last).total_seconds())
    return max(0, left)

def set_casino_touch(user_id: int):
    db_exec("UPDATE users SET last_casino_play=%s WHERE id=%s", (now_iso(), user_id))

def cosmetic_cooldown_left(user_id: int) -> int:
    row = db_fetchone("SELECT last_cosmetic_change FROM users WHERE id=%s", (user_id,))
    if not row or not row[0]:
        return 0
    try:
        last = datetime.fromisoformat(row[0])
    except Exception:
        return 0
    left = COSMETIC_CHANGE_COOLDOWN_SEC - int((datetime.now() - last).total_seconds())
    return max(0, left)

def set_cosmetic_touch(user_id: int):
    db_exec("UPDATE users SET last_cosmetic_change=%s WHERE id=%s", (now_iso(), user_id))

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

def weighted_choice(items):
    total = sum(w for *_rest, w in items)
    r = random.randint(1, total)
    acc = 0
    for t, v, w in items:
        acc += w
        if r <= acc:
            return t, v
    return items[-1][0], items[-1][1]

def vip_apply_reward(user_id: int, vip_type: str, amount: int, unit: str) -> Tuple[bool, str]:
    current, until_dt = check_and_update_vip(user_id)
    cur_rank = VIP_RANK.get(current, 0) if current else 0
    new_rank = VIP_RANK.get(vip_type, 0)

    if cur_rank > new_rank:
        return False, "У вас уже есть привилегия выше. ✅"

    dur = parse_duration(str(amount), unit)
    if not dur:
        return False, "Ошибка времени VIP."

    now = datetime.now()
    if current == vip_type and until_dt:
        new_until = until_dt + dur
    else:
        new_until = now + dur

    db_exec("UPDATE users SET vip_type=%s, vip_until=%s WHERE id=%s", (vip_type, new_until.isoformat(), user_id))
    return True, f"Вы получили VIP: {vip_type} на {amount} {unit} ✅"

# =========================
# ===== АНТИКЛИКЕР ЛОГИКА ==
# =========================
def _parse_buf_csv(s: Optional[str]) -> List[float]:
    if not s:
        return []
    out = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(float(part))
        except Exception:
            continue
    return out

def _buf_to_csv(buf: List[float]) -> str:
    return ",".join(f"{x:.3f}" for x in buf)

def human_lock_enabled(user_id: int) -> bool:
    row = db_fetchone("SELECT human_lock FROM users WHERE id=%s", (user_id,))
    return bool(row and int(row[0]) == 1)

async def maybe_send_human_lock(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    # чтобы не спамить "подтверди" при каждом сообщении
    row = db_fetchone("SELECT human_lock_sent_at FROM users WHERE id=%s", (user_id,))
    last = row[0] if row else None
    now = datetime.now()

    send = True
    if last:
        try:
            last_dt = datetime.fromisoformat(last)
            if (now - last_dt).total_seconds() < 6:
                send = False
        except Exception:
            send = True

    if send:
        db_exec("UPDATE users SET human_lock_sent_at=%s WHERE id=%s", (now_iso(), user_id))
        await safe_reply(update, "❗️Подтверди, что ты человек", reply_markup=human_check_markup())

def click_rate_and_autoclick_check(user_id: int) -> Tuple[bool, bool]:
    """
    returns (allow_click, triggered_human_check_now)
    - allow_click False if too fast OR locked
    - triggered True if we just enabled lock now
    """
    row = db_fetchone(
        "SELECT last_click_at, click_intv_buf, suspicious_clicks, human_lock FROM users WHERE id=%s",
        (user_id,),
    )
    last_click_at, buf_csv, susp, lock = row if row else (None, None, 0, 0)

    if int(lock or 0) == 1:
        return False, False

    now = datetime.now()

    if last_click_at:
        try:
            last_dt = datetime.fromisoformat(last_click_at)
        except Exception:
            last_dt = None
    else:
        last_dt = None

    if last_dt is None:
        db_exec("UPDATE users SET last_click_at=%s WHERE id=%s", (now_iso(), user_id))
        return True, False

    delta = (now - last_dt).total_seconds()

    # антиспам: минимум 0.55 сек
    if delta < CLICK_MIN_INTERVAL_SEC:
        return False, False

    # обновляем интервалы
    buf = _parse_buf_csv(buf_csv)
    buf.append(delta)
    if len(buf) > HUMAN_CHECK_K:
        buf = buf[-HUMAN_CHECK_K:]
    db_exec("UPDATE users SET last_click_at=%s, click_intv_buf=%s WHERE id=%s", (now_iso(), _buf_to_csv(buf), user_id))

    # проверка ровности (только если буфер заполнен)
    triggered = False
    susp = int(susp or 0)

    if len(buf) >= HUMAN_CHECK_K:
        avg = sum(buf[-HUMAN_CHECK_K:]) / HUMAN_CHECK_K
        max_dev = max(abs(x - avg) for x in buf[-HUMAN_CHECK_K:])
        # критерий "очень ровно"
        if max_dev <= HUMAN_CHECK_EPS:
            susp += 1
        else:
            # мягкий спад, чтобы не было вечной подозрительности
            susp = max(0, susp - 1)

        if susp >= HUMAN_CHECK_TRIGGER:
            # включаем блокировку
            db_exec("UPDATE users SET human_lock=1, suspicious_clicks=0 WHERE id=%s", (user_id,))
            triggered = True
        else:
            db_exec("UPDATE users SET suspicious_clicks=%s WHERE id=%s", (susp, user_id))

    return True, triggered

# =========================
# ===== СТАРТ =============
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username

    ensure_user(user_id, username=username)
    ensure_progress_titles(user_id)

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

    # подписка — обязательна
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
    case_reset_if_needed(user_id)

    # чистим session state
    context.user_data.clear()
    context.user_data["menu"] = "main"

    await safe_reply(update, "✨ Добро пожаловать!", reply_markup=main_menu(user_id))

# =========================
# ===== PROFILE SEND ======
# =========================
async def send_profile_message(chat, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    vip_type, vip_until_dt = check_and_update_vip(user_id)
    ensure_progress_titles(user_id)

    row = db_fetchone(
        """
        SELECT balance, COALESCE(total_clicks,0), username,
               COALESCE(active_theme,NULL), COALESCE(active_title,'ROOKIE'),
               COALESCE(upgrade_level,0)
        FROM users WHERE id=%s
        """,
        (user_id,),
    )
    if row:
        bal, total_clicks, stored_username, active_theme, active_title, lvl = row
    else:
        bal, total_clicks, stored_username, active_theme, active_title, lvl = (0, 0, None, None, "ROOKIE", 0)

    used, next_reset = check_click_reset(user_id)
    limit, reward = get_effective_limits_and_reward(user_id)

    preset = STYLE_PRESETS.get(active_theme, STYLE_PRESETS[None])
    header = preset["header"]
    divider = preset["divider"]
    icons = preset["icons"]
    order = preset["order"]

    title_name = TITLE_NAMES.get(active_title, active_title)
    nick_link = user_link_html(user_id, stored_username)

    # VIP рамка вокруг ника (как мы утвердили)
    frame = vip_frame_icon(vip_type)
    if frame:
        nick_line = f"{frame} [{html.escape(title_name)}] {nick_link} {frame}"
    else:
        nick_line = f"[{html.escape(title_name)}] {nick_link}"

    vip_status_text = vip_type if vip_type else "нет"
    vip_left_text = format_time_left(vip_until_dt - datetime.now()) if vip_until_dt else "—"

    lines = [header, "", nick_line, ""]
    blocks = {
        "vip": [
            f"{icons['vip']} VIP статус: {vip_status_text}",
            f"{icons['vip']} Срок VIP: {vip_left_text}",
        ],
        "bal": [f"{icons['bal']} Баланс: {int(float(bal))} GOLD"],
        "period": [f"{icons['period']} Клики (за период): {used}/{limit}"],
        "total": [f"{icons['total']} Клики (всего): {int(total_clicks)}"],
        "upg": [f"{icons['upg']} Улучшение: {int(lvl)}/{UPGRADE_MAX}"],
        "rw": [f"{icons['rw']} Награда за клик: +{reward} GOLD"],
        "reset": [f"⏳ До обновления кликов: {format_time_left(next_reset - datetime.now())}"],
        "divider": [divider],
    }

    for key in order:
        for s in blocks.get(key, []):
            lines.append(s)

    text = "\n".join(lines)

    await chat.reply_text(
        text,
        reply_markup=profile_inline_menu(),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )

# =========================
# ===== INLINE HANDLER =====
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

    # human-lock: блокируем всё, кроме подтверждения
    if q.data != "human_ok" and human_lock_enabled(user_id):
        # не редактируем старое — просто шлём сообщение
        fake_update = Update(update.update_id, message=q.message)  # чтобы safe_reply работал одинаково
        await maybe_send_human_lock(fake_update, context, user_id)
        return

    # подтверждение "Я ЧЕЛОВЕК"
    if q.data == "human_ok":
        db_exec("UPDATE users SET human_lock=0, human_lock_sent_at=NULL, suspicious_clicks=0 WHERE id=%s", (user_id,))
        try:
            await q.message.reply_text("✅ Спасибо! Проверка пройдена. Можно продолжать.")
        except Exception:
            pass
        return

    # подписка — обязательна и для инлайна
    # (кнопка "⬅️ Назад" и любые фичи — только подписанным)
    # исключение: пользователь может быть в процессе подписки через клавиатуру
    # тут — жёстко гейт
    fake_update = Update(update.update_id, message=q.message)
    ok_sub = await require_subscribed(fake_update, context, user_id)
    if not ok_sub:
        return

    check_and_update_vip(user_id)
    ensure_progress_titles(user_id)

    data = q.data or ""

    if data == "back_profile":
        await send_profile_message(q.message, context, user_id)
        return

    if data == "noop":
        return

    # ТОПЫ
    if data == "tops":
        await q.message.reply_text("🏆 Выберите ТОП:", reply_markup=tops_inline_menu())
        return

    if data == "top_clicks":
        rows = db_fetchall(
            """
            SELECT id, username, COALESCE(total_clicks,0) AS tc, COALESCE(active_title,'ROOKIE') AS t
            FROM users ORDER BY tc DESC, id ASC LIMIT 10
            """
        )
        msg = "📊 ТОП по кликам (всего)\n\n"
        if not rows:
            msg += "Пока пусто."
        else:
            for i, (uid, uname, tc, tcode) in enumerate(rows, start=1):
                tname = TITLE_NAMES.get(tcode, tcode)
                msg += f"{i}) [{html.escape(tname)}] {user_link_html(uid, uname)} — {int(tc)} кликов\n"
        await q.message.reply_text(msg, reply_markup=tops_inline_menu(), parse_mode=ParseMode.HTML)
        return

    if data == "top_balance":
        rows = db_fetchall(
            """
            SELECT id, username, balance, COALESCE(active_title,'ROOKIE') AS t
            FROM users ORDER BY balance DESC, id ASC LIMIT 10
            """
        )
        msg = "💰 ТОП по балансу\n\n"
        if not rows:
            msg += "Пока пусто."
        else:
            for i, (uid, uname, bal, tcode) in enumerate(rows, start=1):
                tname = TITLE_NAMES.get(tcode, tcode)
                msg += f"{i}) [{html.escape(tname)}] {user_link_html(uid, uname)} — {round(float(bal), 2)} GOLD\n"
        await q.message.reply_text(msg, reply_markup=tops_inline_menu(), parse_mode=ParseMode.HTML)
        return

    if data == "top_refs":
        rows = db_fetchall(
            """
            SELECT r.referrer_id, u.username, COUNT(*) AS c, COALESCE(u.active_title,'ROOKIE') AS t
            FROM referrals r
            JOIN users uref ON uref.id = r.user_id
            LEFT JOIN users u ON u.id = r.referrer_id
            WHERE uref.subscribed=1
            GROUP BY r.referrer_id, u.username, u.active_title
            ORDER BY c DESC, r.referrer_id ASC
            LIMIT 10
            """
        )
        msg = "👥 ТОП рефоводов (подписанные рефы)\n\n"
        if not rows:
            msg += "Пока пусто."
        else:
            for i, (ref_uid, ref_uname, c, tcode) in enumerate(rows, start=1):
                tname = TITLE_NAMES.get(tcode, tcode)
                msg += f"{i}) [{html.escape(tname)}] {user_link_html(ref_uid, ref_uname)} — {int(c)} рефералов\n"
        await q.message.reply_text(msg, reply_markup=tops_inline_menu(), parse_mode=ParseMode.HTML)
        return

    # DAILY BONUS
    if data == "daily_bonus":
        row = db_fetchone("SELECT last_daily_bonus FROM users WHERE id=%s", (user_id,))
        last_daily = row[0] if row else None

        ok, left = can_take_daily(last_daily)
        if not ok and left is not None:
            await q.message.reply_text(
                f"⏳ Ежедневный бонус уже был.\nСледующий через: {format_time_left(left)}",
            )
            return

        db_exec("UPDATE users SET balance=balance+%s, last_daily_bonus=%s WHERE id=%s",
                (DAILY_BONUS_AMOUNT, now_iso(), user_id))
        await q.message.reply_text(f"✅ Ежедневный бонус получен: +{DAILY_BONUS_AMOUNT} GOLD 🎁")
        return

    # REF BONUSES
    if data == "ref_bonuses":
        await send_ref_bonus_menu(q, context, user_id)
        return

    if data.startswith("claim_ref_"):
        await process_claim_ref_bonus(q, context, user_id, data)
        return

    # CASES
    if data == "cases":
        row = db_fetchone("SELECT cases_common, cases_rare, cases_legend FROM users WHERE id=%s", (user_id,))
        common, rare, legend = row if row else (0, 0, 0)
        await q.message.reply_text("📦 Кейсы:", reply_markup=cases_inline_menu(common, rare, legend))
        return

    if data == "case_shop":
        await q.message.reply_text("🛒 Магазин кейсов (покупка по 1):", reply_markup=case_shop_menu())
        return

    if data == "case_info":
        text = (
            "ℹ️ Что может выпасть:\n\n"
            "📦 Обычный:\n"
            "• 100G / 250G / 700G / 1000G\n"
            "• VIP (1 день) / MVP (1 день)\n"
            "• Джекпот 2000G\n\n"
            "🎁 Редкий:\n"
            "• 400G / 700G / 1400G / 1700G\n"
            "• MVP (3 дня) / PREMIUM (1 день)\n"
            "• Джекпот 4000G\n\n"
            "💎 Легендарный:\n"
            "• 1000G / 1500G / 3300G / 3900G\n"
            "• MVP (5 дней) / PREMIUM (3 дня)\n"
            "• Джекпот 6500G"
        )
        await q.message.reply_text(text)
        return

    if data.startswith("case_buy_"):
        kind = data.split("_")[-1]  # common/rare/legend
        price = CASE_PRICES.get(kind, None)
        if price is None:
            return

        row = db_fetchone("SELECT balance FROM users WHERE id=%s", (user_id,))
        bal = float(row[0]) if row else 0.0
        if bal < price:
            await q.message.reply_text("❌ Недостаточно GOLD.")
            return

        col = "cases_common" if kind == "common" else ("cases_rare" if kind == "rare" else "cases_legend")
        db_exec(f"UPDATE users SET balance=balance-%s, {col}={col}+1 WHERE id=%s", (price, user_id))
        await q.message.reply_text(f"✅ Куплено: {kind.upper()} кейс (+1).")
        row2 = db_fetchone("SELECT cases_common, cases_rare, cases_legend FROM users WHERE id=%s", (user_id,))
        c, r, l = row2 if row2 else (0, 0, 0)
        await q.message.reply_text("📦 Кейсы:", reply_markup=cases_inline_menu(c, r, l))
        return

    if data.startswith("case_open_"):
        kind = data.split("_")[-1]  # common/rare/legend

        left_cd = case_cooldown_left(user_id)
        if left_cd > 0:
            await q.message.reply_text(f"⏳ Подожди {left_cd} сек.")
            return

        case_reset_if_needed(user_id)

        inv_col = "cases_common" if kind == "common" else ("cases_rare" if kind == "rare" else "cases_legend")
        open_col = "case_open_common" if kind == "common" else ("case_open_rare" if kind == "rare" else "case_open_legend")

        row = db_fetchone(f"SELECT {inv_col}, {open_col} FROM users WHERE id=%s", (user_id,))
        inv, opened = row if row else (0, 0)

        if int(inv) <= 0:
            await q.message.reply_text("❌ У вас нет этого кейса. Купите в магазине 🛒")
            return

        limit = CASE_LIMITS_12H[kind]
        if int(opened) >= limit:
            r2 = db_fetchone("SELECT case_reset_at FROM users WHERE id=%s", (user_id,))
            reset_at = None
            if r2 and r2[0]:
                try:
                    reset_at = datetime.fromisoformat(r2[0]) + timedelta(hours=CASE_RESET_HOURS)
                except Exception:
                    reset_at = None
            left = (reset_at - datetime.now()) if reset_at else timedelta(hours=CASE_RESET_HOURS)
            await q.message.reply_text(
                f"❌ Лимит кейсов исчерпан ({limit}/{limit}).\nСледующий сброс через: {format_time_left(left)}"
            )
            return

        db_exec(
            f"UPDATE users SET {inv_col}={inv_col}-1, {open_col}={open_col}+1 WHERE id=%s",
            (user_id,),
        )
        set_case_touch(user_id)

        m = await q.message.reply_text("📦 Открываю кейс…")
        try:
            await asyncio.sleep(2)
            await m.edit_text("🔄 Кручу… ░░░░░")
            await asyncio.sleep(2)
            await m.edit_text("🔄 Кручу… █░░░░")
            await asyncio.sleep(2)
            await m.edit_text("🔄 Кручу… ██░░░")
            await asyncio.sleep(2)
        except Exception:
            pass

        drop_type, drop_value = weighted_choice(CASE_DROPS[kind])

        if drop_type == "GOLD":
            amount = int(drop_value)
            db_exec("UPDATE users SET balance=balance+%s WHERE id=%s", (amount, user_id))
            await q.message.reply_text(f"🎉 Выпало: +{amount} GOLD ✅")
        else:
            vip_type_, amt, unit = drop_value
            ok_, text_ = vip_apply_reward(user_id, vip_type_, int(amt), unit)
            if ok_:
                await q.message.reply_text(f"🎉 Выпало: {vip_type_} ✅\n{text_}")
            else:
                await q.message.reply_text(f"🎉 Выпало: {vip_type_}\n{text_}")

        row2 = db_fetchone("SELECT cases_common, cases_rare, cases_legend FROM users WHERE id=%s", (user_id,))
        c, r, l = row2 if row2 else (0, 0, 0)
        await q.message.reply_text("📦 Кейсы:", reply_markup=cases_inline_menu(c, r, l))
        return

    # COSMETICS
    if data == "cosmetics":
        await q.message.reply_text("🎨 Косметика:", reply_markup=cosmetics_menu())
        return

    if data == "cos_title":
        db_exec(
            """
            DELETE FROM user_titles
            WHERE user_id=%s AND until IS NOT NULL AND until <> '' AND now() > until::timestamp
            """,
            (user_id,),
        )
        rows = db_fetchall("SELECT title_code FROM user_titles WHERE user_id=%s", (user_id,))
        owned = [r[0] for r in rows] if rows else ["ROOKIE"]
        active = get_active_title(user_id)
        await q.message.reply_text("🏷 Выберите титул:", reply_markup=title_select_menu(owned, active))
        return

    if data.startswith("title_set:"):
        left = cosmetic_cooldown_left(user_id)
        if left > 0:
            await q.message.reply_text(f"⏳ Подожди {left} сек.")
            return

        code = data.split(":", 1)[1]
        row = db_fetchone("SELECT 1 FROM user_titles WHERE user_id=%s AND title_code=%s", (user_id, code))
        if not row:
            await q.message.reply_text("❌ У вас нет этого титула.")
            return

        db_exec("UPDATE users SET active_title=%s WHERE id=%s", (code, user_id))
        set_cosmetic_touch(user_id)
        await q.message.reply_text("✅ Титул выбран.")
        await send_profile_message(q.message, context, user_id)
        return

    if data == "cos_theme":
        db_exec(
            """
            DELETE FROM user_themes
            WHERE user_id=%s AND until IS NOT NULL AND until <> '' AND now() > until::timestamp
            """,
            (user_id,),
        )
        rows = db_fetchall("SELECT theme_code FROM user_themes WHERE user_id=%s", (user_id,))
        owned = [r[0] for r in rows] if rows else []
        row2 = db_fetchone("SELECT active_theme FROM users WHERE id=%s", (user_id,))
        active = row2[0] if row2 else None
        await q.message.reply_text("🌌 Фоны:", reply_markup=theme_menu(owned, active))
        return

    if data == "theme_shop":
        await q.message.reply_text("🛒 Магазин фонов (покупка по 1):", reply_markup=theme_shop_menu())
        return

    if data.startswith("theme_buy:"):
        code = data.split(":", 1)[1]
        if code not in THEME_PRICES:
            return

        row = db_fetchone("SELECT 1 FROM user_themes WHERE user_id=%s AND theme_code=%s", (user_id, code))
        if row:
            await q.message.reply_text("✅ У вас уже есть этот фон.")
            return

        price = THEME_PRICES[code]
        bal_row = db_fetchone("SELECT balance FROM users WHERE id=%s", (user_id,))
        bal = float(bal_row[0]) if bal_row else 0.0
        if bal < price:
            await q.message.reply_text("❌ Недостаточно GOLD.")
            return

        db_exec("UPDATE users SET balance=balance-%s WHERE id=%s", (price, user_id))
        db_exec(
            "INSERT INTO user_themes (user_id, theme_code, until) VALUES (%s,%s,NULL) "
            "ON CONFLICT (user_id, theme_code) DO NOTHING",
            (user_id, code),
        )
        await q.message.reply_text("✅ Фон куплен.")
        return

    if data.startswith("theme_set:"):
        left = cosmetic_cooldown_left(user_id)
        if left > 0:
            await q.message.reply_text(f"⏳ Подожди {left} сек.")
            return

        code = data.split(":", 1)[1]
        row = db_fetchone("SELECT 1 FROM user_themes WHERE user_id=%s AND theme_code=%s", (user_id, code))
        if not row:
            await q.message.reply_text("❌ У вас нет этого фона.")
            return

        db_exec("UPDATE users SET active_theme=%s WHERE id=%s", (code, user_id))
        set_cosmetic_touch(user_id)
        await q.message.reply_text("✅ Фон выбран.")
        await send_profile_message(q.message, context, user_id)
        return

    if data == "theme_clear":
        left = cosmetic_cooldown_left(user_id)
        if left > 0:
            await q.message.reply_text(f"⏳ Подожди {left} сек.")
            return
        db_exec("UPDATE users SET active_theme=NULL WHERE id=%s", (user_id,))
        set_cosmetic_touch(user_id)
        await q.message.reply_text("✅ Фон снят.")
        await send_profile_message(q.message, context, user_id)
        return

    # UPGRADES
    if data == "upgrades":
        row = db_fetchone("SELECT upgrade_level FROM users WHERE id=%s", (user_id,))
        lvl = int(row[0]) if row and row[0] is not None else 0
        lvl = max(0, min(UPGRADE_MAX, lvl))

        limit, reward = get_effective_limits_and_reward(user_id)
        bonus = UPGRADE_BONUS_CLICKS.get(lvl, 0)

        text = (
            "⚡ Улучшения\n\n"
            f"Текущий уровень: {lvl}/{UPGRADE_MAX}\n"
            f"Бонус к лимиту: +{bonus} кликов\n"
            f"Итоговый лимит сейчас: {limit}\n"
            f"Награда за клик: +{reward} GOLD"
        )
        await q.message.reply_text(text, reply_markup=upgrades_menu(lvl))
        return

    if data == "upgrade_info":
        lines = ["ℹ️ Инфо по уровням:"]
        for lvl in range(0, UPGRADE_MAX + 1):
            bonus = UPGRADE_BONUS_CLICKS.get(lvl, 0)
            rw = click_reward_by_level(lvl)
            lines.append(f"{lvl}: +{bonus} кликов | +{rw} GOLD/клик")
        await q.message.reply_text("\n".join(lines))
        return

    if data == "upgrade_buy":
        row = db_fetchone("SELECT balance, upgrade_level FROM users WHERE id=%s", (user_id,))
        bal = float(row[0]) if row else 0.0
        lvl = int(row[1]) if row and row[1] is not None else 0
        lvl = max(0, min(UPGRADE_MAX, lvl))

        if lvl >= UPGRADE_MAX:
            await q.message.reply_text("✅ У вас максимальный уровень.")
            return

        price = UPGRADE_PRICES.get(lvl, None)
        if price is None:
            await q.message.reply_text("❌ Ошибка цены улучшения.")
            return

        if bal < price:
            await q.message.reply_text("❌ Недостаточно GOLD.")
            return

        db_exec("UPDATE users SET balance=balance-%s, upgrade_level=upgrade_level+1 WHERE id=%s", (price, user_id))
        await q.message.reply_text(f"✅ Улучшение куплено! Уровень теперь: {lvl+1}")
        await send_profile_message(q.message, context, user_id)
        return

    # CASINO
    if data == "casino":
        await q.message.reply_text("🎲 Казино — выбери игру:", reply_markup=casino_menu())
        return

    if data.startswith("casino_game:"):
        left = casino_cooldown_left(user_id)
        if left > 0:
            await q.message.reply_text(f"⏳ Подожди {left} сек.")
            return

        game = data.split(":", 1)[1]
        context.user_data["casino_game"] = game
        context.user_data["casino_step"] = "amount"
        await q.message.reply_text(f"💰 Введи сумму ставки ({CASINO_MIN_BET}–{CASINO_MAX_BET}):")
        return

    if data.startswith("casino_pick:"):
        if context.user_data.get("casino_step") != "pick":
            await q.message.reply_text("❌ Сначала введи сумму ставки.")
            return

        left = casino_cooldown_left(user_id)
        if left > 0:
            await q.message.reply_text(f"⏳ Подожди {left} сек.")
            return

        bet = context.user_data.get("casino_bet")
        game = context.user_data.get("casino_game")
        pick = data.split(":", 1)[1]

        if not isinstance(bet, int) or bet <= 0 or game not in CASINO_COEF:
            await q.message.reply_text("❌ Ставка не найдена, начни заново.")
            context.user_data.pop("casino_step", None)
            return

        row = db_fetchone("SELECT balance FROM users WHERE id=%s", (user_id,))
        bal = float(row[0]) if row else 0.0
        if bal < bet:
            await q.message.reply_text("❌ Недостаточно GOLD.")
            context.user_data.pop("casino_step", None)
            return

        db_exec("UPDATE users SET balance=balance-%s WHERE id=%s", (bet, user_id))
        set_casino_touch(user_id)
        context.user_data["casino_step"] = "rolling"

        await q.message.reply_text("🎲 Ставка принята. Бросаю кубик…")
        dice_msg = await context.bot.send_dice(chat_id=user_id, emoji="🎲")
        value = getattr(dice_msg.dice, "value", None)
        await asyncio.sleep(5)

        if not isinstance(value, int) or value < 1 or value > 6:
            await q.message.reply_text("❌ Ошибка броска. Ставка возвращена.")
            db_exec("UPDATE users SET balance=balance+%s WHERE id=%s", (bet, user_id))
            context.user_data.pop("casino_step", None)
            return

        win = False
        result_text = ""

        if game == "bigsmall":
            is_big = value >= 4
            win = (pick == "big" and is_big) or (pick == "small" and not is_big)
            result_text = f"Выпало: {value} ({'Больше' if is_big else 'Меньше'})"
        elif game == "evenodd":
            is_even = (value % 2 == 0)
            win = (pick == "even" and is_even) or (pick == "odd" and not is_even)
            result_text = f"Выпало: {value} ({'Чёт' if is_even else 'Нечёт'})"
        else:
            win = (pick.isdigit() and int(pick) == value)
            result_text = f"Выпало: {value}"

        coef = CASINO_COEF[game]
        if win:
            payout = int(bet * coef)
            db_exec("UPDATE users SET balance=balance+%s WHERE id=%s", (payout, user_id))
            await q.message.reply_text(
                f"✅ ВЫ ВЫИГРАЛИ!\n"
                f"Ставка: {bet} GOLD\n"
                f"Коэф: {coef}\n"
                f"{result_text}\n"
                f"🎉 Выигрыш: +{payout} GOLD"
            )
        else:
            await q.message.reply_text(
                f"❌ ВЫ ПРОИГРАЛИ.\n"
                f"Ставка: {bet} GOLD\n"
                f"{result_text}"
            )

        context.user_data.pop("casino_step", None)
        context.user_data.pop("casino_bet", None)
        await q.message.reply_text("🎲 Казино — выбери игру:", reply_markup=casino_menu())
        return

# =========================
# ===== REF BONUS MENU ====
# =========================
def ref_bonuses_inline_menu(ref_count: int, claimed10: int, claimed50: int, claimed100: int):
    buttons = []
    buttons.append([InlineKeyboardButton("✅ 10 рефов — получено", callback_data="noop")]) if claimed10 else buttons.append(
        [InlineKeyboardButton("🎁 Забрать за 10 рефов", callback_data="claim_ref_10")]
    )
    buttons.append([InlineKeyboardButton("✅ 50 рефов — получено", callback_data="noop")]) if claimed50 else buttons.append(
        [InlineKeyboardButton("🎁 Забрать за 50 рефов", callback_data="claim_ref_50")]
    )
    buttons.append([InlineKeyboardButton("✅ 100 рефов — получено", callback_data="noop")]) if claimed100 else buttons.append(
        [InlineKeyboardButton("🎁 Забрать за 100 рефов", callback_data="claim_ref_100")]
    )
    buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_profile")])
    return InlineKeyboardMarkup(buttons)

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
    await q.message.reply_text(text, reply_markup=ref_bonuses_inline_menu(ref_count, claimed10, claimed50, claimed100))

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
        db_exec(
            "UPDATE withdrawals SET status='approved', admin_note=%s, decided_at=%s WHERE id=%s",
            (admin_note, decided_at, wid),
        )
        try:
            msg_user = (
                "✅ Ваша заявка на вывод подтверждена\n"
                f"💰 Сумма: {amount} GOLD\n"
                "🕒 Ожидайте зачисление\n"
            )
            if admin_note.strip():
                msg_user += f"\n💬 Сообщение: {admin_note.strip()}"
            await context.bot.send_message(chat_id=target_uid, text=msg_user)
        except Exception:
            pass
        await safe_reply(update, f"✅ Готово. Заявка #{wid} подтверждена.", reply_markup=admin_menu())
        return True

    if cmd == "cancel":
        db_exec("UPDATE users SET balance=balance+%s WHERE id=%s", (amount, target_uid))
        db_exec(
            "UPDATE withdrawals SET status='declined', admin_note=%s, decided_at=%s WHERE id=%s",
            (admin_note, decided_at, wid),
        )
        try:
            msg_user = (
                "❌ Ваша заявка на вывод отклонена\n"
                f"💰 Сумма: {amount} GOLD\n"
                "↩️ Средства возвращены на баланс.\n"
            )
            if admin_note.strip():
                msg_user += f"\n💬 Причина: {admin_note.strip()}"
            await context.bot.send_message(chat_id=target_uid, text=msg_user)
        except Exception:
            pass
        await safe_reply(update, f"✅ Отклонено. Заявка #{wid} закрыта.", reply_markup=admin_menu())
        return True

    return False

def format_codes_list() -> str:
    t_lines = ["🏷 TITLES (TITLE_CODE → название):"]
    for code, name in sorted(TITLE_NAMES.items()):
        t_lines.append(f"• {code} → {name}")

    th_lines = ["\n🌌 THEMES (THEME_CODE → название/цена):"]
    for code, name in THEME_NAMES.items():
        th_lines.append(f"• {code} → {name} ({THEME_PRICES.get(code, 0)}G)")

    v_lines = ["\n🎖 VIP (тип → лимит):"]
    for k, v in VIP_LIMITS.items():
        v_lines.append(f"• {k} → {v}")

    return "\n".join(t_lines + th_lines + v_lines)

# =========================
# ===== ОБРАБОТКА TEXT =====
# =========================
async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    text = update.message.text.strip()
    user_id = update.effective_user.id
    username = update.effective_user.username

    ensure_user(user_id, username=username)
    check_and_update_vip(user_id)
    ensure_progress_titles(user_id)

    # human-lock: пока не подтвердит — ничего не даём делать (кроме /start и подписки)
    if human_lock_enabled(user_id):
        if text not in ("✅ Я подписался", "/start", "🔙 Назад", "❌ Отмена"):
            await maybe_send_human_lock(update, context, user_id)
            return

    # бан (кроме админа)
    if not is_admin(user_id):
        r = db_fetchone("SELECT banned FROM users WHERE id=%s", (user_id,))
        if r and int(r[0]) == 1:
            await safe_reply(update, "⛔ Вы заблокированы.")
            return

    # ПОДПИСКА (кнопка)
    if text == "✅ Я подписался":
        subscribed = await is_subscribed(context.bot, user_id)
        db_exec("UPDATE users SET subscribed=%s WHERE id=%s", (1 if subscribed else 0, user_id))
        if subscribed:
            await safe_reply(update, "✅ Подписка подтверждена!", reply_markup=main_menu(user_id))
        else:
            await safe_reply(update, "❌ Ты ещё не подписался!", reply_markup=subscribe_menu())
        return

    # /start (как текст) — на всякий случай
    if text == "/start":
        await start(update, context)
        return

    # ГЛОБАЛЬНАЯ ПРОВЕРКА ПОДПИСКИ (везде, не только /start)
    # исключение: сам процесс подписки уже обработан выше
    ok_sub = await require_subscribed(update, context, user_id)
    if not ok_sub:
        return

    # НАЗАД / ОТМЕНА
    if text in ["🔙 Назад", "❌ Отмена"]:
        if is_admin(user_id) and context.user_data.get("admin_action"):
            context.user_data.pop("admin_action", None)
            context.user_data["menu"] = "admin"
            await safe_reply(update, "Действие отменено", reply_markup=admin_menu())
            return

        context.user_data.clear()
        await safe_reply(update, "Главное меню", reply_markup=main_menu(user_id))
        return

    # ПРОФИЛЬ
    if text == "👤 Профиль":
        await safe_reply(update, "Открываю профиль 👇", reply_markup=main_menu(user_id))
        await send_profile_message(update.message, context, user_id)
        return

    # ЗАРАБОТАТЬ
    if text == "💰 Заработать":
        used, _ = check_click_reset(user_id)
        limit, _reward = get_effective_limits_and_reward(user_id)
        if used >= limit:
            await safe_reply(update, "❌ У вас закончились клики", reply_markup=main_menu(user_id))
            return
        context.user_data["earning"] = True
        await safe_reply(update, "👆 Нажимай «КЛИК»", reply_markup=earn_menu())
        return

    # КЛИК
    if text == "👆 КЛИК" and context.user_data.get("earning"):
        # антиспам + антиавтокликер
        allow, triggered = click_rate_and_autoclick_check(user_id)
        if not allow:
            # если включился human-lock — покажем проверку один раз
            if human_lock_enabled(user_id):
                await maybe_send_human_lock(update, context, user_id)
            # слишком быстро — молча игнор (чтобы не ложить бота)
            return

        if triggered and human_lock_enabled(user_id):
            await maybe_send_human_lock(update, context, user_id)
            return

        used, _ = check_click_reset(user_id)
        limit, reward = get_effective_limits_and_reward(user_id)
        if used >= limit:
            await safe_reply(update, "❌ У вас закончились клики", reply_markup=main_menu(user_id))
            return

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
        used += 1
        ensure_progress_titles(user_id)
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
        code = text
        res = db_fetchone("SELECT amount, uses_left FROM promocodes WHERE code=%s", (code,))
        if not res:
            await safe_reply(update, "❌ Неверный промокод", reply_markup=main_menu(user_id))
        else:
            amount, uses_left = res
            used_row = db_fetchone("SELECT 1 FROM used_promocodes WHERE user_id=%s AND code=%s", (user_id, code))
            if used_row:
                await safe_reply(update, "❌ Уже использован", reply_markup=main_menu(user_id))
            elif int(uses_left) <= 0:
                await safe_reply(update, "❌ Промокод недействителен", reply_markup=main_menu(user_id))
            else:
                db_exec("UPDATE users SET balance=balance+%s WHERE id=%s", (amount, user_id))
                db_exec("UPDATE promocodes SET uses_left=uses_left-1 WHERE code=%s", (code,))
                db_exec(
                    "INSERT INTO used_promocodes (user_id, code) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                    (user_id, code),
                )
                await safe_reply(update, f"🎉 ПРОМО АКТИВИРОВАН\n💰 +{amount} GOLD", reply_markup=main_menu(user_id))
        context.user_data.clear()
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
            "• Указывайте только целую сумму от 2000\n"
            "• Примеры: 2000 / 4000 / 8000 / 10000\n"
            "❌ Не нужно: 2100, 2500, 1780 и т.д.",
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

        db_exec(
            "INSERT INTO withdrawals (user_id, amount, requisites, status) VALUES (%s,%s,%s,'pending')",
            (user_id, amount, requisites),
        )
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
        context.user_data.clear()
        return

    # Казино: ввод ставки текстом
    if context.user_data.get("casino_step") == "amount":
        try:
            bet = int(text)
        except Exception:
            await safe_reply(update, "❌ Введите число ставки.")
            return

        if bet < CASINO_MIN_BET or bet > CASINO_MAX_BET:
            await safe_reply(update, f"❌ Ставка должна быть {CASINO_MIN_BET}–{CASINO_MAX_BET}.")
            return

        row = db_fetchone("SELECT balance FROM users WHERE id=%s", (user_id,))
        bal = float(row[0]) if row else 0.0
        if bal < bet:
            await safe_reply(update, "❌ Недостаточно GOLD.")
            return

        game = context.user_data.get("casino_game")
        if game not in CASINO_COEF:
            context.user_data.pop("casino_step", None)
            await safe_reply(update, "❌ Игра не выбрана. Открой казино заново.")
            return

        context.user_data["casino_bet"] = bet
        context.user_data["casino_step"] = "pick"
        await safe_reply(update, "✅ Ставка принята. Выберите вариант:", reply_markup=casino_choice_menu(game))
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
        if text == "📚 Список кодов":
            await safe_reply(update, format_codes_list(), reply_markup=admin_menu())
            return

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

        if text == "⚙ Выдать лимит кликов":
            context.user_data["admin_action"] = "set_base_click_limit"
            await safe_reply(update, "ID НовыйЛимит\nПример: 123456789 2500", reply_markup=cancel_menu())
            return

        if text == "🎖 Выдать привилегию":
            context.user_data["admin_action"] = "give_vip"
            await safe_reply(update, "Формат:\nID VIP 1 час\nID MVP 300 минут\nID PREMIUM 2 дня\nили Infinity", reply_markup=cancel_menu())
            return

        if text == "🏷 Выдать титул":
            context.user_data["admin_action"] = "give_title"
            await safe_reply(update, "Формат:\nID TITLE_CODE 7д\nID TITLE_CODE 300м\nID TITLE_CODE Infinity", reply_markup=cancel_menu())
            return

        if text == "🌌 Выдать фон":
            context.user_data["admin_action"] = "give_theme"
            await safe_reply(update, "Формат:\nID THEME_CODE 7д\nID THEME_CODE 12ч\nID THEME_CODE Infinity", reply_markup=cancel_menu())
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

            elif admin_action == "set_base_click_limit":
                if len(parts) != 2:
                    await safe_reply(update, "❌ Формат: ID НОВЫЙ_ЛИМИТ", reply_markup=cancel_menu())
                    return
                uid, limit = int(parts[0]), int(parts[1])
                ensure_user(uid)
                db_exec("UPDATE users SET base_click_limit=%s WHERE id=%s", (limit, uid))
                await safe_reply(update, f"✅ Базовый лимит кликов для {uid} = {limit}", reply_markup=admin_menu())

            elif admin_action == "give_vip":
                if len(parts) < 3:
                    await safe_reply(update, "❌ Формат:\nID VIP 1 час\nID MVP 300 минут\nID PREMIUM 2 дня\nили Infinity", reply_markup=cancel_menu())
                    return
                uid = int(parts[0])
                vip = parts[1].upper()

                if vip not in VIP_LIMITS:
                    await safe_reply(update, "❌ Привилегия только: VIP / MVP / PREMIUM", reply_markup=cancel_menu())
                    return

                if is_infinity(parts[2]):
                    ensure_user(uid)
                    db_exec("UPDATE users SET vip_type=%s, vip_until=%s WHERE id=%s", (vip, "9999-12-31T23:59:59", uid))
                    await safe_reply(update, f"✅ VIP выдан {uid}: {vip} (Infinity)", reply_markup=admin_menu())
                    return

                if len(parts) < 4:
                    await safe_reply(update, "❌ Формат времени: 1 час / 300 минут / 2 дня", reply_markup=cancel_menu())
                    return

                value, unit = parts[2], parts[3]
                dur = parse_duration(value, unit)
                if not dur:
                    await safe_reply(update, "❌ Время: минут/час/дня (пример: 300 минут / 1 час / 2 дня)", reply_markup=cancel_menu())
                    return

                ensure_user(uid)
                current, until_dt = check_and_update_vip(uid)
                now = datetime.now()
                if current == vip and until_dt:
                    until = until_dt + dur
                else:
                    until = now + dur
                db_exec("UPDATE users SET vip_type=%s, vip_until=%s WHERE id=%s", (vip, until.isoformat(), uid))
                await safe_reply(update, f"✅ VIP выдан {uid}: {vip} ({value} {unit})", reply_markup=admin_menu())

            elif admin_action == "give_title":
                if len(parts) < 3:
                    await safe_reply(update, "❌ Формат:\nID TITLE_CODE 7д\nID TITLE_CODE Infinity", reply_markup=cancel_menu())
                    return
                uid = int(parts[0])
                code = parts[1].upper()
                if code not in TITLE_NAMES:
                    await safe_reply(update, "❌ Неизвестный TITLE_CODE", reply_markup=cancel_menu())
                    return
                ensure_user(uid)

                if is_infinity(parts[2]):
                    db_exec(
                        "INSERT INTO user_titles (user_id, title_code, until) VALUES (%s,%s,NULL) "
                        "ON CONFLICT (user_id,title_code) DO UPDATE SET until=NULL",
                        (uid, code),
                    )
                    await safe_reply(update, f"✅ Титул выдан: {uid} -> {code} (Infinity)", reply_markup=admin_menu())
                    return

                if len(parts) < 4:
                    await safe_reply(update, "❌ Время: 7д / 12ч / 300м", reply_markup=cancel_menu())
                    return
                dur = parse_duration(parts[2], parts[3])
                if not dur:
                    await safe_reply(update, "❌ Время: 7д / 12ч / 300м", reply_markup=cancel_menu())
                    return
                until = datetime.now() + dur
                db_exec(
                    "INSERT INTO user_titles (user_id, title_code, until) VALUES (%s,%s,%s) "
                    "ON CONFLICT (user_id,title_code) DO UPDATE SET until=EXCLUDED.until",
                    (uid, code, until.isoformat()),
                )
                await safe_reply(update, f"✅ Титул выдан: {uid} -> {code} до {until.strftime('%d.%m.%Y %H:%M')}", reply_markup=admin_menu())

            elif admin_action == "give_theme":
                if len(parts) < 3:
                    await safe_reply(update, "❌ Формат:\nID THEME_CODE 7д\nID THEME_CODE Infinity", reply_markup=cancel_menu())
                    return
                uid = int(parts[0])
                code = parts[1].upper()
                if code not in THEME_NAMES:
                    await safe_reply(update, "❌ Неизвестный THEME_CODE", reply_markup=cancel_menu())
                    return
                ensure_user(uid)

                if is_infinity(parts[2]):
                    db_exec(
                        "INSERT INTO user_themes (user_id, theme_code, until) VALUES (%s,%s,NULL) "
                        "ON CONFLICT (user_id,theme_code) DO UPDATE SET until=NULL",
                        (uid, code),
                    )
                    await safe_reply(update, f"✅ Фон выдан: {uid} -> {code} (Infinity)", reply_markup=admin_menu())
                    return

                if len(parts) < 4:
                    await safe_reply(update, "❌ Время: 7д / 12ч / 300м", reply_markup=cancel_menu())
                    return
                dur = parse_duration(parts[2], parts[3])
                if not dur:
                    await safe_reply(update, "❌ Время: 7д / 12ч / 300м", reply_markup=cancel_menu())
                    return
                until = datetime.now() + dur
                db_exec(
                    "INSERT INTO user_themes (user_id, theme_code, until) VALUES (%s,%s,%s) "
                    "ON CONFLICT (user_id,theme_code) DO UPDATE SET until=EXCLUDED.until",
                    (uid, code, until.isoformat()),
                )
                await safe_reply(update, f"✅ Фон выдан: {uid} -> {code} до {until.strftime('%d.%m.%Y %H:%M')}", reply_markup=admin_menu())

            elif admin_action == "broadcast":
                msg = " ".join(parts) if parts else text
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
        logger.warning("Conflict: запущено 2 getUpdates. Бот будет молчать пока не останется 1 инстанс.")
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
