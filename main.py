import os
import logging
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple, List

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
DATABASE_URL = os.getenv("DATABASE_URL")  # Railway -> Variables -> DATABASE_URL
ADMIN_ID = 1924971257
CHANNEL_ID = "@kisspromochannel"

CLICK_REWARD = 1
MIN_WITHDRAW = 1000

DEFAULT_CLICKS_LIMIT = 2000  # ✅ было 1500 -> стало 2000
CLICK_RESET_HOURS = 3
REF_REWARD = 150

DAILY_BONUS_AMOUNT = 500
DAILY_BONUS_HOURS = 24

VIP_LIMITS = {"VIP": 3000, "MVP": 3500, "PREMIUM": 4500}
VIP_ICONS = {"VIP": "🏆", "MVP": "💎", "PREMIUM": "💲"}
VIP_FRAMES = {"VIP": "💎", "MVP": "🏆", "PREMIUM": "🔥"}  # ✅ рамка всегда главнее

COSMETIC_COOLDOWN_SECONDS = 10

# =========================
# ===== КОСМЕТИКА =========
# =========================
# Титулы (код -> отображение)
TITLE_DISPLAY = {
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

    # прогресс-титулы
    "MASTER_CLICK": "Master Click",
    "ELITE_CLICKER": "Elite Clicker",
    "ULTRA_CLICKER": "Ultra Clicker",
    "IMPOSSIBLE_CLICKER": "Impossible Clicker",
}

# Пороги прогресс-титулов: (клики, code)
PROGRESS_TITLES = [
    (0, "ROOKIE"),
    (5000, "MASTER_CLICK"),
    (8000, "ELITE_CLICKER"),
    (13000, "ULTRA_CLICKER"),
    (20000, "IMPOSSIBLE_CLICKER"),
]

# Фоны (код -> (emoji, display, price))
THEMES = {
    # цены твои:
    "FIRE": ("🔥", "Огненный", 1200),
    "DARK": ("🌑", "Тёмный", 1700),
    "CRYSTAL": ("💎", "Кристальный", 2300),
    "ICE": ("❄️", "Ледяной", 2300),
    "NEWYEAR": ("🎄", "Новогодний", 2700),
    "CHOC": ("🍫", "Шоколадный", 3000),
    "TOP": ("⭐️", "Топовый", 4000),
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
    # users
    db_exec(
        """
        CREATE TABLE IF NOT EXISTS users (
            id BIGINT PRIMARY KEY,
            balance DOUBLE PRECISION DEFAULT 0,
            banned INTEGER DEFAULT 0,
            clicks_used INTEGER DEFAULT 0,
            clicks_limit INTEGER DEFAULT 2000,
            last_click_reset TEXT,
            subscribed INTEGER DEFAULT 0
        )
        """
    )

    # referrals
    db_exec(
        """
        CREATE TABLE IF NOT EXISTS referrals (
            user_id BIGINT PRIMARY KEY,
            referrer_id BIGINT,
            rewarded INTEGER DEFAULT 0
        )
        """
    )

    # withdrawals
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

    # promocodes
    db_exec(
        """
        CREATE TABLE IF NOT EXISTS promocodes (
            code TEXT PRIMARY KEY,
            amount DOUBLE PRECISION,
            uses_left INTEGER DEFAULT 1
        )
        """
    )

    # used promocodes
    db_exec(
        """
        CREATE TABLE IF NOT EXISTS used_promocodes (
            user_id BIGINT,
            code TEXT,
            PRIMARY KEY(user_id, code)
        )
        """
    )

    # cosmetics inventory
    db_exec(
        """
        CREATE TABLE IF NOT EXISTS titles_owned (
            user_id BIGINT,
            code TEXT,
            expires_at TEXT DEFAULT NULL,
            PRIMARY KEY(user_id, code)
        )
        """
    )

    db_exec(
        """
        CREATE TABLE IF NOT EXISTS themes_owned (
            user_id BIGINT,
            code TEXT,
            expires_at TEXT DEFAULT NULL,
            PRIMARY KEY(user_id, code)
        )
        """
    )

    # ---- миграции: добавляем колонки, НЕ ТРОГАЕМ старые данные
    db_exec(
        """
        DO $$
        BEGIN
            -- VIP
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='vip_type')
                THEN ALTER TABLE users ADD COLUMN vip_type TEXT DEFAULT NULL;
            END IF;

            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='vip_until')
                THEN ALTER TABLE users ADD COLUMN vip_until TEXT DEFAULT NULL;
            END IF;

            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='vip_base_limit')
                THEN ALTER TABLE users ADD COLUMN vip_base_limit INTEGER DEFAULT NULL;
            END IF;

            -- total clicks
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='total_clicks')
                THEN ALTER TABLE users ADD COLUMN total_clicks BIGINT DEFAULT 0;
            END IF;

            -- username
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='username')
                THEN ALTER TABLE users ADD COLUMN username TEXT DEFAULT NULL;
            END IF;

            -- daily bonus
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='last_daily_bonus')
                THEN ALTER TABLE users ADD COLUMN last_daily_bonus TEXT DEFAULT NULL;
            END IF;

            -- ref bonuses flags
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='ref_bonus_10')
                THEN ALTER TABLE users ADD COLUMN ref_bonus_10 INTEGER DEFAULT 0;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='ref_bonus_50')
                THEN ALTER TABLE users ADD COLUMN ref_bonus_50 INTEGER DEFAULT 0;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='ref_bonus_100')
                THEN ALTER TABLE users ADD COLUMN ref_bonus_100 INTEGER DEFAULT 0;
            END IF;

            -- withdrawals admin fields
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='withdrawals' AND column_name='admin_note')
                THEN ALTER TABLE withdrawals ADD COLUMN admin_note TEXT DEFAULT NULL;
            END IF;

            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='withdrawals' AND column_name='decided_at')
                THEN ALTER TABLE withdrawals ADD COLUMN decided_at TEXT DEFAULT NULL;
            END IF;

            -- cosmetics: active selections + cooldown
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='active_title')
                THEN ALTER TABLE users ADD COLUMN active_title TEXT DEFAULT 'ROOKIE';
            END IF;

            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='active_theme')
                THEN ALTER TABLE users ADD COLUMN active_theme TEXT DEFAULT NULL;
            END IF;

            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='last_cosmetic_change')
                THEN ALTER TABLE users ADD COLUMN last_cosmetic_change TEXT DEFAULT NULL;
            END IF;
        END $$;
        """
    )


def ensure_user(user_id: int, username: Optional[str] = None):
    db_exec("INSERT INTO users (id) VALUES (%s) ON CONFLICT (id) DO NOTHING", (user_id,))
    if username:
        db_exec("UPDATE users SET username=%s WHERE id=%s", (username, user_id))


# =========================
# ===== МЕНЮ ==============
# =========================
def main_menu(user_id: int):
    buttons = [
        ["👤 Профиль", "💰 Заработать"],
        ["👥 Рефералка", "💸 Вывод"],
        ["🎁 Ввести промокод"],
    ]
    if user_id == ADMIN_ID:
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
            ["Рассылка", "📋 Заявки на вывод"],
            ["Все промокоды", "🔙 Назад"],
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
            [InlineKeyboardButton("🎨 Косметика", callback_data="cosmetics")],
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


def cosmetics_inline_menu():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🏷 Титул", callback_data="cos_title")],
            [InlineKeyboardButton("🌌 Фон", callback_data="cos_theme")],
            [InlineKeyboardButton("🛒 Магазин фонов", callback_data="cos_shop")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_profile")],
        ]
    )


def back_to_cosmetics_menu():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="cosmetics")]])


def ref_bonuses_inline_menu(user_id: int, ref_count: int, claimed10: int, claimed50: int, claimed100: int):
    buttons = []
    if claimed10:
        buttons.append([InlineKeyboardButton("✅ 10 рефов — получено", callback_data="noop")])
    else:
        buttons.append([InlineKeyboardButton("🎁 Забрать за 10 рефов", callback_data="claim_ref_10")])

    if claimed50:
        buttons.append([InlineKeyboardButton("✅ 50 рефов — получено", callback_data="noop")])
    else:
        buttons.append([InlineKeyboardButton("🎁 Забрать за 50 рефов", callback_data="claim_ref_50")])

    if claimed100:
        buttons.append([InlineKeyboardButton("✅ 100 рефов — получено", callback_data="noop")])
    else:
        buttons.append([InlineKeyboardButton("🎁 Забрать за 100 рефов", callback_data="claim_ref_100")])

    buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_profile")])
    return InlineKeyboardMarkup(buttons)


# =========================
# ===== ВСПОМОГАТЕЛЬНОЕ ===
# =========================
def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def now_human():
    return datetime.now().strftime("%d.%m.%Y %H:%M")


def format_time_left(td: timedelta):
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


async def safe_send_message(message, text: str, reply_markup=None, parse_mode: Optional[str] = None, disable_preview: bool = True):
    try:
        return await message.reply_text(
            text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_preview,
        )
    except TimedOut:
        try:
            return await message.reply_text(
                text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_preview,
            )
        except Exception as e:
            logger.warning(f"safe_send_message second try failed: {e}")
    except Exception as e:
        logger.warning(f"safe_send_message failed: {e}")


async def safe_reply(update: Update, text: str, reply_markup=None, parse_mode: Optional[str] = None, disable_preview: bool = True):
    if not update.message:
        return
    return await safe_send_message(update.message, text, reply_markup=reply_markup, parse_mode=parse_mode, disable_preview=disable_preview)


async def is_subscribed(bot, user_id: int):
    try:
        member = await bot.get_chat_member(CHANNEL_ID, user_id)
        return member.status in ["member", "administrator", "creator"]
    except Exception:
        return False


def check_click_reset(user_id: int):
    row = db_fetchone("SELECT last_click_reset, clicks_used, clicks_limit FROM users WHERE id=%s", (user_id,))
    now = datetime.now()

    if not row or row[0] is None:
        db_exec(
            "UPDATE users SET last_click_reset=%s, clicks_used=0 WHERE id=%s",
            (now.strftime("%Y-%m-%d %H:%M:%S"), user_id),
        )
        return 0, now + timedelta(hours=CLICK_RESET_HOURS), DEFAULT_CLICKS_LIMIT

    last_reset = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
    next_reset = last_reset + timedelta(hours=CLICK_RESET_HOURS)

    if now >= next_reset:
        db_exec(
            "UPDATE users SET last_click_reset=%s, clicks_used=0 WHERE id=%s",
            (now.strftime("%Y-%m-%d %H:%M:%S"), user_id),
        )
        return 0, now + timedelta(hours=CLICK_RESET_HOURS), row[2]

    return row[1], next_reset, row[2]


def parse_duration(value: str, unit: str):
    v = int(value)
    u = unit.lower()

    # нормализуем рус/сокращения
    if u in ("м", "мин", "минута", "минуты", "минут", "минутка"):
        return timedelta(minutes=v)
    if u in ("ч", "час", "часа", "часов"):
        return timedelta(hours=v)
    if u in ("д", "дн", "день", "дня", "дней"):
        return timedelta(days=v)

    # старые проверки
    if u.startswith("мин"):
        return timedelta(minutes=v)
    if u.startswith("час"):
        return timedelta(hours=v)
    if u.startswith("дн") or u.startswith("ден") or u.startswith("дня"):
        return timedelta(days=v)

    return None


def parse_duration_token(token: str) -> Optional[timedelta]:
    """
    Поддержка: 300м / 12ч / 2д
    """
    t = token.strip().lower()
    if not t:
        return None

    # Infinity обрабатываем отдельно
    if t in ("infinity", "inf", "∞", "♾️", "♾"):
        return None

    # короткий формат
    if t[-1] in ("м", "ч", "д"):
        num = t[:-1]
        if not num.isdigit():
            return None
        v = int(num)
        if t[-1] == "м":
            return timedelta(minutes=v)
        if t[-1] == "ч":
            return timedelta(hours=v)
        if t[-1] == "д":
            return timedelta(days=v)

    return None


def parse_admin_time(parts: List[str], start_index: int) -> Tuple[bool, Optional[timedelta], str]:
    """
    Возвращает (is_infinity, duration, shown)
    Поддержка:
      - Infinity
      - 300м/12ч/2д
      - 300 минут / 1 час / 2 дня
    """
    if len(parts) <= start_index:
        return False, None, ""

    tok = parts[start_index].strip()
    if tok.lower() in ("infinity", "inf", "∞", "♾️", "♾"):
        return True, None, "Infinity"

    # коротко: 12ч/300м/2д
    d = parse_duration_token(tok)
    if d is not None:
        return False, d, tok

    # полно: value unit
    if len(parts) > start_index + 1:
        v = parts[start_index]
        u = parts[start_index + 1]
        if v.isdigit():
            d2 = parse_duration(v, u)
            if d2:
                return False, d2, f"{v} {u}"

    return False, None, ""


def check_and_update_vip(user_id: int):
    row = db_fetchone("SELECT vip_type, vip_until, vip_base_limit FROM users WHERE id=%s", (user_id,))
    if not row:
        return None, None

    vip_type, vip_until, vip_base_limit = row
    if not vip_type or not vip_until:
        return None, None

    try:
        until_dt = datetime.fromisoformat(vip_until)
    except Exception:
        db_exec("UPDATE users SET vip_type=NULL, vip_until=NULL, vip_base_limit=NULL WHERE id=%s", (user_id,))
        return None, None

    now = datetime.now()
    if now >= until_dt:
        restore_limit = vip_base_limit if vip_base_limit is not None else DEFAULT_CLICKS_LIMIT
        db_exec(
            "UPDATE users SET vip_type=NULL, vip_until=NULL, vip_base_limit=NULL, clicks_limit=%s WHERE id=%s",
            (restore_limit, user_id),
        )
        return None, None

    return vip_type, until_dt


def title_name(code: Optional[str]) -> str:
    if not code:
        return TITLE_DISPLAY.get("ROOKIE", "Rookie")
    return TITLE_DISPLAY.get(code, code)


def cleanup_expired_cosmetics(user_id: int):
    # titles
    rows = db_fetchall("SELECT code, expires_at FROM titles_owned WHERE user_id=%s", (user_id,))
    now = datetime.now()
    for code, exp in rows:
        if exp:
            try:
                dt = datetime.fromisoformat(exp)
                if now >= dt:
                    db_exec("DELETE FROM titles_owned WHERE user_id=%s AND code=%s", (user_id, code))
            except Exception:
                pass

    # themes
    rows = db_fetchall("SELECT code, expires_at FROM themes_owned WHERE user_id=%s", (user_id,))
    for code, exp in rows:
        if exp:
            try:
                dt = datetime.fromisoformat(exp)
                if now >= dt:
                    db_exec("DELETE FROM themes_owned WHERE user_id=%s AND code=%s", (user_id, code))
            except Exception:
                pass


def user_has_title(user_id: int, code: str) -> bool:
    if code == "ROOKIE":
        return True
    row = db_fetchone("SELECT 1 FROM titles_owned WHERE user_id=%s AND code=%s", (user_id, code))
    return bool(row)


def user_has_theme(user_id: int, code: str) -> bool:
    row = db_fetchone("SELECT 1 FROM themes_owned WHERE user_id=%s AND code=%s", (user_id, code))
    return bool(row)


def grant_title(user_id: int, code: str, expires_at: Optional[str] = None):
    if code == "ROOKIE":
        return
    db_exec(
        """
        INSERT INTO titles_owned (user_id, code, expires_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (user_id, code) DO UPDATE SET expires_at=EXCLUDED.expires_at
        """,
        (user_id, code, expires_at),
    )


def grant_theme(user_id: int, code: str, expires_at: Optional[str] = None):
    db_exec(
        """
        INSERT INTO themes_owned (user_id, code, expires_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (user_id, code) DO UPDATE SET expires_at=EXCLUDED.expires_at
        """,
        (user_id, code, expires_at),
    )


def grant_progress_titles(user_id: int, total_clicks: int) -> List[str]:
    """
    Выдаёт прогресс-титулы автоматически. Возвращает список НОВЫХ выданных кодов (кроме ROOKIE).
    """
    newly = []
    for threshold, code in PROGRESS_TITLES:
        if total_clicks >= threshold and code != "ROOKIE":
            if not user_has_title(user_id, code):
                grant_title(user_id, code, expires_at=None)
                newly.append(code)
    # убедимся, что active_title не пустой
    row = db_fetchone("SELECT active_title FROM users WHERE id=%s", (user_id,))
    if row and not row[0]:
        db_exec("UPDATE users SET active_title='ROOKIE' WHERE id=%s", (user_id,))
    return newly


def get_effective_active_title(user_id: int) -> str:
    cleanup_expired_cosmetics(user_id)
    row = db_fetchone("SELECT active_title FROM users WHERE id=%s", (user_id,))
    active = row[0] if row and row[0] else "ROOKIE"
    if active != "ROOKIE" and not user_has_title(user_id, active):
        active = "ROOKIE"
        db_exec("UPDATE users SET active_title=%s WHERE id=%s", (active, user_id))
    return active


def get_effective_active_theme(user_id: int) -> Optional[str]:
    cleanup_expired_cosmetics(user_id)
    row = db_fetchone("SELECT active_theme FROM users WHERE id=%s", (user_id,))
    active = row[0] if row else None
    if active and not user_has_theme(user_id, active):
        active = None
        db_exec("UPDATE users SET active_theme=NULL WHERE id=%s", (user_id,))
    return active


def can_change_cosmetic(user_id: int) -> Tuple[bool, int]:
    row = db_fetchone("SELECT last_cosmetic_change FROM users WHERE id=%s", (user_id,))
    if not row or not row[0]:
        return True, 0
    try:
        last = datetime.fromisoformat(row[0])
    except Exception:
        return True, 0
    now = datetime.now()
    diff = (now - last).total_seconds()
    if diff >= COSMETIC_COOLDOWN_SECONDS:
        return True, 0
    return False, int(COSMETIC_COOLDOWN_SECONDS - diff)


def touch_cosmetic_change(user_id: int):
    db_exec("UPDATE users SET last_cosmetic_change=%s WHERE id=%s", (now_iso(), user_id))


def format_user_link_html(user_id: int, username: Optional[str]) -> str:
    """
    Если username есть -> @username
    Если нет -> кликабельный ID (tg://user?id=)
    """
    if username:
        return f"@{username}"
    return f'<a href="tg://user?id={user_id}">{user_id}</a>'


def build_profile_header(vip_type: Optional[str], theme_code: Optional[str]) -> str:
    frame = VIP_FRAMES.get(vip_type, "")
    theme_emoji = THEMES.get(theme_code, ("", "", 0))[0] if theme_code else ""

    # базовая линия
    if theme_emoji:
        inner = f"{theme_emoji}━━━━━━━━ ПРОФИЛЬ ━━━━━━━━{theme_emoji}"
    else:
        inner = "━━━━━━ ПРОФИЛЬ ━━━━━━"

    if frame and theme_emoji:
        return f"{frame}{inner}{frame}"
    if frame and not theme_emoji:
        return f"{frame}{inner}{frame}"
    return inner


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


def can_take_daily(last_daily_bonus: Optional[str]) -> tuple[bool, Optional[timedelta]]:
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
# ===== INLINE HANDLER =====
# =========================
async def inline_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    user_id = q.from_user.id
    username = q.from_user.username
    ensure_user(user_id, username=username)

    data = q.data or ""

    # BACK to profile
    if data == "back_profile":
        await send_profile_from_message(q.message, context, user_id)
        return

    if data == "noop":
        return

    # daily bonus
    if data == "daily_bonus":
        row = db_fetchone("SELECT last_daily_bonus FROM users WHERE id=%s", (user_id,))
        last_daily = row[0] if row else None

        ok, left = can_take_daily(last_daily)
        if not ok and left is not None:
            await safe_send_message(
                q.message,
                f"⏳ Ежедневный бонус уже был.\nСледующий через: {format_time_left(left)}",
            )
            return

        db_exec(
            "UPDATE users SET balance=balance+%s, last_daily_bonus=%s WHERE id=%s",
            (DAILY_BONUS_AMOUNT, now_iso(), user_id),
        )
        await safe_send_message(q.message, f"✅ Ежедневный бонус получен: +{DAILY_BONUS_AMOUNT} GOLD 🎁")
        return

    # open tops menu
    if data == "tops":
        await safe_send_message(q.message, "🏆 Выберите ТОП:", reply_markup=tops_inline_menu())
        return

    # top clicks
    if data == "top_clicks":
        rows = db_fetchall(
            """
            SELECT id, username, COALESCE(total_clicks,0) AS tc, COALESCE(active_title,'ROOKIE') AS t
            FROM users
            ORDER BY tc DESC, id ASC
            LIMIT 10
            """
        )
        msg = "📊 ТОП по кликам (всего)\n\n"
        if not rows:
            msg += "Пока пусто."
        else:
            for i, (uid, uname, tc, t) in enumerate(rows, start=1):
                tname = title_name(t)
                ulink = format_user_link_html(uid, uname)
                msg += f"{i}) [{tname}] {ulink} — {int(tc)} кликов\n"
        await safe_send_message(q.message, msg, reply_markup=tops_inline_menu(), parse_mode="HTML")
        return

    # top balance
    if data == "top_balance":
        rows = db_fetchall(
            """
            SELECT id, username, balance, COALESCE(active_title,'ROOKIE') AS t
            FROM users
            ORDER BY balance DESC, id ASC
            LIMIT 10
            """
        )
        msg = "💰 ТОП по балансу\n\n"
        if not rows:
            msg += "Пока пусто."
        else:
            for i, (uid, uname, bal, t) in enumerate(rows, start=1):
                tname = title_name(t)
                ulink = format_user_link_html(uid, uname)
                msg += f"{i}) [{tname}] {ulink} — {round(float(bal), 2)} GOLD\n"
        await safe_send_message(q.message, msg, reply_markup=tops_inline_menu(), parse_mode="HTML")
        return

    # top refs
    if data == "top_refs":
        rows = db_fetchall(
            """
            SELECT r.referrer_id, u.username, COALESCE(u.active_title,'ROOKIE') AS t, COUNT(*) AS c
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
            for i, (ref_uid, ref_uname, t, c) in enumerate(rows, start=1):
                tname = title_name(t)
                ulink = format_user_link_html(ref_uid, ref_uname)
                msg += f"{i}) [{tname}] {ulink} — {int(c)} рефералов\n"
        await safe_send_message(q.message, msg, reply_markup=tops_inline_menu(), parse_mode="HTML")
        return

    # open ref bonuses menu
    if data == "ref_bonuses":
        await send_ref_bonus_menu(q.message, context, user_id)
        return

    # claim ref bonus
    if data.startswith("claim_ref_"):
        await process_claim_ref_bonus(q.message, context, user_id, data)
        return

    # cosmetics
    if data == "cosmetics":
        await safe_send_message(
            q.message,
            "🎨 Косметика\n\nВыбери раздел:",
            reply_markup=cosmetics_inline_menu(),
        )
        return

    if data == "cos_title":
        await show_titles_menu(q.message, context, user_id)
        return

    if data == "cos_theme":
        await show_themes_menu(q.message, context, user_id)
        return

    if data == "cos_shop":
        await show_theme_shop(q.message, context, user_id)
        return

    # set title
    if data.startswith("set_title:"):
        code = data.split(":", 1)[1]
        await set_user_title(q.message, context, user_id, code)
        return

    # set theme
    if data.startswith("set_theme:"):
        code = data.split(":", 1)[1]
        await set_user_theme(q.message, context, user_id, code)
        return

    # buy theme
    if data.startswith("buy_theme:"):
        code = data.split(":", 1)[1]
        await buy_theme(q.message, context, user_id, code)
        return


async def show_titles_menu(message, context, user_id: int):
    cleanup_expired_cosmetics(user_id)

    # выдаём прогресс-титулы на всякий (если человек давно не кликал)
    row = db_fetchone("SELECT COALESCE(total_clicks,0) FROM users WHERE id=%s", (user_id,))
    tc = int(row[0]) if row else 0
    grant_progress_titles(user_id, tc)

    active = get_effective_active_title(user_id)

    owned = db_fetchall("SELECT code FROM titles_owned WHERE user_id=%s ORDER BY code ASC", (user_id,))
    owned_codes = [c for (c,) in owned]
    if "ROOKIE" not in owned_codes:
        owned_codes = ["ROOKIE"] + owned_codes

    # кнопки
    buttons = []
    for code in owned_codes[:15]:
        disp = TITLE_DISPLAY.get(code, code)
        mark = "✅ " if code == active else ""
        buttons.append([InlineKeyboardButton(f"{mark}{disp}", callback_data=f"set_title:{code}")])

    buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="cosmetics")])

    await safe_send_message(
        message,
        f"🏷 Титулы\n\nАктивный: [{title_name(active)}]\n\nВыбери титул:",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def show_themes_menu(message, context, user_id: int):
    cleanup_expired_cosmetics(user_id)
    active = get_effective_active_theme(user_id)

    owned = db_fetchall("SELECT code FROM themes_owned WHERE user_id=%s ORDER BY code ASC", (user_id,))
    owned_codes = [c for (c,) in owned]

    buttons = []
    if not owned_codes:
        await safe_send_message(
            message,
            "🌌 Фоны\n\nУ тебя пока нет фонов.\nОткрой магазин и купи фон 👇",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🛒 Магазин фонов", callback_data="cos_shop")],
                    [InlineKeyboardButton("⬅️ Назад", callback_data="cosmetics")],
                ]
            ),
        )
        return

    # кнопка снять фон
    mark_none = "✅ " if not active else ""
    buttons.append([InlineKeyboardButton(f"{mark_none}Без фона", callback_data="set_theme:NONE")])

    for code in owned_codes[:15]:
        em, name, _ = THEMES.get(code, ("", code, 0))
        mark = "✅ " if code == active else ""
        buttons.append([InlineKeyboardButton(f"{mark}{em} {name}", callback_data=f"set_theme:{code}")])

    buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="cosmetics")])

    active_text = "нет" if not active else f"{THEMES.get(active, ('', active, 0))[0]} {THEMES.get(active, ('', active, 0))[1]}"
    await safe_send_message(
        message,
        f"🌌 Фоны\n\nАктивный: {active_text}\n\nВыбери фон:",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def show_theme_shop(message, context, user_id: int):
    cleanup_expired_cosmetics(user_id)
    row = db_fetchone("SELECT balance FROM users WHERE id=%s", (user_id,))
    bal = float(row[0]) if row else 0.0

    text = "🛒 Магазин фонов\n\n"
    text += f"💰 Баланс: {round(bal, 2)} GOLD\n\n"
    text += "Выбери фон для покупки (по 1):"

    buttons = []
    for code, (em, name, price) in THEMES.items():
        owned = user_has_theme(user_id, code)
        label = f"{em} {name} — {price}G"
        if owned:
            label = f"✅ {label}"
            buttons.append([InlineKeyboardButton(label, callback_data="noop")])
        else:
            buttons.append([InlineKeyboardButton(label, callback_data=f"buy_theme:{code}")])

    buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="cosmetics")])

    await safe_send_message(message, text, reply_markup=InlineKeyboardMarkup(buttons))


async def set_user_title(message, context, user_id: int, code: str):
    if code == "NONE":
        code = "ROOKIE"

    cleanup_expired_cosmetics(user_id)
    if code != "ROOKIE" and not user_has_title(user_id, code):
        await safe_send_message(message, "❌ У тебя нет такого титула.", reply_markup=back_to_cosmetics_menu())
        return

    ok, left = can_change_cosmetic(user_id)
    if not ok:
        await safe_send_message(message, f"⏳ Подожди {left} сек перед сменой косметики.", reply_markup=back_to_cosmetics_menu())
        return

    db_exec("UPDATE users SET active_title=%s WHERE id=%s", (code, user_id))
    touch_cosmetic_change(user_id)

    await safe_send_message(message, f"✅ Титул установлен: [{title_name(code)}]", reply_markup=back_to_cosmetics_menu())


async def set_user_theme(message, context, user_id: int, code: str):
    cleanup_expired_cosmetics(user_id)

    if code == "NONE":
        ok, left = can_change_cosmetic(user_id)
        if not ok:
            await safe_send_message(message, f"⏳ Подожди {left} сек перед сменой косметики.", reply_markup=back_to_cosmetics_menu())
            return
        db_exec("UPDATE users SET active_theme=NULL WHERE id=%s", (user_id,))
        touch_cosmetic_change(user_id)
        await safe_send_message(message, "✅ Фон снят.", reply_markup=back_to_cosmetics_menu())
        return

    if code not in THEMES:
        await safe_send_message(message, "❌ Неизвестный фон.", reply_markup=back_to_cosmetics_menu())
        return

    if not user_has_theme(user_id, code):
        await safe_send_message(message, "❌ У тебя нет этого фона.", reply_markup=back_to_cosmetics_menu())
        return

    ok, left = can_change_cosmetic(user_id)
    if not ok:
        await safe_send_message(message, f"⏳ Подожди {left} сек перед сменой косметики.", reply_markup=back_to_cosmetics_menu())
        return

    db_exec("UPDATE users SET active_theme=%s WHERE id=%s", (code, user_id))
    touch_cosmetic_change(user_id)

    em, name, _ = THEMES[code]
    await safe_send_message(message, f"✅ Фон установлен: {em} {name}", reply_markup=back_to_cosmetics_menu())


async def buy_theme(message, context, user_id: int, code: str):
    if code not in THEMES:
        await safe_send_message(message, "❌ Неизвестный фон.", reply_markup=back_to_cosmetics_menu())
        return

    cleanup_expired_cosmetics(user_id)
    if user_has_theme(user_id, code):
        await safe_send_message(message, "✅ У тебя уже есть этот фон.", reply_markup=back_to_cosmetics_menu())
        return

    em, name, price = THEMES[code]
    row = db_fetchone("SELECT balance FROM users WHERE id=%s", (user_id,))
    bal = float(row[0]) if row else 0.0

    if bal < price:
        await safe_send_message(message, f"❌ Не хватает GOLD. Нужно: {price}G", reply_markup=back_to_cosmetics_menu())
        return

    db_exec("UPDATE users SET balance=balance-%s WHERE id=%s", (price, user_id))
    grant_theme(user_id, code, expires_at=None)

    await safe_send_message(message, f"✅ Куплено: {em} {name} за {price}G", reply_markup=back_to_cosmetics_menu())


# =========================
# ===== ПРОФИЛЬ/РЕФ-БОНУСЫ
# =========================
async def send_profile_from_message(message, context, user_id: int):
    vip_type, vip_until_dt = check_and_update_vip(user_id)

    # подстрахуем лимит у старых записей
    db_exec("UPDATE users SET clicks_limit=%s WHERE clicks_limit=1500", (DEFAULT_CLICKS_LIMIT,))

    row = db_fetchone(
        "SELECT balance, clicks_used, clicks_limit, COALESCE(total_clicks,0), username FROM users WHERE id=%s",
        (user_id,),
    )
    if row:
        bal, used_now, limit_now, total_clicks, stored_username = row
    else:
        bal, used_now, limit_now, total_clicks, stored_username = (0, 0, DEFAULT_CLICKS_LIMIT, 0, None)

    # прогресс-титулы
    grant_progress_titles(user_id, int(total_clicks))

    used, next_reset, limit = check_click_reset(user_id)

    active_title_code = get_effective_active_title(user_id)
    active_theme_code = get_effective_active_theme(user_id)

    header = build_profile_header(vip_type, active_theme_code)

    ulink = format_user_link_html(user_id, stored_username)
    title_text = title_name(active_title_code)
    vip_status_text = vip_type if vip_type else "нет"
    vip_left_text = format_time_left(vip_until_dt - datetime.now()) if vip_until_dt else "нет VIP статуса"

    # ник + иконка VIP (как раньше)
    vip_icon = VIP_ICONS.get(vip_type, "") if vip_type else ""
    nick_line = f"[{title_text}] {ulink}{vip_icon}"

    await safe_send_message(
        message,
        f"{header}\n"
        f"{nick_line}\n"
        f"VIP статус: {vip_status_text}\n"
        f"Срок VIP статуса: {vip_left_text}\n\n"
        f"💰 Баланс: {round(float(bal), 2)} GOLD\n"
        f"📊 Клики (за период): {used}/{limit}\n"
        f"🏁 Клики (всего): {int(total_clicks)}\n"
        f"⏳ До обновления: {format_time_left(next_reset - datetime.now())}",
        reply_markup=profile_inline_menu(),
        parse_mode="HTML",
    )


async def send_ref_bonus_menu(message, context, user_id: int):
    ref_count = get_subscribed_ref_count(user_id)

    row = db_fetchone(
        "SELECT ref_bonus_10, ref_bonus_50, ref_bonus_100 FROM users WHERE id=%s",
        (user_id,),
    )
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
    await safe_send_message(
        message,
        text,
        reply_markup=ref_bonuses_inline_menu(user_id, ref_count, claimed10, claimed50, claimed100),
    )


async def process_claim_ref_bonus(message, context, user_id: int, data: str):
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
        await safe_send_message(message, "✅ Ты уже забрал эту награду.")
        return

    if ref_count < need:
        await safe_send_message(message, f"❌ Нужно {need} подписанных рефералов. Сейчас: {ref_count}")
        return

    db_exec(f"UPDATE users SET balance=balance+%s, {col}=1 WHERE id=%s", (reward, user_id))
    await safe_send_message(message, f"🎉 Награда получена: +{reward} GOLD ✅")
    await send_ref_bonus_menu(message, context, user_id)


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
    check_and_update_vip(user_id)

    # бан (кроме админа)
    if user_id != ADMIN_ID:
        r = db_fetchone("SELECT banned FROM users WHERE id=%s", (user_id,))
        if r and int(r[0]) == 1:
            await safe_reply(update, "⛔ Вы заблокированы.")
            return

    # НАЗАД / ОТМЕНА
    if text in ["🔙 Назад", "❌ Отмена"]:
        if user_id == ADMIN_ID and context.user_data.get("admin_action"):
            context.user_data.pop("admin_action", None)
            context.user_data["menu"] = "admin"
            await safe_reply(update, "Действие отменено", reply_markup=admin_menu())
            return

        context.user_data.clear()
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
        await send_profile_from_message(update.message, context, user_id)
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

        # обновляем и получаем total_clicks
        db_exec(
            """
            UPDATE users
            SET balance=balance+%s,
                clicks_used=clicks_used+1,
                total_clicks=COALESCE(total_clicks,0)+1
            WHERE id=%s
            """,
            (CLICK_REWARD, user_id),
        )

        row = db_fetchone("SELECT COALESCE(total_clicks,0) FROM users WHERE id=%s", (user_id,))
        total_clicks = int(row[0]) if row else 0

        # прогресс-титулы (если новый — покажем)
        new_titles = grant_progress_titles(user_id, total_clicks)

        used += 1
        msg = f"✅ Заработано {CLICK_REWARD} GOLD ({used}/{limit})"
        if new_titles:
            # покажем самый свежий
            last_code = new_titles[-1]
            msg += f"\n🎉 Открыт новый титул: [{TITLE_DISPLAY.get(last_code, last_code)}] (выбери в 🎨 Косметика)"

        await safe_reply(update, msg, reply_markup=earn_menu())
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

    # =======================
    # ======= АДМИНКА =======
    # =======================
    menu = context.user_data.get("menu")
    admin_action = context.user_data.get("admin_action")

    if text == "🛠 Админка":
        if user_id != ADMIN_ID:
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

    if user_id == ADMIN_ID:
        handled = await admin_process_withdraw_decision(update, context, text)
        if handled:
            return

    if user_id == ADMIN_ID and menu == "admin" and admin_action is None:
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
            context.user_data["admin_action"] = "set_click_limit"
            await safe_reply(update, "ID НовыйЛимит\nПример: 123456789 3000", reply_markup=cancel_menu())
            return

        if text == "🎖 Выдать привилегию":
            context.user_data["admin_action"] = "give_vip"
            await safe_reply(
                update,
                "Формат:\n"
                "ID VIP 1 час\n"
                "ID MVP 300 минут\n"
                "ID PREMIUM 2 дня\n"
                "или коротко: 12ч / 300м / 2д\n"
                "Infinity — навсегда",
                reply_markup=cancel_menu(),
            )
            return

        if text == "🏷 Выдать титул":
            context.user_data["admin_action"] = "give_title"
            await safe_reply(
                update,
                "Формат:\n"
                "ID TITLE_CODE 7д\n"
                "ID TITLE_CODE 12ч\n"
                "ID TITLE_CODE 300м\n"
                "ID TITLE_CODE Infinity\n\n"
                "Пример: 123456789 LEGEND Infinity",
                reply_markup=cancel_menu(),
            )
            return

        if text == "🌌 Выдать фон":
            context.user_data["admin_action"] = "give_theme"
            await safe_reply(
                update,
                "Формат:\n"
                "ID THEME_CODE 7д\n"
                "ID THEME_CODE 12ч\n"
                "ID THEME_CODE 300м\n"
                "ID THEME_CODE Infinity\n\n"
                "Коды: FIRE, DARK, CRYSTAL, ICE, NEWYEAR, CHOC, TOP\n"
                "Пример: 123456789 TOP Infinity",
                reply_markup=cancel_menu(),
            )
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

    if user_id == ADMIN_ID and admin_action:
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

            elif admin_action == "set_click_limit":
                if len(parts) != 2:
                    await safe_reply(update, "❌ Формат: ID НОВЫЙ_ЛИМИТ", reply_markup=cancel_menu())
                    return
                uid, limit = int(parts[0]), int(parts[1])
                ensure_user(uid)
                db_exec("UPDATE users SET clicks_limit=%s WHERE id=%s", (limit, uid))
                await safe_reply(update, f"✅ Лимит кликов для {uid} = {limit}", reply_markup=admin_menu())

            elif admin_action == "give_vip":
                # Поддержка:
                # ID VIP 1 час
                # ID MVP 300 минут
                # ID PREMIUM 2 дня
                # коротко: 12ч / 300м / 2д
                # Infinity
                if len(parts) < 3:
                    await safe_reply(update, "❌ Формат:\nID VIP 1 час\nID MVP 300 минут\nID PREMIUM 2 дня\nили 12ч/300м/2д\nInfinity", reply_markup=cancel_menu())
                    return

                uid = int(parts[0])
                vip = parts[1].upper()
                if vip not in VIP_LIMITS:
                    await safe_reply(update, "❌ Привилегия только: VIP / MVP / PREMIUM", reply_markup=cancel_menu())
                    return

                is_inf, dur, shown = parse_admin_time(parts, 2)
                if is_inf:
                    # навсегда -> ставим далеко-далеко
                    until = datetime.now() + timedelta(days=3650)
                    shown = "Infinity"
                else:
                    if dur is None:
                        await safe_reply(update, "❌ Время: 300 минут / 1 час / 2 дня или 300м/1ч/2д", reply_markup=cancel_menu())
                        return
                    until = datetime.now() + dur

                ensure_user(uid)
                row = db_fetchone("SELECT clicks_limit FROM users WHERE id=%s", (uid,))
                current_limit = int(row[0]) if row else DEFAULT_CLICKS_LIMIT

                new_limit = VIP_LIMITS[vip]

                db_exec(
                    "UPDATE users SET vip_type=%s, vip_until=%s, vip_base_limit=%s, clicks_limit=%s WHERE id=%s",
                    (vip, until.isoformat(), current_limit, new_limit, uid),
                )
                await safe_reply(update, f"✅ Привилегия выдана {uid}: {vip} ({shown})", reply_markup=admin_menu())

            elif admin_action == "give_title":
                # Формат: ID CODE 7д/12ч/300м/Infinity
                if len(parts) < 3:
                    await safe_reply(update, "❌ Формат: ID TITLE_CODE 7д/12ч/300м/Infinity\nПример: 123 LEGEND Infinity", reply_markup=cancel_menu())
                    return

                uid = int(parts[0])
                code = parts[1].upper()

                if code not in TITLE_DISPLAY:
                    await safe_reply(update, "❌ Неизвестный TITLE_CODE", reply_markup=cancel_menu())
                    return

                is_inf, dur, shown = parse_admin_time(parts, 2)
                expires_at = None
                if is_inf:
                    expires_at = None
                    shown = "Infinity"
                else:
                    if dur is None:
                        await safe_reply(update, "❌ Время: 7д/12ч/300м или 7 дней / 12 часов / 300 минут", reply_markup=cancel_menu())
                        return
                    expires_at = (datetime.now() + dur).isoformat(timespec="seconds")

                ensure_user(uid)
                grant_title(uid, code, expires_at=expires_at)

                await safe_reply(update, f"✅ Титул выдан {uid}: {TITLE_DISPLAY.get(code, code)} ({shown})", reply_markup=admin_menu())

            elif admin_action == "give_theme":
                # Формат: ID THEME_CODE 7д/12ч/300м/Infinity
                if len(parts) < 3:
                    await safe_reply(update, "❌ Формат: ID THEME_CODE 7д/12ч/300м/Infinity\nПример: 123 TOP Infinity", reply_markup=cancel_menu())
                    return

                uid = int(parts[0])
                code = parts[1].upper()

                if code not in THEMES:
                    await safe_reply(update, "❌ Неизвестный THEME_CODE", reply_markup=cancel_menu())
                    return

                is_inf, dur, shown = parse_admin_time(parts, 2)
                expires_at = None
                if is_inf:
                    expires_at = None
                    shown = "Infinity"
                else:
                    if dur is None:
                        await safe_reply(update, "❌ Время: 7д/12ч/300м или 7 дней / 12 часов / 300 минут", reply_markup=cancel_menu())
                        return
                    expires_at = (datetime.now() + dur).isoformat(timespec="seconds")

                ensure_user(uid)
                grant_theme(uid, code, expires_at=expires_at)

                await safe_reply(update, f"✅ Фон выдан {uid}: {THEMES[code][0]} {THEMES[code][1]} ({shown})", reply_markup=admin_menu())

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

    # ✅ Обновляем лимит кликов у старых юзеров (можно оставить навсегда)
    db_exec("UPDATE users SET clicks_limit=%s WHERE clicks_limit=1500", (DEFAULT_CLICKS_LIMIT,))

    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))

    # ✅ один inline handler для всего
    app.add_handler(CallbackQueryHandler(inline_handler))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handler))
    app.add_error_handler(error_handler)

    print("✅ Бот запущен")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()

