import os
import logging
import time
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Tuple, Any

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
ADMIN_ID = 1924971257
CHANNEL_ID = "@kisspromochannel"

CLICK_REWARD = 1
MIN_WITHDRAW = 1000

DEFAULT_CLICKS_LIMIT = 1500
CLICK_RESET_HOURS = 3
REF_REWARD = 250

DAILY_BONUS_AMOUNT = 500
DAILY_BONUS_HOURS = 24

VIP_LIMITS = {"VIP": 2500, "MVP": 3000, "PREMIUM": 4000}
VIP_ICONS = {"VIP": "🏆", "MVP": "💎", "PREMIUM": "💲"}
VIP_RANK = {"VIP": 1, "MVP": 2, "PREMIUM": 3}

# =========================
# ===== КЕЙСЫ =============
# =========================
CASE_RESET_HOURS = 12
CASE_LIMITS = {"common": 7, "rare": 4, "legend": 2}  # как ты утвердил
CASE_PRICES = {"common": 500, "rare": 1000, "legend": 3000}
CASE_ANIM_SECONDS = {"common": 7, "rare": 8, "legend": 10}  # интрига 7–10 сек

# ВЕСА (не проценты). Экономика: основной шанс — минус/почти минус, как ты хотел.
# ("gold", amount, weight) или ("vip", (VIPTYPE, days), weight)
CASE_WEIGHTS = {
    "common": [
        ("gold", 100, 60),
        ("gold", 250, 30),
        ("gold", 700, 12),
        ("gold", 1000, 6),
        ("vip", ("VIP", 1), 3),
        ("vip", ("MVP", 1), 2),
        ("gold", 2000, 1),
    ],
    "rare": [
        ("gold", 400, 70),
        ("gold", 700, 40),
        ("gold", 1400, 15),
        ("gold", 1700, 8),
        ("vip", ("MVP", 3), 4),
        ("vip", ("PREMIUM", 1), 2),
        ("gold", 4000, 1),
    ],
    "legend": [
        ("gold", 1000, 70),
        ("gold", 1500, 40),
        ("gold", 3300, 15),
        ("gold", 3900, 8),
        ("vip", ("MVP", 5), 5),
        ("vip", ("PREMIUM", 3), 4),
        ("gold", 6500, 2),
    ],
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
            clicks_limit INTEGER DEFAULT 1500,
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

    # ---- миграции / расширения
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

            -- ref bonus flags
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

            -- ===== CASES =====
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='case_common')
                THEN ALTER TABLE users ADD COLUMN case_common INTEGER DEFAULT 0;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='case_rare')
                THEN ALTER TABLE users ADD COLUMN case_rare INTEGER DEFAULT 0;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='case_legend')
                THEN ALTER TABLE users ADD COLUMN case_legend INTEGER DEFAULT 0;
            END IF;

            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='case_opened_common')
                THEN ALTER TABLE users ADD COLUMN case_opened_common INTEGER DEFAULT 0;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='case_opened_rare')
                THEN ALTER TABLE users ADD COLUMN case_opened_rare INTEGER DEFAULT 0;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='case_opened_legend')
                THEN ALTER TABLE users ADD COLUMN case_opened_legend INTEGER DEFAULT 0;
            END IF;

            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='case_reset_at')
                THEN ALTER TABLE users ADD COLUMN case_reset_at TEXT DEFAULT NULL;
            END IF;

            -- защита от спама открытия кейсов (сервак рестартнулся — всё равно защищает)
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='opening_case')
                THEN ALTER TABLE users ADD COLUMN opening_case INTEGER DEFAULT 0;
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
            [InlineKeyboardButton("📦 Кейсы", callback_data="cases")],
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


def cases_inline_menu(common: int, rare: int, legend: int):
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"📦 Обычный (x{common}) — Открыть", callback_data="open_case_common")],
            [InlineKeyboardButton(f"🎁 Редкий (x{rare}) — Открыть", callback_data="open_case_rare")],
            [InlineKeyboardButton(f"💎 Легендарный (x{legend}) — Открыть", callback_data="open_case_legend")],
            [InlineKeyboardButton("🛒 Магазин кейсов", callback_data="case_shop")],
            [InlineKeyboardButton("ℹ️ Что может выпасть?", callback_data="case_drops")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_profile")],
        ]
    )


def case_shop_menu():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📦 Купить Обычный", callback_data="buy_case_common")],
            [InlineKeyboardButton("🎁 Купить Редкий", callback_data="buy_case_rare")],
            [InlineKeyboardButton("💎 Купить Легендарный", callback_data="buy_case_legend")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="cases")],
        ]
    )


def case_drops_menu():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📦 Обычный — дроп", callback_data="drops_common")],
            [InlineKeyboardButton("🎁 Редкий — дроп", callback_data="drops_rare")],
            [InlineKeyboardButton("💎 Легендарный — дроп", callback_data="drops_legend")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="cases")],
        ]
    )


# =========================
# ===== ВСПОМОГАТЕЛЬНОЕ ===
# =========================
async def safe_reply(update: Update, text: str, reply_markup=None, parse_mode: Optional[str] = None):
    try:
        if update.message:
            return await update.message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except TimedOut:
        try:
            if update.message:
                return await update.message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
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
    if u.startswith("мин"):
        return timedelta(minutes=v)
    if u.startswith("час"):
        return timedelta(hours=v)
    if u.startswith("дн"):
        return timedelta(days=v)
    return None


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


def get_display_nick(user_id: int, tg_username: Optional[str], vip_type: Optional[str]):
    base = f"@{tg_username}" if tg_username else str(user_id)
    icon = VIP_ICONS.get(vip_type, "") if vip_type else ""
    return f"{base}{icon}"


def _html_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def safe_name_for_top_html(username: Optional[str], user_id: int) -> str:
    # Если есть юзернейм — @username, если нет — кликабельный ID
    if username:
        return _html_escape(f"@{username}")
    return f'<a href="tg://user?id={user_id}">ID:{user_id}</a>'


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


# =========================
# ===== CASES HELPERS =====
# =========================
def case_reset_if_needed(user_id: int):
    row = db_fetchone("SELECT case_reset_at FROM users WHERE id=%s", (user_id,))
    now = datetime.now()

    if not row or not row[0]:
        db_exec(
            "UPDATE users SET case_reset_at=%s, case_opened_common=0, case_opened_rare=0, case_opened_legend=0 WHERE id=%s",
            (now.isoformat(timespec="seconds"), user_id),
        )
        return

    try:
        last = datetime.fromisoformat(row[0])
    except Exception:
        db_exec(
            "UPDATE users SET case_reset_at=%s, case_opened_common=0, case_opened_rare=0, case_opened_legend=0 WHERE id=%s",
            (now.isoformat(timespec="seconds"), user_id),
        )
        return

    if now >= last + timedelta(hours=CASE_RESET_HOURS):
        db_exec(
            "UPDATE users SET case_reset_at=%s, case_opened_common=0, case_opened_rare=0, case_opened_legend=0 WHERE id=%s",
            (now.isoformat(timespec="seconds"), user_id),
        )


def case_time_left(user_id: int) -> Optional[timedelta]:
    row = db_fetchone("SELECT case_reset_at FROM users WHERE id=%s", (user_id,))
    if not row or not row[0]:
        return None
    try:
        last = datetime.fromisoformat(row[0])
    except Exception:
        return None
    next_reset = last + timedelta(hours=CASE_RESET_HOURS)
    return next_reset - datetime.now()


def case_roll(case_type: str) -> Tuple[str, Any]:
    items = CASE_WEIGHTS[case_type]
    total_w = sum(w for _, __, w in items)
    r = int.from_bytes(os.urandom(8), "big") % total_w
    cur = 0
    for itype, val, w in items:
        cur += w
        if r < cur:
            return itype, val
    return items[-1][0], items[-1][1]


async def case_animation(message, seconds: int, prefix: str):
    steps = ["░░░░░", "█░░░░", "██░░░", "███░░", "████░"]
    delay = seconds / len(steps)

    try:
        await message.edit_text(f"{prefix} Открываю кейс…")
    except Exception:
        pass

    await asyncio.sleep(max(0.6, delay * 0.8))
    for s in steps:
        try:
            await message.edit_text(f"🔄 Кручу… {s}")
        except Exception:
            pass
        await asyncio.sleep(delay)


def vip_until_new(current_until: Optional[str], add_days: int) -> str:
    now = datetime.now()
    base = now
    if current_until:
        try:
            until_dt = datetime.fromisoformat(current_until)
            if until_dt > now:
                base = until_dt
        except Exception:
            base = now
    return (base + timedelta(days=add_days)).isoformat(timespec="seconds")


def award_vip(user_id: int, vip_type_new: str, days: int) -> Tuple[bool, str]:
    """
    applied=True если применили/продлили/апгрейднули.
    applied=False если выпало ниже текущего.
    """
    check_and_update_vip(user_id)
    row = db_fetchone("SELECT vip_type, vip_until, vip_base_limit, clicks_limit FROM users WHERE id=%s", (user_id,))
    if not row:
        return False, "❌ Пользователь не найден."

    cur_type, cur_until, base_limit, clicks_limit = row
    cur_rank = VIP_RANK.get(cur_type, 0) if cur_type else 0
    new_rank = VIP_RANK.get(vip_type_new, 0)

    if cur_rank > new_rank:
        return False, "👑 У вас уже есть привилегия выше!"

    # тот же — продлеваем
    if cur_type and cur_type == vip_type_new:
        new_until = vip_until_new(cur_until, days)
        db_exec("UPDATE users SET vip_until=%s WHERE id=%s", (new_until, user_id))
        return True, f"🎖 Привилегия продлена: {vip_type_new} +{days}д ✅"

    # апгрейд или не было
    if base_limit is None:
        base_limit = int(clicks_limit) if clicks_limit is not None else DEFAULT_CLICKS_LIMIT

    new_until = vip_until_new(cur_until if cur_type else None, days)
    new_limit = VIP_LIMITS[vip_type_new]

    db_exec(
        "UPDATE users SET vip_type=%s, vip_until=%s, vip_base_limit=%s, clicks_limit=%s WHERE id=%s",
        (vip_type_new, new_until, base_limit, new_limit, user_id),
    )
    return True, f"🎉 Получено VIP: {vip_type_new} на {days}д ✅"


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
    case_reset_if_needed(user_id)

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
    try:
        await q.answer()
    except Exception:
        pass

    user_id = q.from_user.id
    username = q.from_user.username
    ensure_user(user_id, username=username)

    data = q.data or ""

    # BACK
    if data == "back_profile":
        await send_profile(q, context, user_id)
        return

    if data == "noop":
        return

    # ТОПЫ
    if data == "tops":
        await q.message.reply_text("🏆 Выберите ТОП:", reply_markup=tops_inline_menu())
        return

    # daily bonus
    if data == "daily_bonus":
        row = db_fetchone("SELECT last_daily_bonus FROM users WHERE id=%s", (user_id,))
        last_daily = row[0] if row else None

        ok, left = can_take_daily(last_daily)
        if not ok and left is not None:
            await q.message.reply_text(
                f"⏳ Ежедневный бонус уже был.\nСледующий через: {format_time_left(left)}"
            )
            return

        db_exec(
            "UPDATE users SET balance=balance+%s, last_daily_bonus=%s WHERE id=%s",
            (DAILY_BONUS_AMOUNT, now_iso(), user_id),
        )
        await q.message.reply_text(f"✅ Ежедневный бонус получен: +{DAILY_BONUS_AMOUNT} GOLD 🎁")
        return

    # топ по кликам
    if data == "top_clicks":
        rows = db_fetchall(
            "SELECT id, username, COALESCE(total_clicks,0) AS tc FROM users ORDER BY tc DESC, id ASC LIMIT 10"
        )
        msg = "📊 ТОП по кликам (всего)\n\n"
        if not rows:
            msg += "Пока пусто."
        else:
            lines = []
            for i, (uid, uname, tc) in enumerate(rows, start=1):
                lines.append(f"{i}) {safe_name_for_top_html(uname, uid)} — {int(tc)} кликов")
            msg += "<br>".join(lines)

        await q.message.reply_text(
            msg,
            reply_markup=tops_inline_menu(),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        return

    # топ по балансу
    if data == "top_balance":
        rows = db_fetchall("SELECT id, username, balance FROM users ORDER BY balance DESC, id ASC LIMIT 10")
        msg = "💰 ТОП по балансу\n\n"
        if not rows:
            msg += "Пока пусто."
        else:
            lines = []
            for i, (uid, uname, bal) in enumerate(rows, start=1):
                lines.append(f"{i}) {safe_name_for_top_html(uname, uid)} — {round(float(bal), 2)} GOLD")
            msg += "<br>".join(lines)

        await q.message.reply_text(
            msg,
            reply_markup=tops_inline_menu(),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        return

    # топ рефоводов
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
            lines = []
            for i, (ref_uid, ref_uname, c) in enumerate(rows, start=1):
                lines.append(f"{i}) {safe_name_for_top_html(ref_uname, ref_uid)} — {int(c)} рефералов")
            msg += "<br>".join(lines)

        await q.message.reply_text(
            msg,
            reply_markup=tops_inline_menu(),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        return

    # ref bonuses
    if data == "ref_bonuses":
        await send_ref_bonus_menu(q, context, user_id)
        return

    if data.startswith("claim_ref_"):
        await process_claim_ref_bonus(q, context, user_id, data)
        return

    # =========================
    # ===== CASES UI ==========
    # =========================
    if data == "cases":
        case_reset_if_needed(user_id)
        row = db_fetchone("SELECT case_common, case_rare, case_legend FROM users WHERE id=%s", (user_id,))
        common, rare, legend = row if row else (0, 0, 0)
        await q.message.reply_text("📦 Кейсы", reply_markup=cases_inline_menu(int(common), int(rare), int(legend)))
        return

    if data == "case_shop":
        text = (
            "🛒 Магазин кейсов\n\n"
            f"📦 Обычный: {CASE_PRICES['common']}G\n"
            f"🎁 Редкий: {CASE_PRICES['rare']}G\n"
            f"💎 Легендарный: {CASE_PRICES['legend']}G\n\n"
            "Покупка: по 1 штуке."
        )
        await q.message.reply_text(text, reply_markup=case_shop_menu())
        return

    if data == "case_drops":
        await q.message.reply_text("ℹ️ Выбери кейс:", reply_markup=case_drops_menu())
        return

    if data in ("drops_common", "drops_rare", "drops_legend"):
        if data == "drops_common":
            text = (
                "📦 Обычный кейс — что может выпасть:\n\n"
                "💰 100G / 250G / 700G / 1000G\n"
                "🎖 VIP на 1 день\n"
                "💎 MVP на 1 день\n"
                "🏆 Джекпот: 2000G"
            )
        elif data == "drops_rare":
            text = (
                "🎁 Редкий кейс — что может выпасть:\n\n"
                "💰 400G / 700G / 1400G / 1700G\n"
                "💎 MVP на 3 дня\n"
                "💲 PREMIUM на 1 день\n"
                "🏆 Джекпот: 4000G"
            )
        else:
            text = (
                "💎 Легендарный кейс — что может выпасть:\n\n"
                "💰 1000G / 1500G / 3300G / 3900G\n"
                "💎 MVP на 5 дней\n"
                "💲 PREMIUM на 3 дня\n"
                "🏆 Джекпот: 6500G"
            )
        await q.message.reply_text(text, reply_markup=case_drops_menu())
        return

    if data in ("buy_case_common", "buy_case_rare", "buy_case_legend"):
        ctype = "common" if data.endswith("common") else "rare" if data.endswith("rare") else "legend"
        price = CASE_PRICES[ctype]

        row = db_fetchone("SELECT balance FROM users WHERE id=%s", (user_id,))
        bal = float(row[0]) if row else 0.0

        if bal < price:
            await q.message.reply_text("❌ Недостаточно средств.", reply_markup=case_shop_menu())
            return

        db_exec(f"UPDATE users SET balance=balance-%s, case_{ctype}=case_{ctype}+1 WHERE id=%s", (price, user_id))
        await q.message.reply_text("✅ Покупка успешна!", reply_markup=case_shop_menu())
        return

    if data in ("open_case_common", "open_case_rare", "open_case_legend"):
        ctype = "common" if data.endswith("common") else "rare" if data.endswith("rare") else "legend"

        # защита от спама (память)
        if context.user_data.get("case_opening"):
            await q.message.reply_text("⏳ Подожди, кейс открывается…")
            return

        # защита от спама (БД)
        row_open = db_fetchone("SELECT opening_case FROM users WHERE id=%s", (user_id,))
        if row_open and int(row_open[0] or 0) == 1:
            await q.message.reply_text("⏳ Подожди, кейс открывается…")
            return

        case_reset_if_needed(user_id)

        row = db_fetchone(
            f"SELECT case_{ctype}, case_opened_{ctype} FROM users WHERE id=%s",
            (user_id,),
        )
        if not row:
            await q.message.reply_text("❌ Ошибка профиля. Напиши /start")
            return

        have, opened = int(row[0]), int(row[1])
        limit = CASE_LIMITS[ctype]

        if have <= 0:
            rowc = db_fetchone("SELECT case_common, case_rare, case_legend FROM users WHERE id=%s", (user_id,))
            common, rare, legend = rowc if rowc else (0, 0, 0)
            await q.message.reply_text(
                "❌ У вас нет кейсов этого типа.\n🛒 Купите в магазине.",
                reply_markup=cases_inline_menu(int(common), int(rare), int(legend)),
            )
            return

        if opened >= limit:
            left = case_time_left(user_id)
            left_text = format_time_left(left) if left else "скоро"
            await q.message.reply_text(f"⏳ Лимит открытий исчерпан.\nОбновление через: {left_text}")
            return

        # блокируем
        context.user_data["case_opening"] = True
        db_exec("UPDATE users SET opening_case=1 WHERE id=%s", (user_id,))

        prefix = "📦" if ctype == "common" else "🎁" if ctype == "rare" else "💎"
        try:
            msg = await q.message.reply_text(f"{prefix} Открываю кейс…")
            await case_animation(msg, CASE_ANIM_SECONDS[ctype], prefix)

            itype, val = case_roll(ctype)

            # выдача
            if itype == "gold":
                amount = int(val)
                db_exec("UPDATE users SET balance=balance+%s WHERE id=%s", (amount, user_id))
                result_text = f"🎉 Выпало: +{amount} GOLD"
            else:
                vip_name, days = val
                applied, message_text = award_vip(user_id, vip_name, int(days))
                result_text = message_text if applied else message_text + f"\n(Выпало: {vip_name} на {days}д)"

            # списать кейс и засчитать открытие
            db_exec(
                f"""
                UPDATE users SET
                    case_{ctype}=case_{ctype}-1,
                    case_opened_{ctype}=case_opened_{ctype}+1
                WHERE id=%s
                """,
                (user_id,),
            )

            try:
                await msg.edit_text(result_text)
            except Exception:
                await q.message.reply_text(result_text)

        finally:
            context.user_data["case_opening"] = False
            db_exec("UPDATE users SET opening_case=0 WHERE id=%s", (user_id,))

        return


async def send_profile(q, context, user_id: int):
    vip_type, vip_until_dt = check_and_update_vip(user_id)

    row = db_fetchone(
        "SELECT balance, clicks_used, clicks_limit, COALESCE(total_clicks,0), username FROM users WHERE id=%s",
        (user_id,),
    )
    if row:
        bal, _, _, total_clicks, stored_username = row
    else:
        bal, total_clicks, stored_username = (0, 0, None)

    used, next_reset, limit = check_click_reset(user_id)

    nick = get_display_nick(user_id, stored_username, vip_type)
    vip_status_text = vip_type if vip_type else "нет"
    vip_left_text = format_time_left(vip_until_dt - datetime.now()) if vip_until_dt else "нет VIP статуса"

    await q.message.reply_text(
        "👤 Профиль\n"
        f"Ваш ник: {nick}\n"
        f"VIP статус: {vip_status_text}\n"
        f"Срок VIP статуса: {vip_left_text}\n\n"
        f"💰 Баланс: {round(float(bal), 2)} GOLD\n"
        f"📊 Клики (за период): {used}/{limit}\n"
        f"🏁 Клики (всего): {int(total_clicks)}\n"
        f"⏳ До обновления: {format_time_left(next_reset - datetime.now())}",
        reply_markup=profile_inline_menu(),
    )


async def send_ref_bonus_menu(q, context, user_id: int):
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
    await q.message.reply_text(
        text,
        reply_markup=ref_bonuses_inline_menu(int(claimed10), int(claimed50), int(claimed100)),
    )


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
    case_reset_if_needed(user_id)

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
        used += 1
        await safe_reply(update, f"✅ Заработано {CLICK_REWARD} GOLD ({used}/{limit})", reply_markup=earn_menu())
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
        code = text.strip()
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

            if amount < MIN_WITHDRAW or amount > bal or int(amount) != amount:
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
            "Команды для заявок на вывод:\n"
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
            await safe_reply(update, "КОД СУММА КОЛ-ВО\nПример: KISS 10 5", reply_markup=cancel_menu())
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
            await safe_reply(update, "Формат:\nID VIP 1 день\nID MVP 3 дня\nID PREMIUM 1 день", reply_markup=cancel_menu())
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
            rows = db_fetchall("SELECT code, amount, uses_left FROM promocodes ORDER BY code ASC")
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
                    await safe_reply(update, "❌ Формат: ID ЛИМИТ", reply_markup=cancel_menu())
                    return
                uid, limit = int(parts[0]), int(parts[1])
                ensure_user(uid)
                db_exec("UPDATE users SET clicks_limit=%s WHERE id=%s", (limit, uid))
                await safe_reply(update, f"✅ Лимит кликов для {uid} = {limit}", reply_markup=admin_menu())

            elif admin_action == "give_vip":
                if len(parts) != 3:
                    await safe_reply(update, "❌ Формат: ID VIP 3 (дни)", reply_markup=cancel_menu())
                    return
                uid = int(parts[0])
                vip = parts[1].upper()
                days = int(parts[2])

                if vip not in VIP_LIMITS:
                    await safe_reply(update, "❌ Только VIP / MVP / PREMIUM", reply_markup=cancel_menu())
                    return
                if days <= 0:
                    await safe_reply(update, "❌ Дни должны быть > 0", reply_markup=cancel_menu())
                    return

                ensure_user(uid)
                applied, msg = award_vip(uid, vip, days)
                await safe_reply(update, msg, reply_markup=admin_menu())

            elif admin_action == "broadcast":
                msg = text
                users = db_fetchall("SELECT id FROM users")
                sent = 0
                for (uid,) in users:
                    try:
                        await context.bot.send_message(chat_id=uid, text=msg)
                        sent += 1
                        await asyncio.sleep(0.05)
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

