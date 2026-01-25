import os
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

from telegram import Update, ReplyKeyboardMarkup
from telegram.error import TimedOut, Conflict
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# =========================
# ===== НАСТРОЙКИ =========
# =========================
TOKEN = os.getenv("TOKEN")  # Railway Variables -> TOKEN
DATABASE_URL = os.getenv("DATABASE_URL")  # Railway Variables -> DATABASE_URL (из Postgres)
ADMIN_ID = 1924971257
CHANNEL_ID = "@kisspromochannel"

CLICK_REWARD = 1
MIN_WITHDRAW = 1000

DEFAULT_CLICKS_LIMIT = 1500
CLICK_RESET_HOURS = 12
REF_REWARD = 150

VIP_LIMITS = {"VIP": 2500, "MVP": 3000, "PREMIUM": 4000}
VIP_ICONS = {"VIP": "🏆", "MVP": "💎", "PREMIUM": "💲"}

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
def db_connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL не найден. Добавь Railway Variables -> DATABASE_URL (из Postgres).")
    # Railway Postgres обычно требует SSL
    return psycopg2.connect(DATABASE_URL, sslmode="require")

conn = db_connect()
conn.autocommit = True

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

def column_exists(table: str, column: str) -> bool:
    row = db_fetchone(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name=%s AND column_name=%s
        """,
        (table, column),
    )
    return row is not None

def add_column_safe(table: str, col_def: str, col_name: str):
    try:
        if not column_exists(table, col_name):
            db_exec(f'ALTER TABLE "{table}" ADD COLUMN {col_def}')
    except Exception as e:
        logger.warning(f"add_column_safe failed: {e}")

def init_db():
    # users
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

    # used_promocodes
    db_exec(
        """
        CREATE TABLE IF NOT EXISTS used_promocodes (
            user_id BIGINT,
            code TEXT,
            PRIMARY KEY(user_id, code)
        )
        """
    )

    # VIP columns
    add_column_safe("users", "vip_type TEXT DEFAULT NULL", "vip_type")
    add_column_safe("users", "vip_until TEXT DEFAULT NULL", "vip_until")
    add_column_safe("users", "vip_base_limit INTEGER DEFAULT NULL", "vip_base_limit")

    # withdrawals admin columns
    add_column_safe("withdrawals", "admin_note TEXT DEFAULT NULL", "admin_note")
    add_column_safe("withdrawals", "decided_at TEXT DEFAULT NULL", "decided_at")

init_db()

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

# =========================
# ===== ВСПОМОГАТЕЛЬНОЕ ===
# =========================
async def safe_reply(update: Update, text: str, reply_markup=None):
    try:
        if update.message:
            return await update.message.reply_text(text, reply_markup=reply_markup)
    except TimedOut:
        try:
            if update.message:
                return await update.message.reply_text(text, reply_markup=reply_markup)
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

def ensure_user(user_id: int):
    db_exec("INSERT INTO users (id) VALUES (%s) ON CONFLICT (id) DO NOTHING", (user_id,))

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

def get_display_nick(update: Update, vip_type: Optional[str]):
    u = update.effective_user
    base = f"@{u.username}" if u.username else (u.first_name or "User")
    icon = VIP_ICONS.get(vip_type, "") if vip_type else ""
    return f"{base}{icon}"

# =========================
# ===== СТАРТ =============
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    args = context.args

    ensure_user(user_id)

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
# ===== ВЫВОД done/cancel=
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

        msg_user = (
            "✅ Ваша заявка на вывод подтверждена\n"
            f"💰 Сумма: {amount} GOLD\n"
            "🕒 Ожидайте зачисление (или уже отправлено)\n"
        )
        if admin_note.strip():
            msg_user += f"\n💬 Сообщение от админа: {admin_note.strip()}"

        try:
            await context.bot.send_message(chat_id=target_uid, text=msg_user)
        except Exception:
            pass

        await safe_reply(
            update,
            f"✅ Готово. Заявка #{wid} подтверждена.\nПользователь: {target_uid}\nСумма: {amount} GOLD",
            reply_markup=admin_menu(),
        )
        return True

    if cmd == "cancel":
        db_exec("UPDATE users SET balance=balance+%s WHERE id=%s", (amount, target_uid))
        db_exec(
            "UPDATE withdrawals SET status='declined', admin_note=%s, decided_at=%s WHERE id=%s",
            (admin_note, decided_at, wid),
        )

        msg_user = (
            "❌ Ваша заявка на вывод отклонена\n"
            f"💰 Сумма: {amount} GOLD\n"
            "↩️ Средства возвращены на баланс.\n"
        )
        if admin_note.strip():
            msg_user += f"\n💬 Причина: {admin_note.strip()}"

        try:
            await context.bot.send_message(chat_id=target_uid, text=msg_user)
        except Exception:
            pass

        await safe_reply(
            update,
            f"✅ Отклонено. Заявка #{wid} закрыта.\nПользователь: {target_uid}\nСумма: {amount} GOLD (возврат сделан)",
            reply_markup=admin_menu(),
        )
        return True

    return False

# =========================
# ===== ОБРАБОТКА =========
# =========================
async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    text = update.message.text
    user_id = update.effective_user.id

    ensure_user(user_id)
    vip_type, vip_until_dt = check_and_update_vip(user_id)

    # бан (кроме админа)
    if user_id != ADMIN_ID:
        r = db_fetchone("SELECT banned FROM users WHERE id=%s", (user_id,))
        if r and r[0] == 1:
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
        vip_type, vip_until_dt = check_and_update_vip(user_id)

        row = db_fetchone("SELECT balance, clicks_used, clicks_limit FROM users WHERE id=%s", (user_id,))
        bal, used, limit = row if row else (0, 0, DEFAULT_CLICKS_LIMIT)

        used, next_reset, limit = check_click_reset(user_id)

        nick = get_display_nick(update, vip_type)
        vip_status_text = vip_type if vip_type else "нет"
        vip_left_text = format_time_left(vip_until_dt - datetime.now()) if vip_until_dt else "нет VIP статуса"

        await safe_reply(
            update,
            f"👤 Профиль\n"
            f"Ваш ник: {nick}\n"
            f"VIP статус: {vip_status_text}\n"
            f"Срок VIP статуса: {vip_left_text}\n\n"
            f"💰 Баланс: {round(bal, 2)} GOLD\n"
            f"📊 Клики: {used}/{limit}\n"
            f"⏳ До обновления: {format_time_left(next_reset - datetime.now())}",
            reply_markup=main_menu(user_id),
        )
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

    if text == "👆 КЛИК" and context.user_data.get("earning"):
        used, _, limit = check_click_reset(user_id)
        if used >= limit:
            await safe_reply(update, "❌ У вас закончились клики", reply_markup=main_menu(user_id))
            return
        db_exec("UPDATE users SET balance=balance+%s, clicks_used=clicks_used+1 WHERE id=%s", (CLICK_REWARD, user_id))
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
            sub = row[0] if row else 0
            if sub and rewarded == 0:
                db_exec("UPDATE users SET balance=balance+%s WHERE id=%s", (REF_REWARD, user_id))
                db_exec("UPDATE referrals SET rewarded=1 WHERE user_id=%s", (ref_id,))
                earned += REF_REWARD

        link = f"https://t.me/topclickerkisspromobot?start={user_id}"
        await safe_reply(
            update,
            f"👥 Ваша ссылка:\n{link}\n"
            f"💰 За подписанного: {REF_REWARD} GOLD\n"
            f"👥 Всего: {total}\n"
            f"💵 Получено: {earned} GOLD",
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
            elif uses_left <= 0:
                await safe_reply(update, "❌ Промокод недействителен", reply_markup=main_menu(user_id))
            else:
                db_exec("UPDATE users SET balance=balance+%s WHERE id=%s", (amount, user_id))
                db_exec("UPDATE promocodes SET uses_left=uses_left-1 WHERE code=%s", (text,))
                db_exec("INSERT INTO used_promocodes (user_id, code) VALUES (%s, %s) ON CONFLICT DO NOTHING", (user_id, text))
                await safe_reply(update, f"🎉 ПРОМО АКТИВИРОВАН\n💰 +{amount} GOLD", reply_markup=main_menu(user_id))
        context.user_data.clear()
        return

    # ВЫВОД
    if text == "💸 Вывод":
        row = db_fetchone("SELECT balance FROM users WHERE id=%s", (user_id,))
        bal = row[0] if row else 0
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
            bal = row[0] if row else 0

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
        amount = context.user_data.get("withdraw_amount", 0)
        requisites = text.strip()

        db_exec(
            "INSERT INTO withdrawals (user_id, amount, requisites, status) VALUES (%s,%s,%s,'pending')",
            (user_id, amount, requisites),
        )
        db_exec("UPDATE users SET balance=balance-%s WHERE id=%s", (amount, user_id))

        await safe_reply(
            update,
            f"✅ Заявка отправлена!\n"
            f"💰 {amount} GOLD\n"
            f"✍️ {requisites}\n"
            f"🕒 {now_human()}\n\n"
            f"⏳ Регламент вывода: в течение 24 часов. Ожидайте ✅",
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

    # done/cancel
    if user_id == ADMIN_ID:
        handled = await admin_process_withdraw_decision(update, context, text)
        if handled:
            return

    # кнопки админки
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
            await safe_reply(update, "Формат:\nID VIP 1 час\nID MVP 300 минут\nID PREMIUM 2 дня", reply_markup=cancel_menu())
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

    # выполнение админ-действий
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
                banned = row[0] if row else 0
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
                if len(parts) != 4:
                    await safe_reply(update, "❌ Формат:\nID VIP 1 час\nID MVP 300 минут\nID PREMIUM 2 дня", reply_markup=cancel_menu())
                    return
                uid = int(parts[0])
                vip = parts[1].upper()
                value = parts[2]
                unit = parts[3]

                if vip not in VIP_LIMITS:
                    await safe_reply(update, "❌ Привилегия только: VIP / MVP / PREMIUM", reply_markup=cancel_menu())
                    return

                dur = parse_duration(value, unit)
                if not dur:
                    await safe_reply(update, "❌ Время: минут/час/дня (пример: 300 минут / 1 час / 2 дня)", reply_markup=cancel_menu())
                    return

                ensure_user(uid)
                row = db_fetchone("SELECT clicks_limit FROM users WHERE id=%s", (uid,))
                current_limit = row[0] if row else DEFAULT_CLICKS_LIMIT

                until = datetime.now() + dur
                new_limit = VIP_LIMITS[vip]

                db_exec(
                    "UPDATE users SET vip_type=%s, vip_until=%s, vip_base_limit=%s, clicks_limit=%s WHERE id=%s",
                    (vip, until.isoformat(), current_limit, new_limit, uid),
                )

                await safe_reply(
                    update,
                    f"✅ Вы успешно выдали пользователю {uid}\n"
                    f"Привилегию: {vip} {VIP_ICONS[vip]}\n"
                    f"Срок: {value} {unit}\n"
                    f"Лимит кликов: {new_limit}",
                    reply_markup=admin_menu(),
                )

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
        logger.warning("Conflict: похоже запущено 2 экземпляра бота (getUpdates). Оставь один.")
        return
    logger.exception("Unhandled error:", exc_info=err)

# =========================
# ===== MAIN ==============
# =========================
def main():
    if not TOKEN:
        raise RuntimeError("TOKEN не найден. Добавь Railway Variables -> TOKEN")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL не найден. Добавь Railway Variables -> DATABASE_URL (из Postgres)")

    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handler))
    app.add_error_handler(error_handler)

    print("✅ Бот запущен")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
