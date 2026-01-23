import sqlite3
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from datetime import datetime, timedelta

# ===== НАСТРОЙКИ =====
TOKEN = "8588883159:AAHb0mEd43jJhezkz0Q0p7s-R6pCfAqsipQ"
ADMIN_ID = 1924971257
CHANNEL_ID = "@kisspromochannel"

CLICK_REWARD = 1
MIN_WITHDRAW = 1000

DEFAULT_CLICKS_LIMIT = 1500
CLICK_RESET_HOURS = 12
REF_REWARD = 150

VIP_LIMITS = {
    "VIP": 2500,
    "MVP": 3000,
    "PREMIUM": 4000
}
VIP_ICONS = {
    "VIP": "🏆",
    "MVP": "💎",
    "PREMIUM": "💲"
}

# ===== БД =====
conn = sqlite3.connect("bot.db", check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    balance REAL DEFAULT 0,
    banned INTEGER DEFAULT 0,
    clicks_used INTEGER DEFAULT 0,
    clicks_limit INTEGER DEFAULT 1500,
    last_click_reset TEXT,
    subscribed INTEGER DEFAULT 0
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS referrals (
    user_id INTEGER PRIMARY KEY,
    referrer_id INTEGER,
    rewarded INTEGER DEFAULT 0
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS withdrawals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    amount REAL,
    requisites TEXT,
    status TEXT DEFAULT 'pending'
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS promocodes (
    code TEXT PRIMARY KEY,
    amount REAL,
    uses_left INTEGER DEFAULT 1
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS used_promocodes (
    user_id INTEGER,
    code TEXT,
    PRIMARY KEY(user_id, code)
)
""")

# --- добавляем VIP колонки безопасно ---
def _add_column_safe(table: str, col_def: str):
    try:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")
        conn.commit()
    except:
        pass

_add_column_safe("users", "vip_type TEXT DEFAULT NULL")
_add_column_safe("users", "vip_until TEXT DEFAULT NULL")  # ISO datetime
_add_column_safe("users", "vip_base_limit INTEGER DEFAULT NULL")  # чтобы вернуть лимит назад
conn.commit()

# ===== МЕНЮ =====
def main_menu(user_id):
    buttons = [
        ["👤 Профиль", "💰 Заработать"],
        ["👥 Рефералка", "💸 Вывод"],
        ["🎁 Ввести промокод"]
    ]
    if user_id == ADMIN_ID:
        buttons.append(["🛠 Админка"])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def earn_menu():
    return ReplyKeyboardMarkup([["👆 КЛИК"], ["🔙 Назад"]], resize_keyboard=True)

def admin_menu():
    return ReplyKeyboardMarkup(
        [["Создать промокод", "Выдать баланс"],
         ["Забрать баланс", "Бан/Разбан"],
         ["⚙ Выдать лимит кликов", "🎖 Выдать привилегию"],
         ["Рассылка", "📋 Заявки на вывод"],
         ["Все промокоды", "🔙 Назад"]],
        resize_keyboard=True
    )

def cancel_menu():
    return ReplyKeyboardMarkup([["❌ Отмена"], ["🔙 Назад"]], resize_keyboard=True)

def subscribe_menu():
    return ReplyKeyboardMarkup([["🔔 Подписаться"], ["✅ Я подписался"]], resize_keyboard=True)

# ===== ВСПОМОГАТЕЛЬНОЕ =====
async def is_subscribed(bot, user_id):
    try:
        member = await bot.get_chat_member(CHANNEL_ID, user_id)
        return member.status in ["member", "administrator", "creator"]
    except:
        return False

def check_click_reset(user_id):
    cursor.execute("SELECT last_click_reset, clicks_used, clicks_limit FROM users WHERE id=?", (user_id,))
    row = cursor.fetchone()
    now = datetime.now()

    if not row or row[0] is None:
        cursor.execute(
            "UPDATE users SET last_click_reset=?, clicks_used=0 WHERE id=?",
            (now.strftime("%Y-%m-%d %H:%M:%S"), user_id)
        )
        conn.commit()
        return 0, now + timedelta(hours=CLICK_RESET_HOURS), DEFAULT_CLICKS_LIMIT

    last_reset = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
    next_reset = last_reset + timedelta(hours=CLICK_RESET_HOURS)

    if now >= next_reset:
        cursor.execute(
            "UPDATE users SET last_click_reset=?, clicks_used=0 WHERE id=?",
            (now.strftime("%Y-%m-%d %H:%M:%S"), user_id)
        )
        conn.commit()
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
    """
    Проверяет VIP срок.
    Если истёк — снимает VIP и возвращает clicks_limit в vip_base_limit или DEFAULT_CLICKS_LIMIT.
    Возвращает (vip_type или None, vip_until_dt или None)
    """
    cursor.execute("SELECT vip_type, vip_until, vip_base_limit FROM users WHERE id=?", (user_id,))
    row = cursor.fetchone()
    if not row:
        return None, None

    vip_type, vip_until, vip_base_limit = row
    if not vip_type or not vip_until:
        return None, None

    try:
        until_dt = datetime.fromisoformat(vip_until)
    except:
        # если вдруг кривое значение — сбросим
        cursor.execute("UPDATE users SET vip_type=NULL, vip_until=NULL, vip_base_limit=NULL WHERE id=?", (user_id,))
        conn.commit()
        return None, None

    now = datetime.now()
    if now >= until_dt:
        restore_limit = vip_base_limit if vip_base_limit is not None else DEFAULT_CLICKS_LIMIT
        cursor.execute(
            "UPDATE users SET vip_type=NULL, vip_until=NULL, vip_base_limit=NULL, clicks_limit=? WHERE id=?",
            (restore_limit, user_id)
        )
        conn.commit()
        return None, None

    # VIP активен
    return vip_type, until_dt

def get_display_nick(update: Update, vip_type: str | None):
    # Ник: @username если есть, иначе first_name
    u = update.effective_user
    base = f"@{u.username}" if u.username else (u.first_name or "User")
    icon = VIP_ICONS.get(vip_type, "") if vip_type else ""
    return f"{base}{icon}"

# ===== СТАРТ =====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    args = context.args

    cursor.execute("INSERT OR IGNORE INTO users (id) VALUES (?)", (user_id,))
    conn.commit()

    if args:
        try:
            ref_id = int(args[0])
            if ref_id != user_id:
                cursor.execute(
                    "INSERT OR IGNORE INTO referrals (user_id, referrer_id) VALUES (?,?)",
                    (user_id, ref_id)
                )
                conn.commit()
        except:
            pass

    subscribed = await is_subscribed(context.bot, user_id)
    cursor.execute("UPDATE users SET subscribed=? WHERE id=?", (1 if subscribed else 0, user_id))
    conn.commit()

    if not subscribed:
        await update.message.reply_text(
            f"🔔 Подпишись на канал:\n{CHANNEL_ID}\n\nПосле подписки нажми «✅ Я подписался»",
            reply_markup=subscribe_menu()
        )
        return

    check_click_reset(user_id)
    context.user_data.clear()
    context.user_data["menu"] = "main"
    await update.message.reply_text("✨ Добро пожаловать!", reply_markup=main_menu(user_id))

# ===== ОБРАБОТКА =====
async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.effective_user.id

    # всегда проверяем VIP на истечение (и обновляем лимит если надо)
    vip_type, vip_until_dt = check_and_update_vip(user_id)

    # бан (кроме админа)
    if user_id != ADMIN_ID:
        cursor.execute("SELECT banned FROM users WHERE id=?", (user_id,))
        r = cursor.fetchone()
        if r and r[0] == 1:
            await update.message.reply_text("⛔ Вы заблокированы.")
            return

    # НАЗАД / ОТМЕНА
    if text in ["🔙 Назад", "❌ Отмена"]:
        # если отменяем админ-действие — вернемся в админ-меню
        if user_id == ADMIN_ID and context.user_data.get("admin_action"):
            context.user_data.pop("admin_action", None)
            context.user_data["menu"] = "admin"
            await update.message.reply_text("Действие отменено", reply_markup=admin_menu())
            return

        context.user_data.clear()
        await update.message.reply_text("Главное меню", reply_markup=main_menu(user_id))
        return

    # ПОДПИСКА
    if text == "✅ Я подписался":
        subscribed = await is_subscribed(context.bot, user_id)
        cursor.execute("UPDATE users SET subscribed=? WHERE id=?", (1 if subscribed else 0, user_id))
        conn.commit()
        if subscribed:
            await update.message.reply_text("✅ Подписка подтверждена!", reply_markup=main_menu(user_id))
        else:
            await update.message.reply_text("❌ Ты ещё не подписался!", reply_markup=subscribe_menu())
        return

    # ПРОФИЛЬ
    if text == "👤 Профиль":
        # пересчитаем VIP прямо перед выводом
        vip_type, vip_until_dt = check_and_update_vip(user_id)

        cursor.execute("SELECT balance, clicks_used, clicks_limit FROM users WHERE id=?", (user_id,))
        bal, used, limit = cursor.fetchone()

        used, next_reset, limit = check_click_reset(user_id)

        nick = get_display_nick(update, vip_type)
        vip_status_text = vip_type if vip_type else "нет"
        vip_left_text = format_time_left(vip_until_dt - datetime.now()) if vip_until_dt else "нет VIP статуса"

        await update.message.reply_text(
            f"👤 Профиль\n"
            f"Ваш ник: {nick}\n"
            f"VIP статус: {vip_status_text}\n"
            f"Срок VIP статуса: {vip_left_text}\n\n"
            f"💰 Баланс: {round(bal,2)} GOLD\n"
            f"📊 Клики: {used}/{limit}\n"
            f"⏳ До обновления: {format_time_left(next_reset - datetime.now())}",
            reply_markup=main_menu(user_id)
        )
        return

    # ЗАРАБОТАТЬ
    if text == "💰 Заработать":
        used, _, limit = check_click_reset(user_id)
        if used >= limit:
            await update.message.reply_text("❌ У вас закончились клики", reply_markup=main_menu(user_id))
            return
        context.user_data["earning"] = True
        await update.message.reply_text("👆 Нажимай «КЛИК»", reply_markup=earn_menu())
        return

    if text == "👆 КЛИК" and context.user_data.get("earning"):
        used, _, limit = check_click_reset(user_id)
        if used >= limit:
            await update.message.reply_text("❌ У вас закончились клики", reply_markup=main_menu(user_id))
            return
        cursor.execute(
            "UPDATE users SET balance=balance+?, clicks_used=clicks_used+1 WHERE id=?",
            (CLICK_REWARD, user_id)
        )
        conn.commit()
        used += 1
        await update.message.reply_text(
            f"✅ Заработано {CLICK_REWARD} GOLD ({used}/{limit})",
            reply_markup=earn_menu()
        )
        return

    # РЕФЕРАЛКА
    if text == "👥 Рефералка":
        cursor.execute("SELECT user_id, rewarded FROM referrals WHERE referrer_id=?", (user_id,))
        refs = cursor.fetchall()
        total = len(refs)
        earned = 0

        for ref_id, rewarded in refs:
            cursor.execute("SELECT subscribed FROM users WHERE id=?", (ref_id,))
            row = cursor.fetchone()
            sub = row[0] if row else 0
            if sub and rewarded == 0:
                cursor.execute("UPDATE users SET balance=balance+? WHERE id=?", (REF_REWARD, user_id))
                cursor.execute("UPDATE referrals SET rewarded=1 WHERE user_id=?", (ref_id,))
                conn.commit()
                earned += REF_REWARD

        link = f"https://t.me/topclickerkisspromobot?start={user_id}"
        await update.message.reply_text(
            f"👥 Ваша ссылка:\n{link}\n"
            f"💰 За подписанного: {REF_REWARD} GOLD\n"
            f"👥 Всего: {total}\n"
            f"💵 Получено: {earned} GOLD",
            reply_markup=main_menu(user_id)
        )
        return

    # ПРОМО
    if text == "🎁 Ввести промокод":
        context.user_data["menu"] = "promo"
        await update.message.reply_text("Введите промокод:", reply_markup=cancel_menu())
        return

    if context.user_data.get("menu") == "promo":
        cursor.execute("SELECT amount, uses_left FROM promocodes WHERE code=?", (text,))
        res = cursor.fetchone()
        if not res:
            await update.message.reply_text("❌ Неверный промокод", reply_markup=main_menu(user_id))
        else:
            amount, uses_left = res
            cursor.execute("SELECT 1 FROM used_promocodes WHERE user_id=? AND code=?", (user_id, text))
            if cursor.fetchone():
                await update.message.reply_text("❌ Уже использован", reply_markup=main_menu(user_id))
            elif uses_left <= 0:
                await update.message.reply_text("❌ Промокод недействителен", reply_markup=main_menu(user_id))
            else:
                cursor.execute("UPDATE users SET balance=balance+? WHERE id=?", (amount, user_id))
                cursor.execute("UPDATE promocodes SET uses_left=uses_left-1 WHERE code=?", (text,))
                cursor.execute("INSERT INTO used_promocodes VALUES (?,?)", (user_id, text))
                conn.commit()
                await update.message.reply_text(
                    f"🎉 ПРОМО АКТИВИРОВАН\n💰 +{amount} GOLD",
                    reply_markup=main_menu(user_id)
                )
        context.user_data.clear()
        return

    # ВЫВОД
    if text == "💸 Вывод":
        cursor.execute("SELECT balance FROM users WHERE id=?", (user_id,))
        bal = cursor.fetchone()[0]
        if bal < MIN_WITHDRAW:
            await update.message.reply_text(f"❌ Минимум {MIN_WITHDRAW} GOLD", reply_markup=main_menu(user_id))
            return
        context.user_data["withdraw_step"] = "amount"
        await update.message.reply_text("Введите сумму:", reply_markup=cancel_menu())
        return

    if context.user_data.get("withdraw_step") == "amount":
        try:
            amount = float(text)
            cursor.execute("SELECT balance FROM users WHERE id=?", (user_id,))
            bal = cursor.fetchone()[0]
            if amount < MIN_WITHDRAW or amount > bal:
                await update.message.reply_text("❌ Неверная сумма", reply_markup=cancel_menu())
                return
            context.user_data["withdraw_amount"] = amount
            context.user_data["withdraw_step"] = "requisites"
            await update.message.reply_text("Введите реквизиты:", reply_markup=cancel_menu())
        except:
            await update.message.reply_text("❌ Введите число", reply_markup=cancel_menu())
        return

    if context.user_data.get("withdraw_step") == "requisites":
        amount = context.user_data["withdraw_amount"]
        requisites = text
        cursor.execute("INSERT INTO withdrawals (user_id, amount, requisites) VALUES (?,?,?)",
                       (user_id, amount, requisites))
        cursor.execute("UPDATE users SET balance=balance-? WHERE id=?", (amount, user_id))
        conn.commit()
        now = datetime.now().strftime("%d.%m.%Y %H:%M")
        await update.message.reply_text(
            f"✅ Заявка отправлена!\n💰 {amount} GOLD\n✍ {requisites}\n🕒 {now}",
            reply_markup=main_menu(user_id)
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
            await update.message.reply_text("❌ Нет доступа", reply_markup=main_menu(user_id))
            return
        context.user_data["menu"] = "admin"
        context.user_data.pop("admin_action", None)
        await update.message.reply_text("🛠 Админ панель", reply_markup=admin_menu())
        return

    # кнопки админки (запуск действий)
    if user_id == ADMIN_ID and menu == "admin" and admin_action is None:
        if text == "Создать промокод":
            context.user_data["admin_action"] = "create_promocode"
            await update.message.reply_text("Код Сумма Кол-во\nПример: KISS 10 5", reply_markup=cancel_menu())
            return

        if text == "Выдать баланс":
            context.user_data["admin_action"] = "give_balance"
            await update.message.reply_text("ID Сумма\nПример: 123456789 100", reply_markup=cancel_menu())
            return

        if text == "Забрать баланс":
            context.user_data["admin_action"] = "take_balance"
            await update.message.reply_text("ID Сумма\nПример: 123456789 50", reply_markup=cancel_menu())
            return

        if text == "Бан/Разбан":
            context.user_data["admin_action"] = "ban_user"
            await update.message.reply_text("ID пользователя\nПример: 123456789", reply_markup=cancel_menu())
            return

        if text == "⚙ Выдать лимит кликов":
            context.user_data["admin_action"] = "set_click_limit"
            await update.message.reply_text("ID НовыйЛимит\nПример: 123456789 3000", reply_markup=cancel_menu())
            return

        if text == "🎖 Выдать привилегию":
            context.user_data["admin_action"] = "give_vip"
            await update.message.reply_text(
                "Формат:\n"
                "ID VIP 1 час\n"
                "ID MVP 300 минут\n"
                "ID PREMIUM 2 дня",
                reply_markup=cancel_menu()
            )
            return

        if text == "Рассылка":
            context.user_data["admin_action"] = "broadcast"
            await update.message.reply_text("Текст рассылки:", reply_markup=cancel_menu())
            return

        if text == "📋 Заявки на вывод":
            cursor.execute("SELECT id, user_id, amount, status FROM withdrawals ORDER BY id DESC")
            rows = cursor.fetchall()
            msg = "\n".join([f"#{r[0]} | {r[1]} | {r[2]} GOLD | {r[3]}" for r in rows]) or "Нет заявок"
            await update.message.reply_text(msg, reply_markup=admin_menu())
            return

        if text == "Все промокоды":
            cursor.execute("SELECT code, amount, uses_left FROM promocodes")
            rows = cursor.fetchall()
            if not rows:
                await update.message.reply_text("Промокодов пока нет", reply_markup=admin_menu())
            else:
                msg = "🎁 Все промокоды:\n\n"
                for code, amount, uses_left in rows:
                    msg += f"🔑 {code} — 💰 {amount} GOLD — 🕹️ {uses_left} активаций\n"
                await update.message.reply_text(msg, reply_markup=admin_menu())
            return

    # выполнение админ-действий
    if user_id == ADMIN_ID and admin_action:
        parts = text.split()
        try:
            if admin_action == "create_promocode":
                if len(parts) != 3:
                    await update.message.reply_text("❌ Формат: КОД СУММА КОЛ-ВО", reply_markup=cancel_menu())
                    return
                code, amount, uses = parts[0], float(parts[1]), int(parts[2])
                cursor.execute(
                    "INSERT OR REPLACE INTO promocodes (code, amount, uses_left) VALUES (?,?,?)",
                    (code, amount, uses)
                )
                conn.commit()
                await update.message.reply_text(
                    f"✅ Промокод создан: {code} | {amount} | {uses}",
                    reply_markup=admin_menu()
                )

            elif admin_action == "give_balance":
                if len(parts) != 2:
                    await update.message.reply_text("❌ Формат: ID СУММА", reply_markup=cancel_menu())
                    return
                uid, amount = int(parts[0]), float(parts[1])
                cursor.execute("UPDATE users SET balance=balance+? WHERE id=?", (amount, uid))
                conn.commit()
                await update.message.reply_text(f"✅ Выдано {amount} GOLD пользователю {uid}", reply_markup=admin_menu())

            elif admin_action == "take_balance":
                if len(parts) != 2:
                    await update.message.reply_text("❌ Формат: ID СУММА", reply_markup=cancel_menu())
                    return
                uid, amount = int(parts[0]), float(parts[1])
                cursor.execute("UPDATE users SET balance=balance-? WHERE id=?", (amount, uid))
                conn.commit()
                await update.message.reply_text(f"✅ Снято {amount} GOLD у пользователя {uid}", reply_markup=admin_menu())

            elif admin_action == "ban_user":
                if len(parts) != 1:
                    await update.message.reply_text("❌ Формат: ID", reply_markup=cancel_menu())
                    return
                uid = int(parts[0])
                cursor.execute("SELECT banned FROM users WHERE id=?", (uid,))
                row = cursor.fetchone()
                banned = row[0] if row else 0
                new_status = 0 if banned else 1
                cursor.execute("UPDATE users SET banned=? WHERE id=?", (new_status, uid))
                conn.commit()
                await update.message.reply_text(
                    f"✅ Пользователь {uid} {'разбанен' if banned else 'забанен'}",
                    reply_markup=admin_menu()
                )

            elif admin_action == "set_click_limit":
                if len(parts) != 2:
                    await update.message.reply_text("❌ Формат: ID НОВЫЙ_ЛИМИТ", reply_markup=cancel_menu())
                    return
                uid, limit = int(parts[0]), int(parts[1])
                cursor.execute("UPDATE users SET clicks_limit=? WHERE id=?", (limit, uid))
                conn.commit()
                await update.message.reply_text(f"✅ Лимит кликов для {uid} = {limit}", reply_markup=admin_menu())

            elif admin_action == "give_vip":
                # ID VIP 1 час / ID MVP 300 минут / ID PREMIUM 2 дня
                if len(parts) != 4:
                    await update.message.reply_text(
                        "❌ Формат:\nID VIP 1 час\nID MVP 300 минут\nID PREMIUM 2 дня",
                        reply_markup=cancel_menu()
                    )
                    return
                uid = int(parts[0])
                vip = parts[1].upper()
                value = parts[2]
                unit = parts[3]

                if vip not in VIP_LIMITS:
                    await update.message.reply_text("❌ Привилегия только: VIP / MVP / PREMIUM", reply_markup=cancel_menu())
                    return

                dur = parse_duration(value, unit)
                if not dur:
                    await update.message.reply_text("❌ Время: минут/час/дня (пример: 300 минут / 1 час / 2 дня)", reply_markup=cancel_menu())
                    return

                # запомним текущий лимит, чтобы вернуть потом
                cursor.execute("SELECT clicks_limit FROM users WHERE id=?", (uid,))
                row = cursor.fetchone()
                current_limit = row[0] if row else DEFAULT_CLICKS_LIMIT

                until = datetime.now() + dur
                new_limit = VIP_LIMITS[vip]

                cursor.execute("""
                    UPDATE users
                    SET vip_type=?, vip_until=?, vip_base_limit=?, clicks_limit=?
                    WHERE id=?
                """, (vip, until.isoformat(), current_limit, new_limit, uid))
                conn.commit()

                await update.message.reply_text(
                    f"✅ Вы успешно выдали пользователю {uid}\n"
                    f"Привилегию: {vip} {VIP_ICONS[vip]}\n"
                    f"Срок: {value} {unit}\n"
                    f"Лимит кликов: {new_limit}",
                    reply_markup=admin_menu()
                )

            elif admin_action == "broadcast":
                msg = text
                cursor.execute("SELECT id FROM users")
                users = cursor.fetchall()
                sent = 0
                for (uid,) in users:
                    try:
                        await context.bot.send_message(chat_id=uid, text=msg)
                        sent += 1
                    except:
                        pass
                await update.message.reply_text(f"✅ Рассылка завершена. Отправлено: {sent}", reply_markup=admin_menu())

        except Exception as e:
            await update.message.reply_text(f"❌ Ошибка: {e}", reply_markup=admin_menu())
        finally:
            context.user_data.pop("admin_action", None)
            context.user_data["menu"] = "admin"
        return

    # чтобы бот не молчал
    await update.message.reply_text("Выберите пункт меню 👇", reply_markup=main_menu(user_id))

# ===== MAIN =====
def main():
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handler))
    print("✅ Бот запущен")
    app.run_polling()

if __name__ == "__main__":
    main()
