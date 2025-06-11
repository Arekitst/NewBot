from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command, Text
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from functools import wraps
import sqlite3
import asyncio

# ----------- Настройка базы данных ------------

conn = sqlite3.connect('main.db', check_same_thread=False)
cursor = conn.cursor()

def setup_database():
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        balance INTEGER DEFAULT 0,
        prefix TEXT DEFAULT NULL,
        has_vip BOOLEAN DEFAULT FALSE,
        has_antitar BOOLEAN DEFAULT FALSE
    )
    ''')
    conn.commit()

def add_user(user_id, username=None):
    cursor.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
    if cursor.fetchone() is None:
        cursor.execute(
            "INSERT INTO users (user_id, username) VALUES (?, ?)",
            (user_id, username or "")
        )
        conn.commit()

def get_user_profile(user_id):
    cursor.execute("""
        SELECT user_id, username, balance, prefix, has_vip, has_antitar
        FROM users WHERE user_id = ?
    """, (user_id,))
    return cursor.fetchone()

def update_balance(user_id, amount):
    cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
    conn.commit()

def set_balance(user_id, amount):
    cursor.execute("UPDATE users SET balance = ? WHERE user_id = ?", (amount, user_id))
    conn.commit()

# ----------- Инициализация бота ------------

API_TOKEN = "ТВОЙ_ТОКЕН_ЗДЕСЬ"  # не забудь заменить на свой токен!

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# ----------- Админ лист ------------

ADMIN_IDS = {6179115044, 7189733067}

def admin_only(handler):
    @wraps(handler)
    async def wrapper(message: types.Message, *args, **kwargs):
        if message.from_user.id not in ADMIN_IDS:
            await message.reply("🚫 У вас нет прав админа.")
            return
        return await handler(message, *args, **kwargs)
    return wrapper

# ----------- Команда /admin ------------

@dp.message(Command("admin"))
@admin_only
async def cmd_admin_panel(message: types.Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Выдать монеты", callback_data="admin_give")],
        # Можно добавить другие кнопки
    ])
    await message.answer("Панель администратора:", reply_markup=kb)

@dp.callback_query(Text("admin_give"))
async def admin_give_start(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        return await callback.answer("🚫 У вас нет прав админа.", show_alert=True)
    await callback.message.answer("Введите команду в формате:\n/give <user_id> <amount>\nИли ответом на сообщение пользователя: /give <amount>")
    await callback.answer()

# ----------- Команда /give ------------

@dp.message(Command("give"))
@admin_only
async def cmd_give(message: types.Message):
    # Если команда в ответ на сообщение — выдаём пользователю, на чей ответ
    if message.reply_to_message is not None:
        parts = message.text.split()
        if len(parts) != 2 or not parts[1].isdigit():
            return await message.reply("❌ Использование в ответе: /give <amount>")
        amount = int(parts[1])
        if amount <= 0:
            return await message.reply("❌ Сумма должна быть положительной!")
        target_user = message.reply_to_message.from_user
        if target_user.id == message.from_user.id:
            return await message.reply("❌ Нельзя выдавать себе.")
        add_user(target_user.id, target_user.username)
        update_balance(target_user.id, amount)
        new_bal = get_user_profile(target_user.id)[2]
        await message.reply(
            f"🦎 Выдано {amount} 🦎 пользователю {target_user.full_name} (ID: {target_user.id}). "
            f"Новый баланс: {new_bal} 🦎."
        )
    else:
        # Обычная команда: /give <user_id> <amount>
        parts = message.text.split()
        if len(parts) != 3 or not (parts[1].isdigit() and parts[2].isdigit()):
            return await message.reply("❌ Использование: /give <user_id> <amount>")
        target_id = int(parts[1])
        amount = int(parts[2])
        if amount <= 0:
            return await message.reply("❌ Сумма должна быть положительной!")
        if target_id == message.from_user.id:
            return await message.reply("❌ Нельзя выдавать себе.")
        add_user(target_id)
        update_balance(target_id, amount)
        new_bal = get_user_profile(target_id)[2]
        await message.reply(
            f"🦎 Выдано {amount} 🦎 пользователю {target_id}. "
            f"Новый баланс: {new_bal} 🦎."
        )

# ----------- Старт бота ------------

async def main():
    setup_database()
    print("База данных готова, бот запускается...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
