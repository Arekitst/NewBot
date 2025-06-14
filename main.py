import logging
import random
from datetime import datetime
import os
import json
import asyncio

import aiosqlite
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandObject, or_f
from aiogram.types import CallbackQuery, Message, LabeledPrice, PreCheckoutQuery
from aiogram.enums import ChatMemberStatus
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

# --- НАСТРОЙКИ БОТА ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "7873522119:AAHWIa4R2MrexWmEi_wfUexTKtKB4GsxpXw")
DB_PATH = "bot.db"
ADMIN_IDS = [6179115044, 7189733067] # <-- НЕ ЗАБУДЬТЕ ДОБАВИТЬ СВОЙ ID

# --- НАСТРОЙКИ ИГРОВОЙ ЛОГИКИ ---
MAX_PETS = 20 # <-- НОВОЕ: Максимальное количество питомцев
QUIZ_COOLDOWN_HOURS = 5
MARRIAGE_MIN_LEVEL = 35
PET_MIN_LEVEL = 55 # Уровень для доступа к вылуплению яиц
MARRIAGE_COST = 250
PET_DEATH_DAYS = 2

PET_ACTIONS_COST = {
    "feed": 1,
    "grow": 5,
    "water": 2,
    "walk": 3,
}

# --- НАСТРОЙКА ЯИЦ И ВИДОВ ПИТОМЦЕВ ---
EGGS = {
    "common": {"name": "🥚 Обычное яйцо", "cost": 150, "rarity": "common"},
    "rare": {"name": "💎 Редкое яйцо", "cost": 500, "rarity": "rare"},
    "legendary": {"name": "⚜️ Легендарное яйцо", "cost": 1500, "rarity": "legendary"},
    "mythic": {"name": "✨ Мифическое яйцо", "cost": 5000, "rarity": "mythic"},
}

PET_SPECIES = {
    "common": [
        {"species_name": "Полоз", "images": {
            1: "https://i.ibb.co/4gRJSF4N/Gemini-Generated-Image-bbrjqrbbrjqrbbrj.png", 
            10: "https://i.ibb.co/x87LKPq2/image.png",
            35: "https://i.ibb.co/ccnTcgJX/image.png"}},
        {"species_name": "Уж", "images": {
            1: "https://i.ibb.co/qLBW0wN7/image.png", 
            10: "https://i.ibb.co/Z1fRyG8R/image.png",                              
            35: "https://i.ibb.co/Ng6pJ2wm/Gemini-Generated-Image-6z8b4s6z8b4s6z8b.png"}},
    ],
    "rare": [
        {"species_name": "Гадюка", "images": {
        1: "https://i.ibb.co/xSXPC1C7/image.png", 
            10: "https://i.ibb.co/Y4KqkSgt/image.png",
            35: "https://i.ibb.co/rRhY1nX3/image.png"}},
        {"species_name": "Эфа", "images": {
        1: "https://i.ibb.co/TDnDKDJb/image.png", 
            10: "https://i.ibb.co/XfhfSP31/image.png",
            35: "https://i.ibb.co/prvbR5Kf/image.png"}},
    ],
    "legendary": [
        {"species_name": "Питон", "images": {
            1: "https://i.ibb.co/WCXKKBF/image.png", 
            10: "https://i.ibb.co/j9Q9XZTR/image.png",
            35: "https://i.ibb.co/qYjVcqck/Gemini-Generated-Image-aofhgzaofhgzaofh.png"}},
        {"species_name": "Кобра", "images": {
            1: "https://i.ibb.co/DP5QFyJn/Gemini-Generated-Image-gzt9g3gzt9g3gzt9.png", 
            10: "https://i.ibb.co/HLS6vB21/Gemini-Generated-Image-m2l12m2l12m2l12m.png",
            35: "https://i.ibb.co/7xdG7Vmg/Gemini-Generated-Image-pcfv7cpcfv7cpcfv.png"}},
    ],
    "mythic": [
        {"species_name": "Василиск", "images": {
            1: "https://i.ibb.co/0Rtx5sb1/Gemini-Generated-Image-rxh7a8rxh7a8rxh7.png",
            10: "https://i.ibb.co/RpBs3XxM/Gemini-Generated-Image-togzv2togzv2togz.png",
            35: "https://i.ibb.co/FLCVtdVg/Gemini-Generated-Image-bfub33bfub33bfub.png"}},
    ]
}

PING_MESSAGES = [ "чем занимаешься?", "заходи на игру?", "как насчет катки?", "го общаться!", "скучно, давай поговорим?"]

# Словарь для отслеживания активности пользователей в чатах
recent_users_activity = {}

# --- ИНИЦИАЛИЗАЦИЯ ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- FSM СОСТОЯНИЯ ---
class TopupStates(StatesGroup):
    waiting_for_amount = State()

class QuizStates(StatesGroup):
    in_quiz = State()

class PetHatchStates(StatesGroup):
    waiting_for_name = State()

# --- РАБОТА С БАЗОЙ ДАННЫХ ---
async def init_db():
    async with aiosqlite.connect(DB_PATH) as conn:
        # ... (код для таблицы users остается без изменений) ...

        await conn.execute("""CREATE TABLE IF NOT EXISTS pets (
            pet_id INTEGER PRIMARY KEY AUTOINCREMENT, 
            owner_id INTEGER, 
            name TEXT,
            species TEXT, 
            pet_level INTEGER DEFAULT 1, 
            last_fed INTEGER DEFAULT 0,
            last_watered INTEGER DEFAULT 0, 
            last_grown INTEGER DEFAULT 0,
            last_walked INTEGER DEFAULT 0, 
            creation_date INTEGER
        )""")
        
        # Этот код для миграции старой таблицы, если она была с UNIQUE
        try:
            cursor = await conn.execute("PRAGMA index_list('pets')")
            indexes = await cursor.fetchall()
            unique_index_exists = any('unique' in str(idx).lower() and 'owner_id' in str(idx).lower() for idx in indexes)
            if unique_index_exists:
                logger.info("Обнаружена старая структура таблицы 'pets'. Выполняется миграция...")
                await conn.execute("CREATE TABLE pets_new AS SELECT * FROM pets")
                await conn.execute("DROP TABLE pets")
                await conn.execute("""CREATE TABLE pets (
                    pet_id INTEGER PRIMARY KEY AUTOINCREMENT, owner_id INTEGER, name TEXT,
                    species TEXT, pet_level INTEGER DEFAULT 1, last_fed INTEGER DEFAULT 0,
                    last_watered INTEGER DEFAULT 0, last_grown INTEGER DEFAULT 0,
                    last_walked INTEGER DEFAULT 0, creation_date INTEGER)""")
                await conn.execute("INSERT INTO pets SELECT * FROM pets_new")
                await conn.execute("DROP TABLE pets_new")
                logger.info("Миграция таблицы 'pets' завершена.")
        except Exception as e:
            logger.error(f"Ошибка при миграции таблицы pets: {e}")

        # ... (остальной код для user_eggs и quiz_questions остается без изменений) ...

async def populate_questions():
    questions = [
        ("Какая змея считается самой ядовитой в мире?", json.dumps(["Тайпан", "Черная мамба", "Гадюка", "Кобра"]), "Тайпан"),
        ("Какая змея самая большая в мире?", json.dumps(["Анаконда", "Сетчатый питон", "Королевская кобра", "Тигровый питон"]), "Сетчатый питон"),
        ("Есть ли у змей уши?", json.dumps(["Да, но они скрыты", "Только внутреннее ухо", "Нет", "Да, как у ящериц"]), "Только внутреннее ухо"),
        ("Какая змея откладывает самые большие яйца?", json.dumps(["Питон", "Анаконда", "Королевская кобра", "Удав"]), "Королевская кобра"),
        ("Что помогает змеям 'нюхать' языком?", json.dumps(["Орган Якобсона", "Ноздри", "Терморецепторы", "Кончик языка"]), "Орган Якобсона"),
        ("Как называется процесс сбрасывания кожи у змей?", json.dumps(["Линька", "Метаморфоза", "Регенерация", "Анабиоз"]), "Линька"),
        ("Какая змея способна 'плеваться' ядом?", json.dumps(["Ошейниковая кобра", "Гадюка Рассела", "Бушмейстер", "Эфа"]), "Ошейниковая кобра"),
        ("Сколько примерно видов змей существует в мире?", json.dumps(["Около 1000", "Около 2000", "Около 3500", "Более 5000"]), "Около 3500"),
        ("Какая из этих змей не ядовита?", json.dumps(["Молочная змея", "Коралловый аспид", "Тайпан", "Морская змея"]), "Молочная змея"),
        ("Какую скорость может развить Черная мамба?", json.dumps(["До 5 км/ч", "До 10 км/ч", "До 20 км/ч", "До 30 км/ч"]), "До 20 км/ч"),
        ("Что из этого НЕ едят змеи?", json.dumps(["Птиц", "Яйца", "Рыбу", "Траву"]), "Траву"),
        ("Какая змея известна своим 'капюшоном'?", json.dumps(["Кобра", "Мамба", "Удав", "Питон"]), "Кобра"),
    ]
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute("SELECT COUNT(*) FROM quiz_questions")
        count = (await cursor.fetchone())[0]
        if count == 0:
            await conn.executemany("INSERT INTO quiz_questions (question_text, options, correct_answer) VALUES (?, ?, ?)", questions)
            await conn.commit()
            logger.info(f"Added {len(questions)} initial questions to the database.")

async def db_execute(query, params=(), fetch=None):
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cursor = await conn.execute(query, params)
        if fetch == 'one':
            result = await cursor.fetchone()
        elif fetch == 'all':
            result = await cursor.fetchall()
        else:
            await conn.commit()
            result = None
        return result

async def get_user(user_id: int):
    return await db_execute("SELECT * FROM users WHERE user_id = ?", (user_id,), fetch='one')

async def add_user(user_id: int, username: str):
    await db_execute(
        "INSERT OR IGNORE INTO users (user_id, username, balance, level) VALUES (?, ?, 0, 0)",
        (user_id, username)
    )

async def update_user_field(user_id: int, field: str, value):
    await db_execute(f"UPDATE users SET {field} = ? WHERE user_id = ?", (value, user_id))

async def get_pet(owner_id: int):
    return await db_execute("SELECT * FROM pets WHERE owner_id = ?", (owner_id,), fetch='one')

async def create_pet(owner_id: int, name: str, species: str):
    now = int(datetime.now().timestamp())
    await db_execute("INSERT INTO pets (owner_id, name, species, last_fed, last_watered, last_grown, last_walked, creation_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                      (owner_id, name, species, now, now, now, now, now))

async def update_pet_field(owner_id: int, field: str, value):
    """Обновляет указанное поле для питомца."""
    await db_execute(f"UPDATE pets SET {field} = ? WHERE owner_id = ?", (value, owner_id))

async def delete_pet(owner_id: int):
    await db_execute("DELETE FROM pets WHERE owner_id = ?", (owner_id,))

async def get_user_eggs(owner_id: int):
    return await db_execute("SELECT * FROM user_eggs WHERE owner_id = ?", (owner_id,), fetch='all')

async def add_user_egg(owner_id: int, egg_type: str):
    await db_execute("INSERT INTO user_eggs (owner_id, egg_type) VALUES (?, ?)", (owner_id, egg_type))

async def delete_user_egg(user_egg_id: int):
    await db_execute("DELETE FROM user_eggs WHERE user_egg_id = ?", (user_egg_id,))

async def get_random_question():
    return await db_execute("SELECT * FROM quiz_questions ORDER BY RANDOM() LIMIT 1", fetch='one')

async def get_user_mention_by_id(user_id: int) -> str:
    try:
        user = await bot.get_chat(user_id)
        return f'<a href="tg://user?id={user.id}">{user.full_name}</a>'
    except TelegramBadRequest:
        return f"Пользователь (ID: {user_id})"
    except Exception as e:
        logger.error(f"Could not get user mention for {user_id}: {e}")
        return f"Пользователь (ID: {user_id})"

async def check_items(user_id: int):
    user = await get_user(user_id)
    if not user: return
    now = int(datetime.now().timestamp())
    updates = {}
    if user["prefix_end"] != 0 and user["prefix_end"] < now: updates["prefix_end"] = 0
    if user["antitar_end"] != 0 and user["antitar_end"] < now: updates["antitar_end"] = 0
    if user["vip_end"] != 0 and user["vip_end"] < now: updates["vip_end"] = 0
    for field, value in updates.items(): await update_user_field(user_id, field, value)

async def check_pet_death(owner_id: int):
    pet = await get_pet(owner_id)
    if not pet:
        return True
    now_ts = int(datetime.now().timestamp())
    death_timestamp = now_ts - (PET_DEATH_DAYS * 24 * 3600)
    last_action_time = max(pet['last_fed'] or 0, pet['last_watered'] or 0, pet['last_walked'] or 0)
    
    if last_action_time > death_timestamp:
        return True

    await delete_pet(owner_id)
    try:
        await bot.send_message(owner_id, f"💔 Ваш питомец {pet['name']} ({pet['species']}) умер от недостатка ухода...")
    except Exception as e:
        logger.error(f"Не удалось отправить сообщение о смерти питомца пользователю {owner_id}: {e}")
    return False

# --- ОБРАБОТЧИКИ КОМАНД ---
@dp.message(or_f(Command("start", "help", "старт", "помощь"), F.text.lower().in_(['start', 'help', 'старт', 'помощь'])))
async def cmd_start(message: Message):
    try:
        user_id = message.from_user.id
        username = message.from_user.username or message.from_user.full_name
        await add_user(user_id, username)
        
        if message.chat.type == 'private':
            tutorial_text = (
                "👋 Приветствуем в змеином боте!\n\n"
                "**Основные команды:**\n"
                "▫️ `/profile` или `профиль` — посмотреть свой профиль.\n"
                "▫️ `/hunt` или `охота` — отправиться на охоту.\n"
                "▫️ `/pay` или `перевод` (в ответ) — перевести ящерок.\n"
                "▫️ `/shop` или `магазин` — купить улучшения.\n"
                "▫️ `/topup` или `пополнить` — пополнить баланс за Telegram ★.\n\n"
                "**Игровые механики:**\n"
                "🐍 `/quiz` или `викторина` - пройти викторину.\n"
                "💖 `/marry` или `женить` (в ответ) - сделать предложение.\n"
                "🥚 `/eggshop` или `магазиняиц` - магазин яиц.\n"
                "🧺 `/myeggs` или `моияйца` - посмотреть свои яйца.\n"
                "🐾 `/mypet` или `мойпитомец` - управление питомцем.\n"
                "📞 `/ping` или `пинг` - позвать игрока в чате.\n\n"
                "**Команды отношений:**\n"
                "💍 `/accept` или `принять` - принять предложение.\n"
                "💔 `/divorce` или `развод` - разорвать отношения."
            )
            await message.answer(tutorial_text)
        else:
            await message.answer("🐍 Змеиный бот к вашим услугам! Чтобы посмотреть список команд, напишите мне в личные сообщения.")

    except Exception as e:
        logger.exception(f"Error in start command: {e}")
        await message.answer("Произошла ошибка при регистрации. Попробуйте снова.")

@dp.message(or_f(Command("profile", "профиль"), F.text.lower().in_(['profile', 'профиль'])))
async def cmd_profile(message: Message):
    try:
        target_user_msg = message.reply_to_message or message
        user_id = target_user_msg.from_user.id
        username = target_user_msg.from_user.username or target_user_msg.from_user.full_name

        # 🔧 Добавим юзера, если не существует
        await add_user(user_id, username)
        user = await get_user(user_id)
        if not user:
            await message.answer("Профиль не найден и не удалось создать.")
            return

        # 🔧 Заполним нулями пустые поля
        defaulted = {
            "balance": 0,
            "level": 0,
            "prefix_end": 0,
            "antitar_end": 0,
            "vip_end": 0,
            "partner_id": 0,
        }
        for key, default in defaulted.items():
            if user[key] is None:
                await update_user_field(user_id, key, default)

        await check_items(user_id)
        user = await get_user(user_id)  # ещё раз после обновлений

        balance = user["balance"]
        level = user["level"]

        now = int(datetime.now().timestamp())
        def format_item(end_timestamp):
            if end_timestamp and end_timestamp > now:
                dt = datetime.fromtimestamp(end_timestamp)
                return f"активен до {dt.strftime('%d.%m.%Y %H:%M')}"
            return "отсутствует"

        partner_status = "в активном поиске"
        if user["partner_id"]:
            partner_name = await get_user_mention_by_id(user['partner_id'])
            partner_status = f"в отношениях с {partner_name}"

        profile_title = "👤 Ваш профиль" if user_id == message.from_user.id else f"👤 Профиль {target_user_msg.from_user.full_name}"

        text = (
            f"{profile_title}:\n"
            f"Уровень: {level} 🐍\n"
            f"Баланс: {balance} 🦎\n"
            f"Статус: {partner_status}\n\n"
            f"Префикс: {format_item(user['prefix_end'])}\n"
            f"Антитар: {format_item(user['antitar_end'])}\n"
            f"VIP: {format_item(user['vip_end'])}"
        )

        kb = InlineKeyboardBuilder()
        kb.add(types.InlineKeyboardButton(text="🐍 Пройти викторину", callback_data="start_quiz"))
        kb.add(types.InlineKeyboardButton(text="🐾 Мой питомец", callback_data="my_pet_profile"))
        kb.add(types.InlineKeyboardButton(text="🛒 Магазин", callback_data="shop_main"))
        kb.adjust(1)

        await message.answer(text, reply_markup=kb.as_markup(), parse_mode="HTML")
    except Exception as e:
        logger.exception(f"Ошибка в команде /profile: {e}")
        await message.answer("⚠️ Произошла ошибка при получении профиля.")
@dp.message(or_f(Command("hunt", "охота"), F.text.lower().in_(['hunt', 'охота'])))
async def cmd_hunt(message: Message):
    user_id = message.from_user.id
    await add_user(user_id, message.from_user.username or message.from_user.full_name)
    user = await get_user(user_id)
    now = int(datetime.now().timestamp())
    last_hunt = user["last_hunt"] or 0
    cooldown = 24 * 3600 
    if now - last_hunt < cooldown:
        remaining = cooldown - (now - last_hunt)
        hours, remainder = divmod(remaining, 3600)
        minutes, _ = divmod(remainder, 60)
        await message.answer(f"⏳ Охота недоступна. Попробуйте через {int(hours)} ч {int(minutes)} мин.")
        return
    catch = random.randint(1, 10)
    current_balance = user["balance"] or 0
    new_balance = current_balance + catch
    await update_user_field(user_id, "balance", new_balance)
    await update_user_field(user_id, "last_hunt", now)
    await message.answer(f"🎉 Вы отправились на охоту и поймали {catch} 🦎!\nВаш новый баланс: {new_balance} 🦎")

@dp.message(or_f(Command("pay", "перевод"), F.text.lower().startswith(('pay ', 'перевод '))))
async def cmd_pay(message: Message, command: CommandObject = None):
    if message.chat.type == 'private':
        await message.answer("Эту команду нужно использовать в группе, отвечая на сообщение пользователя.")
        return

    if not message.reply_to_message or message.reply_to_message.from_user.is_bot or message.reply_to_message.from_user.id == message.from_user.id:
        await message.reply("❗️ **Ошибка:**\nИспользуйте эту команду в ответ на сообщение другого пользователя.")
        return
    
    args = None
    if command:
        args = command.args
    else:
        parts = message.text.split(maxsplit=1)
        args = parts[1] if len(parts) > 1 else None

    if args is None:
        await message.reply("❗️ **Ошибка:**\nУкажите сумму для перевода. Пример: `перевод 50`")
        return

    try:
        amount = int(args)
        if amount <= 0:
            raise ValueError
    except (TypeError, ValueError):
        await message.reply("❗️ **Ошибка:**\nНеверный формат суммы. Укажите положительное число. Пример: `перевод 50`")
        return
        
    sender = message.from_user
    recipient = message.reply_to_message.from_user
        
    await add_user(sender.id, sender.username or sender.full_name)
    await add_user(recipient.id, recipient.username or recipient.full_name)
    sender_data = await get_user(sender.id)
    
    sender_balance = sender_data['balance'] or 0
    if sender_balance < amount:
        await message.reply(f"❌ **Недостаточно средств!**\nУ вас на балансе всего {sender_balance} 🦎.")
        return
        
    recipient_data = await get_user(recipient.id)
    recipient_balance = recipient_data['balance'] or 0
    await update_user_field(sender.id, "balance", sender_balance - amount)
    await update_user_field(recipient.id, "balance", recipient_balance + amount)
    
    sender_mention = await get_user_mention_by_id(sender.id)
    recipient_mention = await get_user_mention_by_id(recipient.id)
    await message.answer(f"💸 **Перевод успешен!**\n\n{sender_mention} перевел(а) {amount} 🦎 пользователю {recipient_mention}.", parse_mode="HTML")

# --- АДМИН-КОМАНДЫ ---
@dp.message(or_f(Command("give", "выдать"), F.text.lower().startswith(('give ', 'выдать '))))
async def cmd_give(message: Message, command: CommandObject = None):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет прав для использования этой команды.")
        return

    args = command.args if command else (message.text.split(maxsplit=1)[1] if ' ' in message.text else None)

    if not args:
        await message.answer("❗️ Ошибка: не указаны аргументы.\nИспользование: `give <user_id> <amount>`")
        return
        
    try:
        arg_parts = args.split()
        if len(arg_parts) != 2: raise ValueError("Неверное количество аргументов")
        target_id, amount = int(arg_parts[0]), int(arg_parts[1])
        if amount <= 0: raise ValueError("Сумма должна быть положительной")
    except ValueError:
        await message.answer("❗️ Ошибка в аргументах.\nИспользование: `give <user_id> <сумма>`")
        return
        
    target_user = await get_user(target_id)
    if not target_user:
        await add_user(target_id, f"User {target_id}")
        target_user = await get_user(target_id)

    current_balance = target_user["balance"] or 0
    new_balance = current_balance + amount
    await update_user_field(target_id, "balance", new_balance)
    await message.answer(f"✅ Выдали {amount} 🦎 пользователю с ID {target_id}. Новый баланс: {new_balance} 🦎")

@dp.message(or_f(Command("take", "забрать"), F.text.lower().startswith(('take ', 'забрать '))))
async def cmd_take(message: Message, command: CommandObject = None):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет прав для использования этой команды.")
        return

    args = command.args if command else (message.text.split(maxsplit=1)[1] if ' ' in message.text else None)

    if not args:
        await message.answer("❗️ Ошибка: не указаны аргументы.\nИспользование: `take <user_id> <amount|all>`")
        return

    try:
        arg_parts = args.split()
        if len(arg_parts) != 2: raise ValueError("Неверное количество аргументов")
        
        target_id = int(arg_parts[0])
        amount_str = arg_parts[1]

        target_user = await get_user(target_id)
        if not target_user:
            await message.answer(f"Пользователь с ID {target_id} не найден.")
            return
        
        target_balance = target_user['balance'] or 0

        if amount_str.lower() == 'all':
            amount_to_take = target_balance
        elif amount_str.isdigit():
            amount_to_take = int(amount_str)
        else:
            raise ValueError("Неверный формат суммы")

        if amount_to_take <= 0:
             raise ValueError("Сумма должна быть положительной")

        if amount_to_take > target_balance:
            await message.answer(f"Нельзя забрать больше, чем есть у пользователя ({target_balance} 🦎).")
            return

        new_balance = target_balance - amount_to_take
        await update_user_field(target_id, 'balance', new_balance)
        await message.answer(f"✅ У пользователя ID {target_id} было изъято {amount_to_take} 🦎. Новый баланс: {new_balance} 🦎.")

    except ValueError:
        await message.answer("❗️ Ошибка в аргументах.\nИспользование: `take <user_id> <сумма|all>`")
    except Exception as e:
        logger.error(f"Error in /take command: {e}")
        await message.answer("Непредвиденная ошибка выполнения команды.")

@dp.message(or_f(Command("givelevel", "выдатьуровень"), F.text.lower().startswith(('givelevel ', 'выдатьуровень '))))
async def cmd_givelevel(message: Message, command: CommandObject = None):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет прав для использования этой команды.")
        return
    
    args = command.args if command else (message.text.split(maxsplit=1)[1] if ' ' in message.text else None)

    if not args:
        await message.answer("❗️ Ошибка: не указаны аргументы.\nИспользование: `givelevel <user_id> <level>`")
        return

    try:
        arg_parts = args.split()
        if len(arg_parts) != 2 or not arg_parts[1].isdigit(): raise ValueError
        target_id, level = int(arg_parts[0]), int(arg_parts[1])
        
        target_user = await get_user(target_id)
        if not target_user:
            await add_user(target_id, "Unknown by admin")
        
        await update_user_field(target_id, 'level', level)
        await message.answer(f"✅ Пользователю ID {target_id} установлен {level} уровень.")
    except ValueError:
        await message.answer("❗️ Ошибка в аргументах.\nИспользование: `givelevel <user_id> <уровень>`")
    except Exception as e:
        logger.error(f"Error in /givelevel command: {e}")
        await message.answer("Непредвиденная ошибка выполнения команды.")

@dp.message(or_f(Command("giveegg", "выдатьяйцо"), F.text.lower().startswith(('giveegg ', 'выдатьяйцо '))))
async def cmd_giveegg(message: Message, command: CommandObject = None):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет прав для использования этой команды.")
        return

    args = command.args if command else (message.text.split(maxsplit=1)[1] if ' ' in message.text else None)

    if not args:
        await message.answer(f"❗️ Ошибка: не указаны аргументы.\nИспользование: `giveegg <user_id> <type>`\nДоступные типы: {', '.join(EGGS.keys())}")
        return

    try:
        arg_parts = args.split()
        if len(arg_parts) != 2 or arg_parts[1] not in EGGS: raise ValueError
        target_id, egg_type = int(arg_parts[0]), arg_parts[1]

        target_user = await get_user(target_id)
        if not target_user:
            await add_user(target_id, "Unknown by admin")
        
        await add_user_egg(target_id, egg_type)
        await message.answer(f"✅ Пользователю ID {target_id} выдано яйцо типа '{egg_type}'.")

    except ValueError:
        await message.answer(f"❗️ Ошибка в аргументах.\nИспользование: `giveegg <user_id> <type>`\nДоступные типы: {', '.join(EGGS.keys())}")
    except Exception as e:
        logger.error(f"Error in /giveegg command: {e}")
        await message.answer("Непредвиденная ошибка выполнения команды.")

# --- СИСТЕМА ВИКТОРИНЫ ---
@dp.message(or_f(Command("quiz", "викторина"), F.text.lower().in_(['quiz', 'викторина'])))
async def cmd_quiz(message: Message, state: FSMContext):
    await start_quiz_logic(message.from_user.id, message, state)

@dp.callback_query(F.data == "start_quiz")
async def cb_start_quiz(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await start_quiz_logic(callback.from_user.id, callback, state, is_callback=True)

async def start_quiz_logic(user_id: int, event: Message | CallbackQuery, state: FSMContext, is_callback: bool = False):
    user = await get_user(user_id)
    message = event if not is_callback else event.message
    
    if not user:
        await add_user(user_id, event.from_user.username or event.from_user.full_name)
        user = await get_user(user_id)

    now = int(datetime.now().timestamp())
    last_quiz = user['last_quiz'] or 0
    cooldown = QUIZ_COOLDOWN_HOURS * 3600

    if now - last_quiz < cooldown:
        remaining = cooldown - (now - last_quiz)
        hours, remainder = divmod(remaining, 3600)
        minutes, _ = divmod(remainder, 60)
        text = f"⏳ Вы уже проходили викторину. Следующая попытка через {int(hours)} ч {int(minutes)} мин."
        if is_callback:
            await event.answer(text, show_alert=True)
        else:
            await message.answer(text)
        return

    question_data = await get_random_question()
    if not question_data:
        text = "В базе данных пока нет вопросов для викторины. Зайдите позже!"
        if is_callback:
            await event.answer(text, show_alert=True)
        else:
            await message.answer(text)
        return
    
    await state.set_state(QuizStates.in_quiz)
    await state.update_data(question_id=question_data['question_id'], correct_answer=question_data['correct_answer'])

    options = json.loads(question_data['options'])
    random.shuffle(options)
    
    kb = InlineKeyboardBuilder()
    for option in options:
        kb.add(types.InlineKeyboardButton(text=option, callback_data=f"quiz_answer:{option}"))
    kb.adjust(1)
    
    text = f"🐍 **Вопрос викторины:**\n\n{question_data['question_text']}"
    try:
        if is_callback:
            await message.edit_text(text, reply_markup=kb.as_markup())
        else:
            await message.answer(text, reply_markup=kb.as_markup())
    except TelegramBadRequest:
        pass

@dp.callback_query(QuizStates.in_quiz, F.data.startswith("quiz_answer:"))
async def cb_process_quiz_answer(callback: CallbackQuery, state: FSMContext):
    user_answer = callback.data.split(":", 1)[1]
    quiz_data = await state.get_data()
    correct_answer = quiz_data.get('correct_answer')
    
    if not correct_answer:
        await callback.answer()
        return

    user = await get_user(callback.from_user.id)
    current_level = user['level'] or 0

    if user_answer == correct_answer:
        new_level = current_level + 1
        result_text = f"✅ **Правильно!**\n\nВаш уровень повышен: {current_level} ➡️ {new_level}"
    else:
        new_level = max(0, current_level - 1)
        result_text = f"❌ **Неверно!** Правильный ответ: {correct_answer}\n\nВаш уровень понижен: {current_level} ➡️ {new_level}"

    await update_user_field(callback.from_user.id, 'level', new_level)
    await update_user_field(callback.from_user.id, 'last_quiz', int(datetime.now().timestamp()))
    await state.clear()
    
    await callback.message.edit_text(result_text, reply_markup=None)
    await callback.answer()

# --- СИСТЕМА ПИТОМЦЕВ ---
@dp.message(or_f(Command("eggshop", "магазиняиц"), F.text.lower().in_(['eggshop', 'магазиняиц'])))
async def cmd_eggshop(message: Message):
    text = "🥚 **Магазин яиц** 🥚\n\nВыберите яйцо, чтобы приобрести его:"
    kb = InlineKeyboardBuilder()
    for egg_type, data in EGGS.items():
        kb.add(types.InlineKeyboardButton(text=f"{data['name']} ({data['cost']} 🦎)", callback_data=f"buy_egg:{egg_type}"))
    kb.adjust(1)
    await message.answer(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("buy_egg:"))
async def cb_buy_egg(callback: CallbackQuery):
    egg_type = callback.data.split(":")[1]
    egg_data = EGGS.get(egg_type)
    if not egg_data: return await callback.answer("Такое яйцо не найдено!", show_alert=True)
    
    user_id = callback.from_user.id
    user = await get_user(user_id)
    user_balance = user['balance'] or 0
    
    if user_balance < egg_data['cost']:
        return await callback.answer(f"Недостаточно средств! Нужно {egg_data['cost']} 🦎.", show_alert=True)
    
    await update_user_field(user_id, 'balance', user_balance - egg_data['cost'])
    await add_user_egg(user_id, egg_type)
    
    await callback.answer(f"Вы успешно купили {egg_data['name']}!", show_alert=True)
    await callback.message.answer(f"🎉 Вы приобрели {egg_data['name']}! Посмотреть свои яйца и вылупить питомца можно по команде /myeggs или моияйца.")

@dp.message(or_f(Command("myeggs", "моияйца"), F.text.lower().in_(['myeggs', 'моияйца'])))
async def cmd_myeggs(message: Message):
    user_id = message.from_user.id
    user_eggs = await get_user_eggs(user_id)
    
    if not user_eggs:
        return await message.answer("У вас нет купленных яиц. Загляните в /eggshop или магазин яиц!")
        
    text = "🧺 **Ваши яйца** 🧺\n\nНажмите на кнопку, чтобы вылупить питомца:"
    kb = InlineKeyboardBuilder()
    for egg in user_eggs:
        egg_data = EGGS.get(egg['egg_type'])
        if egg_data:
            kb.add(types.InlineKeyboardButton(text=f"Вылупить {egg_data['name']}", callback_data=f"hatch_egg:{egg['user_egg_id']}"))
    kb.adjust(1)
    await message.answer(text, reply_markup=kb.as_markup())
    
@dp.callback_query(F.data.startswith("hatch_egg:"))
async def cb_hatch_egg(callback: CallbackQuery, state: FSMContext):
    user_egg_id = int(callback.data.split(":")[1])
    user_id = callback.from_user.id
    
    user = await get_user(user_id)
    if (user['level'] or 0) < PET_MIN_LEVEL:
        return await callback.answer(f"Вылуплять питомцев можно только с {PET_MIN_LEVEL} уровня!", show_alert=True)
        
    pet = await get_pet(user_id)
    if pet:
        return await callback.answer("У вас уже есть питомец! Нельзя завести второго.", show_alert=True)
        
    user_eggs = await get_user_eggs(user_id)
    target_egg = next((e for e in user_eggs if e['user_egg_id'] == user_egg_id), None)
    
    if not target_egg:
        await callback.message.edit_text("Этого яйца у вас больше нет.")
        return await callback.answer()
        
    await state.set_state(PetHatchStates.waiting_for_name)
    await state.update_data(user_egg_id=user_egg_id, egg_type=target_egg['egg_type'])
    
    await callback.message.edit_text("Отлично! Как вы назовете своего нового питомца? Введите имя (до 15 символов).")
    await callback.answer()

@dp.message(PetHatchStates.waiting_for_name)
async def process_pet_name_after_hatch(message: Message, state: FSMContext):
    pet_name = message.text
    if len(pet_name) > 15:
        return await message.answer("Имя слишком длинное. Попробуйте еще раз (до 15 символов).")

    hatch_data = await state.get_data()
    user_egg_id = hatch_data['user_egg_id']
    egg_type = hatch_data['egg_type']
    
    egg_rarity = EGGS[egg_type]['rarity']
    possible_species = PET_SPECIES[egg_rarity]
    hatched_species_data = random.choice(possible_species)
    hatched_species_name = hatched_species_data['species_name']
    
    await delete_user_egg(user_egg_id)
    await create_pet(message.from_user.id, pet_name, hatched_species_name)
    await state.clear()
    
    await message.answer(f"🎉 Из яйца вылупился **{hatched_species_name}**!\nВы назвали его **{pet_name}**.\n\nПоздравляем! Заботьтесь о нем с помощью команды /mypet или мойпитомец.")

@dp.message(or_f(Command("mypet", "мойпитомец"), F.text.lower().in_(['mypet', 'мойпитомец'])))
async def cmd_mypet(message: Message):
    await my_pet_profile_logic(message.from_user.id, message)

@dp.callback_query(F.data == "my_pet_profile")
async def cb_mypet(callback: CallbackQuery):
    await my_pet_profile_logic(callback.from_user.id, callback, is_callback=True)

async def my_pet_profile_logic(user_id: int, event: Message | CallbackQuery, is_callback: bool = False):
    if is_callback:
        await event.answer()
    
    message = event if not is_callback else event.message
        
    if not await check_pet_death(user_id):
        if is_callback:
            try: await message.delete()
            except: pass
        return

    pet = await get_pet(user_id)
    if not pet:
        kb = InlineKeyboardBuilder().add(types.InlineKeyboardButton(text="🥚 В магазин яиц", callback_data="go_to_eggshop"))
        text = "У вас еще нет питомца. Загляните в магазин яиц, чтобы завести своего!"
        if is_callback and message.photo:
            await message.delete()
            await message.answer(text, reply_markup=kb.as_markup())
        elif is_callback:
            await message.edit_text(text, reply_markup=kb.as_markup())
        else:
            await message.answer(text, reply_markup=kb.as_markup())
        return

    now_ts = int(datetime.now().timestamp())
    pet_level = pet['pet_level']
    pet_species = pet['species']
    
    def format_time_since(timestamp):
        if not timestamp: return "никогда"
        dt_obj = datetime.fromtimestamp(timestamp)
        return dt_obj.strftime('%d.%m %H:%M')

    caption = (
        f"🐾 **Питомец: {pet['name']}** ({pet_species})\n\n"
        f"Уровень: {pet_level}\n"
        f"Корм: {format_time_since(pet['last_fed'])}\n"
    )
    if pet_level >= 10:
        caption += f"Вода: {format_time_since(pet['last_watered'])}\n"
    if pet_level >= 15:
        caption += f"Прогулка: {format_time_since(pet['last_walked'])}\n"

    kb = InlineKeyboardBuilder()
    kb.add(types.InlineKeyboardButton(text=f"Покормить ({PET_ACTIONS_COST['feed']}🦎)", callback_data="pet_action:feed"))
    
    grow_cooldown_ok = now_ts - (pet['last_grown'] or 0) > 24 * 3600
    grow_btn_text = f"Растить ({PET_ACTIONS_COST['grow']}🦎)" if grow_cooldown_ok else "Растить (КД)"
    kb.add(types.InlineKeyboardButton(text=grow_btn_text, callback_data="pet_action:grow"))

    if pet_level >= 10:
        kb.add(types.InlineKeyboardButton(text=f"Поить ({PET_ACTIONS_COST['water']}🦎)", callback_data="pet_action:water"))
    if pet_level >= 15:
        kb.add(types.InlineKeyboardButton(text=f"Выгуливать ({PET_ACTIONS_COST['walk']}🦎)", callback_data="pet_action:walk"))
    
    kb.adjust(2)

    image_url = "https://i.imgur.com/3TSa7A0.png"
    species_data = next((s for rarity in PET_SPECIES.values() for s in rarity if s['species_name'] == pet_species), None)
    if species_data:
        for level_threshold, url in sorted(species_data['images'].items(), reverse=True):
            if pet_level >= level_threshold:
                image_url = url
                break
    
    try:
        if is_callback:
            media = types.InputMediaPhoto(media=image_url, caption=caption)
            await message.edit_media(media=media, reply_markup=kb.as_markup())
        else:
            await bot.send_photo(user_id, photo=image_url, caption=caption, reply_markup=kb.as_markup())
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            if is_callback: await event.answer("Данные питомца не изменились.")
        else:
            logger.error(f"Failed to edit pet profile, sending new: {e}")
            await bot.send_photo(user_id, photo=image_url, caption=caption, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "go_to_eggshop")
async def cb_go_to_eggshop(callback: CallbackQuery):
    await callback.message.delete()
    await cmd_eggshop(callback.message)
    await callback.answer()

@dp.callback_query(F.data.startswith("pet_action:"))
async def cb_pet_action(callback: CallbackQuery):
    action = callback.data.split(":")[1]
    user_id = callback.from_user.id
    
    if not await check_pet_death(user_id):
        try: await callback.message.delete()
        except: pass
        await callback.answer("Ваш питомец умер...", show_alert=True)
        return

    pet = await get_pet(user_id)
    user = await get_user(user_id)
    cost = PET_ACTIONS_COST.get(action, 0)
    
    user_balance = user['balance'] or 0
    if user_balance < cost:
        await callback.answer(f"Недостаточно ящерок! Нужно {cost} 🦎.", show_alert=True)
        return

    now = int(datetime.now().timestamp())
    
    result_text = ""
    if action == "grow":
        if now - (pet['last_grown'] or 0) < 24 * 3600:
            await callback.answer("Растить питомца можно только раз в день!", show_alert=True)
            return
        await db_execute("UPDATE pets SET pet_level = ?, last_grown = ? WHERE owner_id = ?", (pet['pet_level'] + 1, now, user_id))
        result_text = f"Вы вырастили своего питомца! Его новый уровень: {pet['pet_level'] + 1}."
    elif action == "feed":
        await update_pet_field(user_id, "last_fed", now)
        result_text = "Вы покормили питомца."
    elif action == "water":
        if pet['pet_level'] < 10:
            await callback.answer("Эта функция доступна с 10 уровня питомца!", show_alert=True)
            return
        await update_pet_field(user_id, "last_watered", now)
        result_text = "Вы напоили питомца."
    elif action == "walk":
        if pet['pet_level'] < 15:
            await callback.answer("Эта функция доступна с 15 уровня питомца!", show_alert=True)
            return
        await update_pet_field(user_id, "last_walked", now)
        result_text = "Вы выгуляли питомца."
    else:
        return

    await update_user_field(user_id, "balance", user_balance - cost)
    await callback.answer(result_text, show_alert=False)
    await my_pet_profile_logic(user_id, callback, is_callback=True)

# --- СИСТЕМА МАГАЗИНА ---
SHOP_ITEMS = {"prefix": {"name": "Префикс", "prices": {1: 20, 3: 40, 7: 100}}, "antitar": {"name": "Антитар", "prices": {1: 30, 3: 60, 7: 130}}, "vip": {"name": "VIP", "prices": {1: 50, 3: 100, 7: 300}}}

def create_shop_menu():
    kb = InlineKeyboardBuilder()
    for item_id, item_data in SHOP_ITEMS.items():
        kb.add(types.InlineKeyboardButton(text=item_data["name"], callback_data=f"shop_item:{item_id}"))
    kb.adjust(1)
    return kb.as_markup()

def create_item_menu(item_id: str):
    kb = InlineKeyboardBuilder()
    item_data = SHOP_ITEMS[item_id]
    for days, price in item_data["prices"].items():
        kb.add(types.InlineKeyboardButton(text=f"{days} дн. - {price} 🦎", callback_data=f"buy:{item_id}:{days}"))
    kb.add(types.InlineKeyboardButton(text="⬅️ Назад", callback_data="shop_main"))
    kb.adjust(1)
    return kb.as_markup()

@dp.message(or_f(Command("shop", "магазин"), F.text.lower().in_(['shop', 'магазин'])))
async def cmd_shop(message: Message):
    await message.answer("🛒 Магазин: выберите предмет для покупки.", reply_markup=create_shop_menu())

@dp.callback_query(F.data == "shop_main")
async def cb_shop_main(callback: CallbackQuery):
    await callback.message.edit_text("🛒 Магазин: выберите предмет для покупки.", reply_markup=create_shop_menu())
    await callback.answer()

@dp.callback_query(F.data.startswith("shop_item:"))
async def cb_shop_item(callback: CallbackQuery):
    item_id = callback.data.split(":")[1]
    item_name = SHOP_ITEMS[item_id]["name"]
    await callback.message.edit_text(f"Выберите срок для покупки товара «{item_name}»:", reply_markup=create_item_menu(item_id))
    await callback.answer()

@dp.callback_query(F.data.startswith("buy:"))
async def cb_buy_item(callback: CallbackQuery):
    try:
        _, item_id, days_str = callback.data.split(":")
        days = int(days_str)
        user_id = callback.from_user.id
        item_data = SHOP_ITEMS.get(item_id)
        if not item_data:
            return await callback.answer("Ошибка: товар не найден.", show_alert=True)
        price = item_data["prices"].get(days)
        item_name = item_data["name"]
        if price is None:
            return await callback.answer("Ошибка: цена не найдена.", show_alert=True)
        user = await get_user(user_id)
        if not user:
            return await callback.answer("Сначала напишите /start", show_alert=True)
        
        user_balance = user["balance"] or 0
        if user_balance < price:
            await callback.answer(f"❌ У вас недостаточно 🦎 (у вас {user_balance}, требуется {price}).", show_alert=True)
            return
        
        new_balance = user_balance - price
        await update_user_field(user_id, "balance", new_balance)
        now_ts = int(datetime.now().timestamp())
        add_seconds = days * 24 * 3600
        field_name = f"{item_id}_end"
        current_end = user[field_name] or 0
        new_end = max(current_end, now_ts) + add_seconds
        await update_user_field(user_id, field_name, new_end)
        await callback.message.edit_text(f"✅ Покупка успешна! Вы приобрели «{item_name}».\nВаш новый баланс: {new_balance} 🦎")
        await callback.answer()
    except Exception as e:
        logger.exception(f"Error in buy handler: {e}")
        await callback.answer("Произошла ошибка при покупке.", show_alert=True)

# --- СИСТЕМА ПОПОЛНЕНИЯ ЧЕРЕЗ TELEGRAM STARS ---
@dp.message(or_f(Command("topup", "пополнить"), F.text.lower().in_(['topup', 'пополнить'])))
async def cmd_topup(message: Message, state: FSMContext):
    await message.answer("Введите количество ящерок, которое вы хотите купить.\n\n▫️ **Курс:** 3 ящерки = 1 ★\n▫️ **Лимиты:** от 20 до 10 000 ящерок за раз.\n▫️ Количество должно быть кратно 3.\n\nДля отмены просто напишите /cancel или отмена.")
    await state.set_state(TopupStates.waiting_for_amount)

@dp.message(or_f(Command("cancel", "отмена"), F.text.lower().in_(['cancel', 'отмена'])), F.state == TopupStates.waiting_for_amount)
async def cancel_topup(message: Message, state: FSMContext):
    await message.answer("Действие отменено.")
    await state.clear()

@dp.message(TopupStates.waiting_for_amount)
async def process_topup_amount(message: Message, state: FSMContext):
    try:
        lizards_to_buy = int(message.text)
    except ValueError:
        return await message.answer("❌ Пожалуйста, введите целое число.")
    if not (20 <= lizards_to_buy <= 10000):
        return await message.answer("❌ Вы можете купить от 20 до 10 000 ящерок за раз.")
    if lizards_to_buy % 3 != 0:
        lower = (lizards_to_buy // 3) * 3
        upper = lower + 3
        return await message.answer(f"❌ Количество ящерок должно быть кратно 3.\n\nВы можете купить, например, {lower if lower >= 20 else upper} или {upper} 🦎.")
    stars_price = lizards_to_buy // 3
    await state.clear()
    await bot.send_invoice(chat_id=message.from_user.id, title=f"Покупка {lizards_to_buy} 🦎", description=f"Пакет из {lizards_to_buy} ящерок для вашего баланса в боте.", payload=f"lizard_topup:{message.from_user.id}:{lizards_to_buy}", currency="XTR", prices=[LabeledPrice(label=f"{lizards_to_buy} 🦎", amount=stars_price)])

@dp.pre_checkout_query()
async def pre_checkout_query_handler(pre_checkout_query: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)

@dp.message(F.successful_payment)
async def successful_payment_handler(message: Message):
    try:
        payload = message.successful_payment.invoice_payload
        _, user_id_str, lizards_str = payload.split(":")
        user_id = int(user_id_str)
        lizards_to_add = int(lizards_str)
        await add_user(user_id, message.from_user.username or message.from_user.full_name)
        user = await get_user(user_id)
        current_balance = user['balance'] or 0
        new_balance = current_balance + lizards_to_add
        await update_user_field(user_id, 'balance', new_balance)
        await bot.send_message(chat_id=user_id, text=f"✅ Оплата прошла успешно!\n\nВам начислено: {lizards_to_add} 🦎\nВаш новый баланс: {new_balance} �")
    except Exception as e:
        logger.error(f"Error in successful_payment_handler: {e}")
        await bot.send_message(chat_id=message.from_user.id, text="Произошла ошибка при начислении ящерок. Пожалуйста, свяжитесь с администратором.")

# --- СИСТЕМА БРАКОВ ---
@dp.message(or_f(Command("marry", "женить"), F.text.lower().in_(['marry', 'женить'])))
async def cmd_marry(message: Message):
    if message.chat.type == 'private':
        await message.answer("Эту команду нужно использовать в группе, отвечая на сообщение пользователя.")
        return

    if not message.reply_to_message or message.reply_to_message.from_user.is_bot or message.reply_to_message.from_user.id == message.from_user.id:
        return await message.reply("Чтобы сделать предложение, используйте эту команду в ответ на сообщение другого пользователя.")
        
    proposer = message.from_user
    target = message.reply_to_message.from_user

    await add_user(proposer.id, proposer.username or proposer.full_name)
    await add_user(target.id, target.username or target.full_name)
    proposer_data = await get_user(proposer.id)
    target_data = await get_user(target.id)
    
    if (proposer_data['level'] or 0) < MARRIAGE_MIN_LEVEL:
        return await message.reply(f"❌ Для вступления в брак нужен {MARRIAGE_MIN_LEVEL} уровень. Ваш уровень: {proposer_data['level'] or 0}.")
    if (target_data['level'] or 0) < MARRIAGE_MIN_LEVEL:
        return await message.reply(f"❌ У пользователя {target.full_name} недостаточный уровень для брака ({target_data['level'] or 0}/{MARRIAGE_MIN_LEVEL}).")
    
    if proposer_data['partner_id']:
        return await message.reply("Вы уже состоите в отношениях.")
    if (proposer_data['balance'] or 0) < MARRIAGE_COST:
        return await message.reply(f"❌ Для предложения нужно {MARRIAGE_COST} 🦎.\nУ вас на балансе: {proposer_data['balance'] or 0} 🦎.")
    if target_data['partner_id']:
        return await message.reply(f"{target.full_name} уже состоит в отношениях.")
    if target_data['proposal_from_id']:
        return await message.reply(f"У {target.full_name} уже есть активное предложение. Дождитесь ответа.")

    kb = InlineKeyboardBuilder()
    kb.add(types.InlineKeyboardButton(text="Да, я уверен", callback_data=f"marry_confirm:{proposer.id}:{target.id}"))
    kb.add(types.InlineKeyboardButton(text="Отмена", callback_data="marry_cancel"))
    target_mention = await get_user_mention_by_id(target.id)
    await message.reply(f"Вы уверены, что хотите сделать предложение {target_mention}?\nСтоимость этого действия: {MARRIAGE_COST} 🦎.\n\nЭто действие нельзя будет отменить.", reply_markup=kb.as_markup(), parse_mode="HTML")

@dp.message(or_f(Command("accept", "принять"), F.text.lower().in_(['accept', 'принять'])))
async def cmd_accept(message: Message):
    user_id = message.from_user.id
    await add_user(user_id, message.from_user.username or message.from_user.full_name)
    user_data = await get_user(user_id)
    if not user_data['proposal_from_id']:
        return await message.reply("Вам никто не делал предложений.")
    proposer_id = user_data['proposal_from_id']
    proposer_data = await get_user(proposer_id)
    if not proposer_data or proposer_data['partner_id']:
        await message.reply("К сожалению, этот пользователь уже состоит в отношениях или не найден.")
        return await update_user_field(user_id, "proposal_from_id", 0)
        
    await update_user_field(user_id, "partner_id", proposer_id)
    await update_user_field(proposer_id, "partner_id", user_id)
    await update_user_field(user_id, "proposal_from_id", 0)
    user_mention = await get_user_mention_by_id(user_id)
    proposer_mention = await get_user_mention_by_id(proposer_id)
    await message.answer(f"💖 Поздравляем! {proposer_mention} и {user_mention} теперь официально состоят в отношениях! 💖", parse_mode="HTML")

@dp.message(or_f(Command("divorce", "развод"), F.text.lower().in_(['divorce', 'развод'])))
async def cmd_divorce(message: Message):
    user_id = message.from_user.id
    await add_user(user_id, message.from_user.username or message.from_user.full_name)
    user_data = await get_user(user_id)
    if not user_data['partner_id']:
        return await message.reply("Вы не состоите в отношениях, некого бросать.")
    kb = InlineKeyboardBuilder()
    kb.add(types.InlineKeyboardButton(text="Да, я хочу развестись", callback_data="confirm_divorce"))
    kb.add(types.InlineKeyboardButton(text="Отмена", callback_data="cancel_divorce"))
    await message.reply("Вы уверены, что хотите разорвать отношения? Это действие необратимо.", reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("marry_confirm:"))
async def confirm_marry(callback: CallbackQuery):
    _, proposer_id_str, target_id_str = callback.data.split(":")
    proposer_id = int(proposer_id_str)
    if callback.from_user.id != proposer_id:
        return await callback.answer("Это не ваше предложение!", show_alert=True)
    target_id = int(target_id_str)
    proposer_data = await get_user(proposer_id)
    target_data = await get_user(target_id)
    if not proposer_data or not target_data:
        await callback.message.edit_text("Ошибка: один из пользователей не найден.")
        return await callback.answer()
    if (proposer_data['balance'] or 0) < MARRIAGE_COST:
        await callback.message.edit_text(f"❌ Упс! На вашем счету больше недостаточно средств. Требуется {MARRIAGE_COST} 🦎.")
        return await callback.answer()
    if target_data['partner_id'] or target_data['proposal_from_id']:
        await callback.message.edit_text("❌ Упс! Этот пользователь уже получил предложение или вступил в отношения.")
        return await callback.answer()
    try:
        new_balance = (proposer_data['balance'] or 0) - MARRIAGE_COST
        await update_user_field(proposer_id, "balance", new_balance)
        await update_user_field(target_id, "proposal_from_id", proposer_id)
        proposer_mention = await get_user_mention_by_id(proposer_id)
        target_mention = await get_user_mention_by_id(target_id)
        await callback.message.edit_text("Предложение успешно отправлено!")
        await callback.message.answer(f"💍 {target_mention}, вам поступило предложение руки и сердца от {proposer_mention}!\n\nЧтобы принять его, напишите команду `/accept` или `принять`.", parse_mode="HTML")
        await callback.answer()
    except Exception as e:
        logger.error(f"Error during marriage confirmation: {e}")
        await callback.message.edit_text("Произошла непредвиденная ошибка. Попробуйте позже.")
        await callback.answer()

@dp.callback_query(F.data == "marry_cancel")
async def cancel_marry(callback: CallbackQuery):
    await callback.message.edit_text("Предложение отменено.")
    await callback.answer()

@dp.callback_query(F.data == "confirm_divorce")
async def confirm_divorce(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_data = await get_user(user_id)
    if not user_data or not user_data['partner_id']:
        await callback.message.edit_text("Вы не состоите в отношениях.")
        return await callback.answer()
    partner_id = user_data['partner_id']
    await update_user_field(user_id, "partner_id", 0)
    await update_user_field(partner_id, "partner_id", 0)
    user_mention = await get_user_mention_by_id(user_id)
    partner_mention = await get_user_mention_by_id(partner_id)
    await callback.message.edit_text("Отношения разорваны.")
    await callback.message.answer(f"💔 {user_mention} и {partner_mention} больше не вместе. 💔", parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "cancel_divorce")
async def cancel_divorce(callback: CallbackQuery):
    await callback.message.edit_text("Развод отменен. Ваши отношения в безопасности!")
    await callback.answer()

@dp.message(or_f(Command("ping", "пинг"), F.text.lower().in_(['ping', 'пинг'])))
async def cmd_ping(message: Message):
    """Пингует случайного активного пользователя в чате."""
    if message.chat.type not in {'group', 'supergroup'}:
        await message.reply("Эту команду можно использовать только в группах.")
        return

    chat_id = message.chat.id
    pinger_id = message.from_user.id

    if chat_id not in recent_users_activity or len(recent_users_activity[chat_id]) <= 1:
        await message.reply("Я еще не видел здесь активности, некого пинговать.")
        return

    now = datetime.now().timestamp()
    active_users_ids = [
        uid for uid, last_seen in recent_users_activity[chat_id].items()
        if (now - last_seen) < 86400 and uid != pinger_id
    ]

    if not active_users_ids:
        await message.reply("Кроме вас в последнее время никто не активничал.")
        return

    target_id = random.choice(active_users_ids)
    try:
        target_mention = await get_user_mention_by_id(target_id)
        pinger_mention = await get_user_mention_by_id(pinger_id)
        ping_text = random.choice(PING_MESSAGES)

        await message.answer(f"📞 {target_mention}, пользователь {pinger_mention} спрашивает: «{ping_text}»", parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error in ping command while getting user mentions: {e}")
        await message.reply("Не удалось выбрать пользователя для пинга.")

# --- ОБРАБОТЧИК АКТИВНОСТИ (ДЛЯ КОМАНДЫ /PING)---
@dp.message()
async def track_user_activity(message: types.Message):
    """Отслеживает активность пользователей в группах для работы /ping."""
    if message.chat.type in {'group', 'supergroup'}:
        if not message.from_user.is_bot:
            recent_users_activity.setdefault(message.chat.id, {})
            recent_users_activity[message.chat.id][message.from_user.id] = datetime.now().timestamp()

# --- ЗАПУСК БОТА ---
async def main():
    await init_db()
    await populate_questions()
    bot.default_parse_mode = "HTML"
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
