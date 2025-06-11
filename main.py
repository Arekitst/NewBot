import logging
import asyncio
import random
from datetime import datetime
import os

import aiosqlite
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandObject
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
ADMIN_IDS = [6179115044, 7189733067] # Для "супер-админ" команды /give

PING_MESSAGES = [
    "чем занимаешься?", "заходи на игру?", "как насчет катки?", "го общаться!", "скучно, давай поговорим?", "какие планы на вечер?", "что нового?", "мы скучаем по тебе!", "пора вернуться в строй!", "расскажи анекдот!", "Пойдем на крайнюю перед анабиозом🌟", "какой фильм посоветуешь?", "нужна твоя экспертная оценка!", "ты где пропал(а)?", "давно не виделись!", "заглядывай к нам почаще!", "тут без тебя никак!", "есть минутка?", "какую музыку слушаешь?", "у нас тут интересно!", "появись, мы все простим!", "какие новости?", "нужна помощь зала!", "как настроение?", "кто смотрел новый сериал?", "делитесь мемами!", "кажется, кто-то отлынивает от общения...", "давай поболтаем?", "кто хочет в пати?", "что почитать посоветуешь?", "расскажи, как прошел твой день?",
]

# --- ИНИЦИАЛИЗАЦИЯ ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

class TopupStates(StatesGroup):
    waiting_for_amount = State()

# --- РАБОТА С БАЗОЙ ДАННЫХ ---
async def init_db():
    try:
        conn = await aiosqlite.connect(DB_PATH)
        await conn.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT, balance INTEGER,
            prefix_end INTEGER, antitar_end INTEGER, vip_end INTEGER,
            last_hunt INTEGER, partner_id INTEGER, proposal_from_id INTEGER
        )""")
        await conn.commit()
        cursor = await conn.execute("PRAGMA table_info(users)")
        columns = [row[1] for row in await cursor.fetchall()]
        if 'partner_id' not in columns:
            await conn.execute("ALTER TABLE users ADD COLUMN partner_id INTEGER")
        if 'proposal_from_id' not in columns:
            await conn.execute("ALTER TABLE users ADD COLUMN proposal_from_id INTEGER")
        await conn.commit()
        await conn.close()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.exception(f"Error initializing database: {e}")

async def get_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone()

async def add_user(user_id: int, username: str):
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            "INSERT OR IGNORE INTO users (user_id, username, balance, prefix_end, antitar_end, vip_end, last_hunt, partner_id, proposal_from_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, username, 0, 0, 0, 0, 0, 0, 0)
        )
        await conn.commit()

async def update_user_field(user_id: int, field: str, value):
    async with aiosqlite.connect(DB_PATH) as conn:
        query = f"UPDATE users SET {field} = ? WHERE user_id = ?"
        await conn.execute(query, (value, user_id))
        await conn.commit()

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
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

# --- ХРАНИЛИЩЕ АКТИВНОСТИ (В ПАМЯТИ) ---
recent_users_activity = {} # {chat_id: {user_id: timestamp}}

# --- ОБРАБОТЧИКИ КОМАНД ---
@dp.message(Command("start", "help"))
async def cmd_start(message: Message):
    try:
        user_id = message.from_user.id
        username = message.from_user.username or message.from_user.full_name
        await add_user(user_id, username)
        tutorial_text = (
            "👋 Приветствуем в боте!\n\n"
            "**Основные команды:**\n"
            "▫️ /profile — посмотреть свой профиль и баланс.\n"
            "▫️ /hunt — отправиться на охоту и поймать 🦎.\n"
            "▫️ /pay (в ответ) — перевести ящерок другому.\n"
            "▫️ /shop — купить улучшения.\n"
            "▫️ /topup — пополнить баланс за Telegram ★.\n\n"
            "**Команды отношений:**\n"
            "❤️ `/marry` (в ответ) - сделать предложение (250 🦎).\n"
            "💍 `/accept` - принять предложение.\n"
            "💔 `/divorce` - разорвать отношения.\n\n"
            "**Для администраторов группы:**\n"
            "👑 /ping — упомянуть до 20 неактивных участников."
        )
        await message.answer(tutorial_text)
    except Exception as e:
        logger.exception(f"Error in start command: {e}")
        await message.answer("Произошла ошибка при регистрации. Попробуйте снова.")

@dp.message(Command("profile"))
async def cmd_profile(message: Message):
    try:
        target_user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
        user_id = target_user.id
        await add_user(user_id, target_user.username or target_user.full_name)
        user = await get_user(user_id)
        if not user:
            await message.answer("Не удалось найти или создать профиль пользователя.")
            return
        await check_items(user_id)
        user = await get_user(user_id)
        balance = user["balance"]
        now = int(datetime.now().timestamp())
        def format_item(end_timestamp):
            if end_timestamp and end_timestamp > now:
                dt = datetime.fromtimestamp(end_timestamp)
                return f"активен до {dt.strftime('%d.%m.%Y %H:%M')}"
            else:
                return "отсутствует"
        partner_status = "в активном поиске"
        if user["partner_id"]:
            partner_name = await get_user_mention_by_id(user['partner_id'])
            partner_status = f"в отношениях с {partner_name}"
        profile_title = "👤 Ваш профиль" if target_user.id == message.from_user.id else f"👤 Профиль {target_user.full_name}"
        text = (f"{profile_title}:\nБаланс: {balance} 🦎\nСтатус: {partner_status}\n\nПрефикс: {format_item(user['prefix_end'])}\nАнтитар: {format_item(user['antitar_end'])}\nVIP: {format_item(user['vip_end'])}")
        kb = InlineKeyboardBuilder()
        kb.add(types.InlineKeyboardButton(text="🛒 Магазин", callback_data="shop_main"))
        await message.answer(text, reply_markup=kb.as_markup(), parse_mode="HTML")
    except Exception as e:
        logger.exception(f"Error in profile command: {e}")
        await message.answer("Произошла ошибка при получении профиля. Попробуйте снова.")

@dp.message(Command("hunt"))
async def cmd_hunt(message: Message):
    try:
        user_id = message.from_user.id
        await add_user(user_id, message.from_user.username or message.from_user.full_name)
        user = await get_user(user_id)
        await check_items(user_id)
        now = int(datetime.now().timestamp())
        last_hunt = user["last_hunt"] or 0
        cooldown = 24 * 3600
        if last_hunt and now - last_hunt < cooldown:
            remaining = cooldown - (now - last_hunt)
            hours, minutes = divmod(remaining, 3600)
            minutes //= 60
            await message.answer(f"⏳ Охота недоступна. Попробуйте через {hours} ч {minutes} мин.")
            return
        catch = random.randint(1, 10)
        new_balance = user["balance"] + catch
        await update_user_field(user_id, "balance", new_balance)
        await update_user_field(user_id, "last_hunt", now)
        await message.answer(f"🎉 Вы отправились на охоту и поймали {catch} 🦎!\nВаш новый баланс: {new_balance} 🦎")
    except Exception as e:
        logger.exception(f"Error in hunt command: {e}")
        await message.answer("Произошла ошибка при охоте. Попробуйте снова.")

@dp.message(Command("pay"))
async def cmd_pay(message: Message):
    if not message.reply_to_message:
        await message.reply("❗️ **Ошибка:**\nИспользуйте эту команду в ответ на сообщение пользователя, которому хотите перевести ящерок.")
        return
    try:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.reply("❗️ **Ошибка:**\nУкажите сумму для перевода. Пример: `/pay 50`")
            return
        amount = int(args[1])
        if amount <= 0:
            await message.reply("❗️ **Ошибка:**\nСумма перевода должна быть положительным числом.")
            return
    except ValueError:
        await message.reply("❗️ **Ошибка:**\nНеверный формат суммы. Пожалуйста, введите число.")
        return
    except Exception as e:
        logger.error(f"Error parsing pay command: {e}")
        await message.reply("Произошла неизвестная ошибка при обработке команды.")
        return
    sender = message.from_user
    recipient = message.reply_to_message.from_user
    if sender.id == recipient.id:
        await message.reply("❗️ **Ошибка:**\nВы не можете перевести ящерок самому себе.")
        return
    if recipient.is_bot:
        await message.reply("❗️ **Ошибка:**\nВы не можете перевести ящерок боту.")
        return
    await add_user(sender.id, sender.username or sender.full_name)
    await add_user(recipient.id, recipient.username or recipient.full_name)
    sender_data = await get_user(sender.id)
    recipient_data = await get_user(recipient.id)
    if not sender_data or sender_data['balance'] < amount:
        await message.reply(f"❌ **Недостаточно средств!**\nУ вас на балансе всего {sender_data.get('balance', 0)} 🦎.")
        return
    try:
        new_sender_balance = sender_data['balance'] - amount
        new_recipient_balance = recipient_data['balance'] + amount
        await update_user_field(sender.id, "balance", new_sender_balance)
        await update_user_field(recipient.id, "balance", new_recipient_balance)
        sender_mention = await get_user_mention_by_id(sender.id)
        recipient_mention = await get_user_mention_by_id(recipient.id)
        await message.answer(f"💸 **Перевод успешен!**\n\n{sender_mention} перевел(а) {amount} 🦎 пользователю {recipient_mention}.\n\nНовый баланс отправителя: {new_sender_balance} 🦎.")
    except Exception as e:
        logger.error(f"Error during transaction: {e}")
        await message.reply("Произошла критическая ошибка во время транзакции. Средства не были переведены.")

# --- АДМИН-КОМАНДЫ ---
@dp.message(Command("give"))
async def cmd_give(message: Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Только супер-админ может использовать эту команду!")
        return
    try:
        args = command.args.split()
        if len(args) != 2:
            await message.answer("Использование: /give <user_id> <amount>")
            return
        target_id, amount = int(args[0]), int(args[1])
        if amount <= 0:
            await message.answer("Количество должно быть положительным числом.")
            return
    except (ValueError, TypeError):
        await message.answer("Неверный формат аргументов. Нужно: /give <user_id> <amount>")
        return
    await add_user(target_id, "Unknown")
    user = await get_user(target_id)
    if not user:
        await message.answer(f"Пользователь с ID {target_id} не найден.")
        return
    new_balance = user["balance"] + amount
    await update_user_field(target_id, "balance", new_balance)
    await message.answer(f"✅ Выдали {amount} 🦎 пользователю с ID {target_id}. Новый баланс: {new_balance} 🦎")

@dp.message(Command("ping"))
async def cmd_ping_user(message: Message):
    if message.chat.type not in {'group', 'supergroup'}:
        await message.reply("Эту команду можно использовать только в группах.")
        return
    chat_id = message.chat.id
    try:
        member = await bot.get_chat_member(chat_id=chat_id, user_id=message.from_user.id)
        if member.status not in [ChatMemberStatus.CREATOR, ChatMemberStatus.ADMINISTRATOR]:
            await message.reply("❌ Эта команда доступна только администраторам этой группы.")
            return
            
        activity_for_this_chat = recent_users_activity.get(chat_id, {})
        all_admins = await bot.get_chat_administrators(chat_id)
        admin_ids = {admin.user.id for admin in all_admins}
        
        now = datetime.now().timestamp()
        
        # Определяем границы "коридора неактивности"
        one_hour_ago = now - 3600  # Верхняя граница (неактивен > 1 часа)
        three_days_ago = now - (3 * 24 * 3600) # Нижняя граница (неактивен < 3 дней)
        
        eligible_users = [
            user_id for user_id, last_active in activity_for_this_chat.items()
            if three_days_ago < last_active < one_hour_ago and user_id not in admin_ids
        ]

        if not eligible_users:
            await message.reply("😔 Некого упоминать. Не найдено пользователей, которые были неактивны от 1 часа до 3 дней.")
            return
            
        count_to_ping = min(20, len(eligible_users))
        users_to_ping = random.sample(eligible_users, count_to_ping)

        mentions = [await get_user_mention_by_id(user_id) for user_id in users_to_ping]
        
        random_message = random.choice(PING_MESSAGES)
        mentions_text = ", ".join(mentions)
        final_text = f"{mentions_text}, {random_message}"

        await bot.send_message(chat_id=chat_id, text=final_text, parse_mode="HTML")
        logger.info(f"Admin {message.from_user.id} used /ping in chat {chat_id}. Pinged {count_to_ping} users.")
        
    except Exception as e:
        logger.error(f"Error in /ping command in chat {chat_id}: {e}")
        await message.reply("😕 Произошла ошибка при выполнении команды.")

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
@dp.message(Command("shop"))
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
            await callback.answer("Ошибка: товар не найден.", show_alert=True)
            return
        price = item_data["prices"].get(days)
        item_name = item_data["name"]
        if price is None:
            await callback.answer("Ошибка: цена не найдена.", show_alert=True)
            return
        user = await get_user(user_id)
        if not user:
            await callback.answer("Сначала напишите /start", show_alert=True)
            return
        if user["balance"] < price:
            await callback.message.edit_text(f"❌ У вас недостаточно 🦎 (у вас {user['balance']}, требуется {price}).", reply_markup=create_item_menu(item_id))
            await callback.answer()
            return
        new_balance = user["balance"] - price
        await update_user_field(user_id, "balance", new_balance)
        now_ts = int(datetime.now().timestamp())
        add_seconds = days * 24 * 3600
        field_name = f"{item_id}_end"
        current_end = user[field_name] or 0
        new_end = max(current_end, now_ts) + add_seconds
        await update_user_field(user_id, field_name, new_end)
        await callback.message.edit_text(f"✅ Покупка успешна! Вы приобрели «{item_name}».\nВаш новый баланс: {new_balance} 🦎")
        await callback.answer()
        buyer_mention = await get_user_mention_by_id(user_id)
        admin_notification_text = (f"🔔 Новая покупка!\n\nКто купил: {buyer_mention}\nЧто купили: {item_name} на {days} дн.\nОстаток на балансе: {new_balance} 🦎")
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(chat_id=admin_id, text=admin_notification_text, parse_mode="HTML")
            except Exception as e:
                logger.error(f"Failed to send admin notification to {admin_id}: {e}")
    except Exception as e:
        logger.exception(f"Error in buy handler: {e}")
        await callback.answer("Произошла ошибка при покупке.", show_alert=True)

# --- СИСТЕМА ПОПОЛНЕНИЯ ЧЕРЕЗ TELEGRAM STARS ---
@dp.message(Command("topup"))
async def cmd_topup(message: Message, state: FSMContext):
    await message.answer("Введите количество ящерок, которое вы хотите купить.\n\n▫️ **Курс:** 3 ящерки = 1 ★\n▫️ **Лимиты:** от 20 до 10 000 ящерок за раз.\n▫️ Количество должно быть кратно 3.\n\nДля отмены просто напишите /cancel.")
    await state.set_state(TopupStates.waiting_for_amount)
@dp.message(Command("cancel"), F.state == TopupStates.waiting_for_amount)
async def cancel_topup(message: Message, state: FSMContext):
    await message.answer("Действие отменено.")
    await state.clear()
@dp.message(TopupStates.waiting_for_amount)
async def process_topup_amount(message: Message, state: FSMContext):
    try:
        lizards_to_buy = int(message.text)
    except ValueError:
        await message.answer("❌ Пожалуйста, введите целое число.")
        return
    if not (20 <= lizards_to_buy <= 10000):
        await message.answer("❌ Вы можете купить от 20 до 10 000 ящерок за раз.")
        return
    if lizards_to_buy % 3 != 0:
        lower = (lizards_to_buy // 3) * 3
        upper = lower + 3
        await message.answer(f"❌ Количество ящерок должно быть кратно 3.\n\nВы можете купить, например, {lower if lower >= 20 else upper} или {upper} 🦎.")
        return
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
        current_balance = user['balance'] if user else 0
        new_balance = current_balance + lizards_to_add
        await update_user_field(user_id, 'balance', new_balance)
        await bot.send_message(chat_id=user_id, text=f"✅ Оплата прошла успешно!\n\nВам начислено: {lizards_to_add} 🦎\nВаш новый баланс: {new_balance} 🦎")
    except Exception as e:
        logger.error(f"Error in successful_payment_handler: {e}")
        await bot.send_message(chat_id=message.from_user.id, text="Произошла ошибка при начислении ящерок. Пожалуйста, свяжитесь с администратором.")

# --- СИСТЕМА БРАКОВ ---
MARRIAGE_COST = 250
@dp.message(Command("marry"))
async def cmd_marry(message: Message):
    if not message.reply_to_message:
        await message.reply("Чтобы сделать предложение, используйте эту команду в ответ на сообщение пользователя.")
        return
    proposer = message.from_user
    target = message.reply_to_message.from_user
    if proposer.id == target.id:
        await message.reply("Вы не можете сделать предложение самому себе.")
        return
    if target.is_bot:
        await message.reply("Вы не можете сделать предложение боту.")
        return
    await add_user(proposer.id, proposer.username or proposer.full_name)
    await add_user(target.id, target.username or target.full_name)
    proposer_data = await get_user(proposer.id)
    target_data = await get_user(target.id)
    if proposer_data['partner_id']:
        await message.reply("Вы уже состоите в отношениях.")
        return
    if proposer_data['balance'] < MARRIAGE_COST:
        await message.reply(f"❌ Для предложения нужно {MARRIAGE_COST} 🦎.\nУ вас на балансе: {proposer_data['balance']} 🦎.")
        return
    if target_data['partner_id']:
        await message.reply(f"{target.full_name} уже состоит в отношениях.")
        return
    if target_data['proposal_from_id']:
        await message.reply(f"У {target.full_name} уже есть активное предложение. Дождитесь ответа.")
        return
    kb = InlineKeyboardBuilder()
    kb.add(types.InlineKeyboardButton(text="Да, я уверен", callback_data=f"marry_confirm:{proposer.id}:{target.id}"))
    kb.add(types.InlineKeyboardButton(text="Отмена", callback_data="marry_cancel"))
    target_mention = await get_user_mention_by_id(target.id)
    await message.reply(f"Вы уверены, что хотите сделать предложение {target_mention}?\nСтоимость этого действия: {MARRIAGE_COST} 🦎.\n\nЭто действие нельзя будет отменить.", reply_markup=kb.as_markup(), parse_mode="HTML")
@dp.message(Command("accept"))
async def cmd_accept(message: Message):
    user_id = message.from_user.id
    await add_user(user_id, message.from_user.username or message.from_user.full_name)
    user_data = await get_user(user_id)
    if not user_data['proposal_from_id']:
        await message.reply("Вам никто не делал предложений.")
        return
    proposer_id = user_data['proposal_from_id']
    proposer_data = await get_user(proposer_id)
    if not proposer_data or proposer_data['partner_id']:
        await message.reply("К сожалению, этот пользователь уже состоит в отношениях или не найден.")
        await update_user_field(user_id, "proposal_from_id", 0)
        return
    await update_user_field(user_id, "partner_id", proposer_id)
    await update_user_field(proposer_id, "partner_id", user_id)
    await update_user_field(user_id, "proposal_from_id", 0)
    user_mention = await get_user_mention_by_id(user_id)
    proposer_mention = await get_user_mention_by_id(proposer_id)
    await message.answer(f"💖 Поздравляем! {proposer_mention} и {user_mention} теперь официально состоят в отношениях! 💖", parse_mode="HTML")
@dp.message(Command("divorce"))
async def cmd_divorce(message: Message):
    user_id = message.from_user.id
    await add_user(user_id, message.from_user.username or message.from_user.full_name)
    user_data = await get_user(user_id)
    if not user_data['partner_id']:
        await message.reply("Вы не состоите в отношениях, некого бросать.")
        return
    kb = InlineKeyboardBuilder()
    kb.add(types.InlineKeyboardButton(text="Да, я хочу развестись", callback_data="confirm_divorce"))
    kb.add(types.InlineKeyboardButton(text="Отмена", callback_data="cancel_divorce"))
    await message.reply("Вы уверены, что хотите разорвать отношения? Это действие необратимо.", reply_markup=kb.as_markup())
@dp.callback_query(F.data.startswith("marry_confirm:"))
async def confirm_marry(callback: CallbackQuery):
    _, proposer_id_str, target_id_str = callback.data.split(":")
    proposer_id = int(proposer_id_str)
    if callback.from_user.id != proposer_id:
        await callback.answer("Это не ваше предложение!", show_alert=True)
        return
    target_id = int(target_id_str)
    proposer_data = await get_user(proposer_id)
    target_data = await get_user(target_id)
    if not proposer_data or not target_data:
        await callback.message.edit_text("Ошибка: один из пользователей не найден.")
        await callback.answer()
        return
    if proposer_data['balance'] < MARRIAGE_COST:
        await callback.message.edit_text(f"❌ Упс! На вашем счету больше недостаточно средств. Требуется {MARRIAGE_COST} 🦎.")
        await callback.answer()
        return
    if target_data['partner_id'] or target_data['proposal_from_id']:
        await callback.message.edit_text("❌ Упс! Этот пользователь уже получил предложение или вступил в отношения.")
        await callback.answer()
        return
    try:
        new_balance = proposer_data['balance'] - MARRIAGE_COST
        await update_user_field(proposer_id, "balance", new_balance)
        await update_user_field(target_id, "proposal_from_id", proposer_id)
        proposer_mention = await get_user_mention_by_id(proposer_id)
        target_mention = await get_user_mention_by_id(target_id)
        await callback.message.edit_text("Предложение успешно отправлено!")
        await callback.message.answer(f"💍 {target_mention}, вам поступило предложение руки и сердца от {proposer_mention}!\n\nЧтобы принять его, напишите команду `/accept`.\n\n_(С {proposer_mention} было списано {MARRIAGE_COST} 🦎 за это предложение)_", parse_mode="HTML")
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
        await callback.answer()
        return
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


# --- ОБРАБОТЧИК АКТИВНОСТИ (ДОЛЖЕН БЫТЬ В КОНЦЕ)---
@dp.message()
async def a_track_user_activity_final(message: types.Message):
    """
    Этот обработчик ловит все сообщения, которые не были пойманы командами.
    Он используется для отслеживания активности в группах.
    """
    if message.chat.type in {'group', 'supergroup'}:
        if not message.from_user.is_bot:
            recent_users_activity.setdefault(message.chat.id, {})[message.from_user.id] = datetime.now().timestamp()


# --- ЗАПУСК БОТА ---
async def main():
    await init_db()
    bot.default_parse_mode = "HTML"
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())