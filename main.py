import logging
import random
from datetime import datetime
import os
import json
import asyncio

import asyncpg  # <-- НОВАЯ БИБЛИОТЕКА
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

# --- НАСТРОЙКИ БОТА (ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ) ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DB_URL") # <-- НОВАЯ ПЕРЕМЕННАЯ ДЛЯ SUPABASE

if not BOT_TOKEN or not DATABASE_URL:
    logger.critical("КРИТИЧЕСКАЯ ОШИБКА: Переменные окружения BOT_TOKEN и/или DB_URL не установлены.")
    exit()

ADMIN_IDS = [6179115044, 7189733067]

# --- НАСТРОЙКИ ИГРОВОЙ ЛОГИКИ (без изменений) ---
MAX_PETS = 20
QUIZ_COOLDOWN_HOURS = 5
MARRIAGE_MIN_LEVEL = 35
PET_MIN_LEVEL = 55
MARRIAGE_COST = 250
PET_DEATH_DAYS = 2

PET_ACTIONS_COST = {
    "feed": 1, "grow": 5, "water": 2, "walk": 3,
}

EGGS = {
    "common": {"name": "🥚 Обычное яйцо", "cost": 150, "rarity": "common"},
    "rare": {"name": "💎 Редкое яйцо", "cost": 500, "rarity": "rare"},
    "legendary": {"name": "⚜️ Легендарное яйцо", "cost": 1500, "rarity": "legendary"},
    "mythic": {"name": "✨ Мифическое яйцо", "cost": 5000, "rarity": "mythic"},
}

PET_SPECIES = {
    "common": [
        {"species_name": "Полоз", "images": {1: "https://i.ibb.co/4gRJSF4N/Gemini-Generated-Image-bbrjqrbbrjqrbbrj.png", 10: "https://i.ibb.co/x87LKPq2/image.png", 35: "https://i.ibb.co/ccnTcgJX/image.png"}},
        {"species_name": "Уж", "images": {1: "https://i.ibb.co/qLBW0wN7/image.png", 10: "https://i.ibb.co/Z1fRyG8R/image.png", 35: "https://i.ibb.co/Ng6pJ2wm/Gemini-Generated-Image-6z8b4s6z8b4s6z8b.png"}},
    ],
    "rare": [
        {"species_name": "Гадюка", "images": {1: "https://i.ibb.co/xSXPC1C7/image.png", 10: "https://i.ibb.co/Y4KqkSgt/image.png", 35: "https://i.ibb.co/rRhY1nX3/image.png"}},
        {"species_name": "Эфа", "images": {1: "https://i.ibb.co/TDnDKDJb/image.png", 10: "https://i.ibb.co/XfhfSP31/image.png", 35: "https://i.ibb.co/prvbR5Kf/image.png"}},
    ],
    "legendary": [
        {"species_name": "Питон", "images": {1: "https://i.ibb.co/WCXKKBF/image.png", 10: "https://i.ibb.co/j9Q9XZTR/image.png", 35: "https://i.ibb.co/qYjVcqck/Gemini-Generated-Image-aofhgzaofhgzaofh.png"}},
        {"species_name": "Кобра", "images": {1: "https://i.ibb.co/DP5QFyJn/Gemini-Generated-Image-gzt9g3gzt9g3gzt9.png", 10: "https://i.ibb.co/HLS6vB21/Gemini-Generated-Image-m2l12m2l12m2l12m.png", 35: "https://i.ibb.co/7xdG7Vmg/Gemini-Generated-Image-pcfv7cpcfv7cpcfv.png"}},
    ],
    "mythic": [
        {"species_name": "Василиск", "images": {1: "https://i.ibb.co/0Rtx5sb1/Gemini-Generated-Image-rxh7a8rxh7a8rxh7.png", 10: "https://i.ibb.co/RpBs3XxM/Gemini-Generated-Image-togzv2togzv2togz.png", 35: "https://i.ibb.co/FLCVtdVg/Gemini-Generated-Image-bfub33bfub33bfub.png"}},
    ]
}

PING_MESSAGES = [ "чем занимаешься?", "заходи на игру?", "как насчет катки?", "го общаться!", "скучно, давай поговорим?"]
recent_users_activity = {}

# --- ИНИЦИАЛИЗАЦИЯ ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
db_pool = None # <-- Пул соединений с БД

# --- FSM СОСТОЯНИЯ (без изменений) ---
class TopupStates(StatesGroup):
    waiting_for_amount = State()

class QuizStates(StatesGroup):
    in_quiz = State()

class PetHatchStates(StatesGroup):
    waiting_for_name = State()


# --- РАБОТА С БАЗОЙ ДАННЫХ (POSTGRESQL / ASYNCPG) ---

async def create_pool():
    """Создает пул соединений с базой данных."""
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(dsn=DATABASE_URL)
        logger.info("Пул соединений с PostgreSQL успешно создан.")
    except Exception as e:
        logger.critical(f"Не удалось подключиться к PostgreSQL: {e}")
        exit()

async def db_execute(query, *params, fetch=None):
    """
    Выполняет SQL-запрос с использованием пула соединений.
    :param query: SQL-запрос с плейсхолдерами $1, $2, ...
    :param params: Параметры для запроса.
    :param fetch: 'one' для получения одной записи, 'all' для всех, None для выполнения без возврата.
    :return: Результат запроса или None.
    """
    global db_pool
    if not db_pool:
        logger.error("Пул соединений не инициализирован!")
        return None
        
    async with db_pool.acquire() as connection:
        try:
            if fetch == 'one':
                return await connection.fetchrow(query, *params)
            elif fetch == 'all':
                return await connection.fetch(query, *params)
            else:
                await connection.execute(query, *params)
                return None
        except Exception as e:
            logger.error(f"Ошибка выполнения SQL-запроса: {query} с параметрами {params}. Ошибка: {e}")
            return None

async def init_db():
    """Инициализирует таблицы в базе данных, если их не существует."""
    await db_execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            balance BIGINT DEFAULT 0,
            level INTEGER DEFAULT 0,
            last_hunt BIGINT DEFAULT 0,
            last_quiz BIGINT DEFAULT 0,
            partner_id BIGINT DEFAULT 0,
            proposal_from_id BIGINT DEFAULT 0,
            prefix_end BIGINT DEFAULT 0,
            antitar_end BIGINT DEFAULT 0,
            vip_end BIGINT DEFAULT 0,
            quiz_highscore INTEGER DEFAULT 0 -- Новая колонка
        );
    """)
    # Безопасно добавляем колонку, если ее нет, для существующих баз
    try:
        await db_execute("ALTER TABLE users ADD COLUMN quiz_highscore INTEGER DEFAULT 0;")
    except asyncpg.exceptions.DuplicateColumnError:
        pass # Колонка уже существует, все в порядке
    
    # Остальные таблицы без изменений
    await db_execute("""
        CREATE TABLE IF NOT EXISTS pets (
            pet_id SERIAL PRIMARY KEY, owner_id BIGINT NOT NULL, name TEXT,
            species TEXT, pet_level INTEGER DEFAULT 1, last_fed BIGINT DEFAULT 0,
            last_watered BIGINT DEFAULT 0, last_grown BIGINT DEFAULT 0,
            last_walked BIGINT DEFAULT 0, creation_date BIGINT
        );
    """)
    await db_execute("""
        CREATE TABLE IF NOT EXISTS user_eggs (
            user_egg_id SERIAL PRIMARY KEY, owner_id BIGINT, egg_type TEXT
        );
    """)
    await db_execute("""
        CREATE TABLE IF NOT EXISTS quiz_questions (
            question_id SERIAL PRIMARY KEY, question_text TEXT NOT NULL,
            options JSONB NOT NULL, correct_answer TEXT NOT NULL
        );
    """)
    logger.info("Проверка таблиц в БД завершена.")

async def populate_questions():
    """Заполняет таблицу вопросов, если она пуста."""
    count_record = await db_execute("SELECT COUNT(*) FROM quiz_questions", fetch='one')
    if count_record and count_record[0] == 0:
        NEW_QUESTIONS = [
    ("Какая змея строит гнезда для своих яиц?", json.dumps(["Королевская кобра", "Черная мамба", "Питон", "Анаконда"]), "Королевская кобра"),
    ("Какое чувство у змей развито слабее всего?", json.dumps(["Зрение", "Слух", "Обоняние", "Осязание"]), "Слух"),
    ("Какая змея может 'скользить' по воздуху на расстояние до 100 метров?", json.dumps(["Украшенная древесная змея", "Райская древесная змея", "Летучая змея", "Все ответы верны"]), "Все ответы верны"),
    ("Что означает 'укус без впрыскивания яда'?", json.dumps(["Промах", "Сухой укус", "Предупредительный укус", "Неполный укус"]), "Сухой укус"),
    ("Какая змея является самой тяжелой в мире?", json.dumps(["Сетчатый питон", "Зеленая анаконда", "Тигровый питон", "Темный тигровый питон"]), "Зеленая анаконда"),
    ("Сколько легких у большинства змей?", json.dumps(["Одно", "Два, но одно рудиментарное", "Два полноценных", "Ни одного"]), "Два, но одно рудиментарное"),
    ("Как называется змея, которая имитирует окраску ядовитого кораллового аспида?", json.dumps(["Молочная змея", "Подвязочная змея", "Кукурузный полоз", "Королевская змея"]), "Молочная змея"),
    ("Какая змея имеет самые длинные ядовитые зубы (до 5 см)?", json.dumps(["Гюрза", "Бушмейстер", "Габонская гадюка", "Тайпан"]), "Габонская гадюка"),
    ("Для чего змеи используют свой раздвоенный язык?", json.dumps(["Для определения направления запаха", "Для удержания добычи", "Для защиты", "Для питья"]), "Для определения направления запаха"),
    ("Какой вид змей часто держат в качестве домашних питомцев из-за их спокойного нрава?", json.dumps(["Королевский питон", "Тигровый питон", "Радужный удав", "Все ответы верны"]), "Королевский питон"),
    ("Существуют ли змеи, которые питаются исключительно другими змеями?", json.dumps(["Да, например, королевская кобра", "Нет, все змеи едят грызунов", "Только в мифологии", "Да, но только в неволе"]), "Да, например, королевская кобра"),
    ("Как змеи 'слышат' без ушных раковин?", json.dumps(["Через вибрации почвы и костей черепа", "У них есть скрытые уши под кожей", "Они не слышат вообще", "С помощью языка"]), "Через вибрации почвы и костей черепа"),
    ("Какая из этих змей живородящая, а не откладывает яйца?", json.dumps(["Обыкновенная гадюка", "Кукурузный полоз", "Королевский питон", "Уж обыкновенный"]), "Обыкновенная гадюка"),
    ("Где обитает самая маленькая змея в мире, узкоротая змея Карлы?", json.dumps(["На острове Барбадос", "В джунглях Амазонки", "В пустыне Сахара", "На Мадагаскаре"]), "На острове Барбадос"),
    ("Что такое 'термолокация' у змей?", json.dumps(["Способность видеть в темноте", "Способность чувствовать тепло", "Способность ориентироваться по магнитному полю", "Способность менять температуру тела"]), "Способность чувствовать тепло"),
    ("Какая змея способна менять цвет кожи?", json.dumps(["Гадюка носатая", "Зеленый питон", "Некоторые виды удавов", "Никакая"]), "Некоторые виды удавов"),
    ("Что такое 'автотомия' у змей?", json.dumps(["Способность отращивать хвост", "Способность сбрасывать хвост", "Способность переваривать кости", "Никогда не встречается у змей"]), "Никогда не встречается у змей"),
    ("Какая змея является символом медицины?", json.dumps(["Эскулапов полоз", "Гадюка Асклепия", "Кобра", "Уж"]), "Эскулапов полоз"),
    ("Сколько позвонков может быть у змеи?", json.dumps(["До 100", "До 200", "До 400 и более", "Всегда 33, как у человека"]), "До 400 и более"),
    ("Какая змея из перечисленных не является ядовитой?", json.dumps(["Зеленая мамба", "Бумсланг", "Изумрудный древесный удав", "Тигровая змея"]), "Изумрудный древесный удав"),
    ("Какой континент является единственным, где нет коренных видов змей?", json.dumps(["Австралия", "Антарктида", "Европа", "Северная Америка"]), "Антарктида"),
    ("Как называется наука, изучающая змей?", json.dumps(["Герпетология", "Серпентология", "Офиология", "Все ответы верны"]), "Все ответы верны"),
    ("Какая змея имеет характерный 'рог' на носу?", json.dumps(["Рогатая гадюка", "Носатая гадюка", "Обе змеи", "Ни одна из них"]), "Обе змеи"),
    ("Что позволяет питонам заглатывать добычу намного больше их головы?", json.dumps(["Эластичные связки челюсти", "Они разрывают добычу на куски", "У них нет челюстных костей", "Они впрыскивают разжижающий фермент"]), "Эластичные связки челюсти"),
    ("Какая страна известна полным отсутствием змей, согласно легенде?", json.dumps(["Исландия", "Новая Зеландия", "Ирландия", "Гренландия"]), "Ирландия"),
    ("Яд какой змеи используется в медицине для создания лекарств?", json.dumps(["Гюрзы", "Кобры", "Гадюки", "Всех перечисленных"]), "Всех перечисленных"),
    ("Как называется защитная поза кобры, когда она раздувает капюшон?", json.dumps(["Атакующая стойка", "Оборонительная демонстрация", "Брачный танец", "Предупреждение"]), "Оборонительная демонстрация"),
    ("Какая змея может издавать звук, похожий на шипение кошки?", json.dumps(["Шипящая гадюка", "Эфа", "Квакер", "Никакая"]), "Шипящая гадюка"),
    ("Какой вид змей является водным и почти никогда не выходит на сушу?", json.dumps(["Морские змеи (ластохвосты)", "Анаконды", "Водяные ужи", "Все перечисленные"]), "Морские змеи (ластохвосты)"),
    ("Что из этого не входит в рацион яичной змеи?", json.dumps(["Птичьи яйца", "Яйца ящериц", "Мыши", "Яйца черепах"]), "Мыши"),
    ("Какое максимальное количество яда за один укус может выделить королевская кобра?", json.dumps(["До 1 мл", "До 3 мл", "До 7 мл", "До 15 мл"]), "До 7 мл"),
    ("Как называется процесс, при котором змея 'рожает' живых детенышей?", json.dumps(["Яйцеживорождение", "Живорождение", "Оба варианта верны", "Такого не бывает"]), "Оба варианта верны"),
    ("Какая змея имеет самый быстродействующий яд?", json.dumps(["Тайпан Маккоя", "Черная мамба", "Морская змея Белчера", "Крючконосая морская змея"]), "Тайпан Маккоя"),
    ("У какой змеи на хвосте есть 'погремушка'?", json.dumps(["Гремучая змея", "Щитомордник", "Эфа", "Все гадюковые"]), "Гремучая змея"),
    ("Какая змея может 'плеваться' ядом на расстояние до 3 метров?", json.dumps(["Ошейниковая кобра", "Мозамбикская кобра", "Черношеяя кобра", "Все перечисленные"]), "Все перечисленные"),
    ("Какая змея считается 'королевой' камуфляжа?", json.dumps(["Габонская гадюка", "Листоносая змея", "Песчаная эфа", "Зеленый питон"]), "Габонская гадюка"),
    ("Существуют ли слепые змеи?", json.dumps(["Да, браминийский слепун", "Нет, у всех змей есть глаза", "Только в глубоких пещерах", "Да, но они вымерли"]), "Да, браминийский слепун"),
    ("Как долго змея может обходиться без еды после крупной добычи?", json.dumps(["Неделю", "Месяц", "Несколько месяцев и дольше", "Три дня"]), "Несколько месяцев и дольше"),
    ("Какая часть тела змеи продолжает расти всю жизнь?", json.dumps(["Хвост", "Голова", "Всё тело", "Ничего из перечисленного"]), "Всё тело"),
    ("Что такое 'мускусный запах', выделяемый некоторыми змеями?", json.dumps(["Средство для привлечения партнеров", "Защитный механизм для отпугивания хищников", "Способ метить территорию", "Побочный продукт пищеварения"]), "Защитный механизм для отпугивания хищников"),
    ("Какая змея является самой длинной ядовитой змеей в мире?", json.dumps(["Королевская кобра", "Черная мамба", "Тайпан", "Бушмейстер"]), "Королевская кобра"),
    ("В какой среде обитает большинство видов змей?", json.dumps(["В лесах", "В пустынях", "В воде", "В горах"]), "В лесах"),
    ("Почему язык змеи постоянно находится в движении?", json.dumps(["Для сбора информации об окружающей среде", "Для охлаждения", "Это нервный тик", "Для гидратации"]), "Для сбора информации об окружающей среде"),
    ("Какая из этих змей не душит свою добычу?", json.dumps(["Питон", "Удав", "Гадюка", "Анаконда"]), "Гадюка"),
    ("Какая змея известна своей способностью 'ходить по воде' (скользить по поверхности)?", json.dumps(["Василиск", "Водяной щитомордник", "Змея-рыболов", "Никакая, это миф"]), "Водяной щитомордник"),
    ("Какого цвета кровь у змей?", json.dumps(["Красная", "Синяя", "Зеленая", "Прозрачная"]), "Красная"),
    ("Сколько раз в год змея может линять?", json.dumps(["Один раз", "Два раза", "От 2 до 12 раз, в зависимости от возраста и вида", "Только в детстве"]), "От 2 до 12 раз, в зависимости от возраста и вида"),
    ("Какой вид змей известен тем, что 'заботится' о своем потомстве после рождения?", json.dumps(["Королевская кобра", "Питон", "Гадюка", "Практически никакой"]), "Практически никакой"),
    ("Что произойдет, если гремучая змея укусит саму себя?", json.dumps(["Она умрет", "Ничего, у нее иммунитет к своему яду", "Она заболеет, но выживет", "Такое невозможно"]), "Ничего, у нее иммунитет к своему яду"),
    ("Какая змея впадает в спячку зимой в умеренных широтах?", json.dumps(["Гадюки", "Ужи", "Медянки", "Все перечисленные"]), "Все перечисленные"),
    ("Что означает, когда змея шипит?", json.dumps(["Это предупреждение", "Она дышит", "Она злится", "Все ответы верны"]), "Все ответы верны"),
    ("У какой змеи яд обладает нейротоксическим действием, парализуя нервную систему?", json.dumps(["Кобры и мамбы", "Гадюки", "Гремучие змеи", "Все ядовитые змеи"]), "Кобры и мамбы"),
    ("Какая змея часто встречается в городских парках и садах Европы?", json.dumps(["Обыкновенный уж", "Медянка", "Гадюка Никольского", "Эскулапов полоз"]), "Обыкновенный уж"),
    ("Что такое 'тепловые ямки' у змей?", json.dumps(["Органы для поиска воды", "Органы инфракрасного зрения", "Места для откладывания яиц", "Углубления для маскировки"]), "Органы инфракрасного зрения")
]
      
        for q in questions:
            await db_execute("INSERT INTO quiz_questions (question_text, options, correct_answer) VALUES ($1, $2, $3)", q[0], q[1], q[2])
        logger.info(f"Добавлено {len(questions)} вопросов в базу данных.")

# --- Функции для работы с данными (адаптированные под PostgreSQL) ---

async def get_user(user_id: int):
    return await db_execute("SELECT * FROM users WHERE user_id = $1", user_id, fetch='one')

async def add_user(user_id: int, username: str):
    # ON CONFLICT (user_id) DO NOTHING - аналог INSERT OR IGNORE для PostgreSQL
    await db_execute(
        "INSERT INTO users (user_id, username) VALUES ($1, $2) ON CONFLICT (user_id) DO NOTHING",
        user_id, username
    )

async def update_user_field(user_id: int, field: str, value):
    # f-строка здесь безопасна, т.к. `field` - это имя столбца из нашего кода, а не ввод пользователя.
    await db_execute(f"UPDATE users SET {field} = $1 WHERE user_id = $2", value, user_id)

async def get_pet(owner_id: int):
    # Теперь мы можем иметь несколько питомцев, но пока оставим логику на одного.
    # В будущем можно будет выбирать питомца по ID.
    return await db_execute("SELECT * FROM pets WHERE owner_id = $1 LIMIT 1", owner_id, fetch='one')

async def create_pet(owner_id: int, name: str, species: str):
    now = int(datetime.now().timestamp())
    await db_execute(
        "INSERT INTO pets (owner_id, name, species, last_fed, last_watered, last_grown, last_walked, creation_date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        owner_id, name, species, now, now, now, now, now
    )

async def update_pet_field(owner_id: int, field: str, value):
    await db_execute(f"UPDATE pets SET {field} = $1 WHERE owner_id = $2", value, owner_id)

async def delete_pet(owner_id: int):
    await db_execute("DELETE FROM pets WHERE owner_id = $1", owner_id)

async def get_user_eggs(owner_id: int):
    return await db_execute("SELECT * FROM user_eggs WHERE owner_id = $1", owner_id, fetch='all')

async def add_user_egg(owner_id: int, egg_type: str):
    await db_execute("INSERT INTO user_eggs (owner_id, egg_type) VALUES ($1, $2)", owner_id, egg_type)

async def delete_user_egg(user_egg_id: int):
    await db_execute("DELETE FROM user_eggs WHERE user_egg_id = $1", user_egg_id)

async def get_random_question():
    # RANDOM() в PostgreSQL работает так же, как в SQLite
    return await db_execute("SELECT * FROM quiz_questions ORDER BY RANDOM() LIMIT 1", fetch='one')

# --- Остальные функции (без изменений в логике) ---

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
    if user["prefix_end"] and user["prefix_end"] < now: updates["prefix_end"] = 0
    if user["antitar_end"] and user["antitar_end"] < now: updates["antitar_end"] = 0
    if user["vip_end"] and user["vip_end"] < now: updates["vip_end"] = 0
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

# ... (ВСЕ ОБРАБОТЧИКИ КОМАНД, КОЛЛБЭКОВ И СООБЩЕНИЙ ОСТАЮТСЯ ЗДЕСЬ БЕЗ ИЗМЕНЕНИЙ) ...
# Я не буду копировать их сюда снова, так как в их ВНУТРЕННЕЙ логике ничего не поменялось.
# Они просто вызывают обновленные функции для работы с БД.
# Просто вставьте сюда все ваши хендлеры, начиная с @dp.message(or_f(Command("start", ...)))
# и заканчивая @dp.message() async def track_user_activity...

# --- НАЧАЛО: СКОПИРУЙТЕ СВОИ ОБРАБОТЧИКИ СЮДА ---

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

        await add_user(user_id, username)
        user = await get_user(user_id)
        if not user:
            await message.answer("Профиль не найден и не удалось создать.")
            return

        # Проверка на None и установка значений по умолчанию, если нужно
        user_data = dict(user)
        defaults = {
            "balance": 0, "level": 0, "prefix_end": 0, 
            "antitar_end": 0, "vip_end": 0, "partner_id": 0
        }
        needs_update = False
        for key, default in defaults.items():
            if user_data.get(key) is None:
                user_data[key] = default
                needs_update = True
        
        if needs_update:
            for key, value in defaults.items():
                if user[key] is None:
                    await update_user_field(user_id, key, value)
            user = await get_user(user_id) # Перезагружаем данные

        await check_items(user_id)
        user = await get_user(user_id)

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
        if is_callback and hasattr(message, 'photo') and message.photo:
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

async def notify_admins_of_purchase(user_id: int, item_name: str, days: int, new_balance: int, new_end_timestamp: int):
    """Отправляет уведомление о покупке админам и в специальную группу."""
    try:
        # 1. Сбор информации для сообщения
        user_mention = await get_user_mention_by_id(user_id)
        purchase_time = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
        end_time = datetime.fromtimestamp(new_end_timestamp).strftime('%d.%m.%Y %H:%M:%S')

        # 2. Формирование текста уведомления
        text = (
            f"🛒 <b>Новая покупка!</b>\n\n"
            f"👤 <b>Покупатель:</b> {user_mention} (ID: <code>{user_id}</code>)\n"
            f"🛍️ <b>Товар:</b> {item_name}\n"
            f"⏳ <b>Срок:</b> {days} дн.\n"
            f"🦎 <b>Остаток баланса:</b> {new_balance}\n\n"
            f"🕒 <b>Время покупки:</b> {purchase_time}\n"
            f"🔚 <b>Окончание подписки:</b> {end_time}"
        )

        # 3. Определение получателей уведомления
        target_group_id = -1001863605735
        # Используем set, чтобы избежать отправки дубликатов в один и тот же чат
        notification_chat_ids = set(ADMIN_IDS)
        notification_chat_ids.add(target_group_id)

        # 4. Отправка сообщений всем получателям
        for chat_id in notification_chat_ids:
            try:
                await bot.send_message(chat_id, text)
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление о покупке в чат {chat_id}: {e}")

    except Exception as e:
        logger.error(f"Критическая ошибка в функции уведомления о покупке: {e}")


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
        await db_execute("UPDATE pets SET pet_level = $1, last_grown = $2 WHERE owner_id = $3", pet['pet_level'] + 1, now, user_id)
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

        # --- НАЧАЛО ИЗМЕНЕНИЙ ---
        # Вызываем функцию уведомления после успешной транзакции
        await notify_admins_of_purchase(
            user_id=user_id,
            item_name=item_name,
            days=days,
            new_balance=new_balance,
            new_end_timestamp=new_end
        )
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
        
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
        await bot.send_message(chat_id=user_id, text=f"✅ Оплата прошла успешно!\n\nВам начислено: {lizards_to_add} 🦎\nВаш новый баланс: {new_balance} 🦎")
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



# --- ОБРАБОТЧИК АКТИВНОСТИ (ДЛЯ КОМАНДЫ /PING)---
@dp.message(or_f(Command("ping", "пинг"), F.text.lower().in_(['ping', 'пинг'])))
async def cmd_ping(message: Message):
    """Пингует случайного активного пользователя в чате. Доступно только админам."""
    if message.chat.type not in {'group', 'supergroup'}:
        await message.reply("Эту команду можно использовать только в группах.")
        return

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---
    # Получаем информацию о статусе пользователя в чате
    try:
        member = await bot.get_chat_member(message.chat.id, message.from_user.id)
    except Exception as e:
        logger.error(f"Не удалось проверить статус пользователя {message.from_user.id} в чате {message.chat.id}: {e}")
        # Можно отправить сообщение об ошибке или просто молча выйти
        return

    # Проверяем, является ли пользователь администратором или создателем
    if member.status not in {ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR}:
        # Можно ничего не отвечать, чтобы не привлекать внимание к команде,
        # либо отправить сообщение ниже:
        # await message.reply("Эта команда доступна только администраторам группы.")
        return
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

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

        await message.answer(f"📞 {target_mention}, администратор {pinger_mention} спрашивает: «{ping_text}»", parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error in ping command while getting user mentions: {e}")
        await message.reply("Не удалось выбрать пользователя для пинга.")

# --- КОНЕЦ: СКОПИРУЙТЕ СВОИ ОБРАБОТЧИКИ СЮДА ---


# --- ЗАПУСК БОТА ---
async def main():
    global db_pool
    await create_pool()
    await init_db()
    await populate_questions()
    
    bot.default_parse_mode = "HTML"
    
    try:
        await dp.start_polling(bot)
    finally:
        if db_pool:
            await db_pool.close()
            logger.info("Пул соединений с PostgreSQL закрыт.")
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
