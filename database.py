import aiosqlite
import os
import logging
from datetime import datetime

DB_PATH = "data/bot.db"
logger = logging.getLogger(__name__)


async def init_db():
    """Инициализация базы данных"""
    try:
        os.makedirs("data", exist_ok=True)
        logger.info(f"Инициализация базы данных: {DB_PATH}")
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    subscribed INTEGER DEFAULT 0
                )
            """)
            
            # Таблица для хранения результатов розыгрыша
            await db.execute("""
                CREATE TABLE IF NOT EXISTS raffle_winners (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    raffle_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    prize_place INTEGER NOT NULL,
                    prize_amount TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            """)
            
            await db.commit()
            
            # Проверяем количество пользователей в базе
            async with db.execute("SELECT COUNT(*) FROM users") as cursor:
                total_users = (await cursor.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM users WHERE subscribed = 1") as cursor:
                subscribed_users = (await cursor.fetchone())[0]
            
            logger.info(f"База данных инициализирована. Всего пользователей: {total_users}, подписанных: {subscribed_users}")
    except Exception as e:
        logger.error(f"Ошибка при инициализации базы данных: {e}", exc_info=True)
        raise


async def add_user(user_id: int, username: str = None, first_name: str = None):
    """Добавление или обновление пользователя в базе данных"""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            # Проверяем, существует ли пользователь
            async with db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,)) as cursor:
                exists = await cursor.fetchone()
            
            if exists:
                # Обновляем username и first_name если они изменились
                await db.execute("""
                    UPDATE users SET username = ?, first_name = ? WHERE user_id = ?
                """, (username, first_name, user_id))
            else:
                # Создаем нового пользователя
                await db.execute("""
                    INSERT INTO users (user_id, username, first_name)
                    VALUES (?, ?, ?)
                """, (user_id, username, first_name))
            await db.commit()
            logger.debug(f"Пользователь {user_id} ({username or first_name}) добавлен/обновлен в базе данных")
    except Exception as e:
        logger.error(f"Ошибка при добавлении пользователя {user_id} в базу данных: {e}", exc_info=True)


async def mark_subscribed(user_id: int):
    """Отметить пользователя как подписанного"""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                UPDATE users SET subscribed = 1 WHERE user_id = ?
            """, (user_id,))
            await db.commit()
            logger.info(f"Пользователь {user_id} отмечен как подписанный")
    except Exception as e:
        logger.error(f"Ошибка при отметке пользователя {user_id} как подписанного: {e}", exc_info=True)


async def is_registered(user_id: int) -> bool:
    """Проверить, зарегистрирован ли пользователь"""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT subscribed FROM users WHERE user_id = ?
        """, (user_id,)) as cursor:
            row = await cursor.fetchone()
            return row is not None and row[0] == 1


async def get_all_registered_users():
    """Получить всех зарегистрированных пользователей"""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT user_id FROM users WHERE subscribed = 1
            """) as cursor:
                rows = await cursor.fetchall()
                user_ids = [row[0] for row in rows]
                logger.info(f"Загружено {len(user_ids)} зарегистрированных пользователей из базы данных")
                return user_ids
    except Exception as e:
        logger.error(f"Ошибка при получении списка пользователей: {e}", exc_info=True)
        return []


async def get_eligible_raffle_participants():
    """Получить всех участников розыгрыша с их данными (user_id, username, first_name)
    
    В розыгрыше участвуют только пользователи с username (не приватные аккаунты)
    """
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT user_id, username, first_name 
                FROM users 
                WHERE subscribed = 1 
                AND username IS NOT NULL
                AND username != ''
            """) as cursor:
                rows = await cursor.fetchall()
                participants = [
                    {
                        "user_id": row[0],
                        "username": row[1],
                        "first_name": row[2]
                    }
                    for row in rows
                ]
                logger.info(f"Найдено {len(participants)} участников розыгрыша с username")
                return participants
    except Exception as e:
        logger.error(f"Ошибка при получении участников розыгрыша: {e}", exc_info=True)
        return []


async def update_user_subscription_status(user_id: int, is_subscribed: bool):
    """Обновить статус подписки пользователя"""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                UPDATE users SET subscribed = ? WHERE user_id = ?
            """, (1 if is_subscribed else 0, user_id))
            await db.commit()
            logger.debug(f"Статус подписки пользователя {user_id} обновлен: {is_subscribed}")
    except Exception as e:
        logger.error(f"Ошибка при обновлении статуса подписки пользователя {user_id}: {e}", exc_info=True)


async def save_raffle_winners(winners: list):
    """Сохранить результаты розыгрыша в базу данных
    
    Args:
        winners: Список словарей с ключами: prize_place, prize_amount, user_id, username, first_name
    """
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            for winner in winners:
                await db.execute("""
                    INSERT INTO raffle_winners (prize_place, prize_amount, user_id, username, first_name)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    winner["prize_place"],
                    winner["prize_amount"],
                    winner["user_id"],
                    winner.get("username"),
                    winner.get("first_name")
                ))
            await db.commit()
            logger.info(f"Сохранено {len(winners)} победителей розыгрыша в базу данных")
    except Exception as e:
        logger.error(f"Ошибка при сохранении результатов розыгрыша: {e}", exc_info=True)
        raise


async def get_latest_raffle_winners():
    """Получить последних победителей розыгрыша"""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT prize_place, prize_amount, user_id, username, first_name
                FROM raffle_winners
                ORDER BY raffle_date DESC, prize_place ASC
                LIMIT 3
            """) as cursor:
                rows = await cursor.fetchall()
                winners = [
                    {
                        "prize_place": row[0],
                        "prize_amount": row[1],
                        "user_id": row[2],
                        "username": row[3],
                        "first_name": row[4]
                    }
                    for row in rows
                ]
                return winners
    except Exception as e:
        logger.error(f"Ошибка при получении победителей розыгрыша: {e}", exc_info=True)
        return []



