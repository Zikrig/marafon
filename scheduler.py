import asyncio
import logging
import random
from datetime import datetime
from aiogram import Bot
from aiogram.enums import ChatMemberStatus
from aiogram.exceptions import TelegramNetworkError, TelegramRetryAfter, TelegramAPIError
from config import (
    BROADCASTS, 
    MARATHON_END_DATE, 
    # RAFFLE_DATE, 
    CHANNEL_MAIN,
    CHANNEL_MAIN_ID,
    CHANNEL_OKSANA_ID,
    CHANNEL_NATALIA_ID,
    CHANNEL_MARIA_ID
)
from messages import (
    get_day_before_message,
    get_hour_before_message,
    get_5min_before_message,
    get_after_broadcast_message,
    get_next_broadcast_announcement,
    MESSAGE_MARATHON_END,
    get_raffle_message
)
from database import (
    get_all_registered_users,
    get_eligible_raffle_participants,
    update_user_subscription_status,
    save_raffle_winners
)

logger = logging.getLogger(__name__)


async def check_subscription(bot: Bot, user_id: int) -> bool:
    """Проверка подписки пользователя на все каналы"""
    # Проверка, что ID каналов настроены
    channel_ids = {
        "MAIN": CHANNEL_MAIN_ID,
        "OKSANA": CHANNEL_OKSANA_ID,
        "NATALIA": CHANNEL_NATALIA_ID,
        "MARIA": CHANNEL_MARIA_ID
    }
    
    if not all(channel_ids.values()):
        logger.warning(f"ID каналов не настроены в .env файле. Значения: {channel_ids}")
        # Если ID не настроены, пропускаем проверку (для разработки)
        return True
    
    statuses = []
    for channel_name, channel_id in channel_ids.items():
        try:
            member = await bot.get_chat_member(channel_id, user_id)
            is_subscribed = member.status in [
                ChatMemberStatus.MEMBER, 
                ChatMemberStatus.ADMINISTRATOR, 
                ChatMemberStatus.CREATOR
            ]
            statuses.append(is_subscribed)
        except Exception as e:
            logger.error(f"Ошибка проверки подписки на канал {channel_name} (ID: {channel_id}) для пользователя {user_id}: {e}")
            # Если не можем проверить - считаем, что не подписан
            statuses.append(False)
    
    result = all(statuses)
    return result


async def safe_send_message(bot: Bot, chat_id: int, text: str, max_retries: int = 3):
    """Безопасная отправка сообщения с повторными попытками при сетевых ошибках"""
    for attempt in range(max_retries):
        try:
            # Отправляем без парсинга Markdown, чтобы избежать ошибок парсинга
            # Используем пустую строку для явного отключения парсинга
            await bot.send_message(chat_id=chat_id, text=text, parse_mode="")
            return True
        except TelegramRetryAfter as e:
            # Если превышен лимит - ждем указанное время
            wait_time = e.retry_after
            logger.warning(f"Превышен лимит запросов для {chat_id}. Ожидание {wait_time} секунд...")
            await asyncio.sleep(wait_time)
            continue
        except TelegramNetworkError as e:
            # Сетевые ошибки - повторяем с задержкой
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2  # Экспоненциальная задержка: 2, 4, 6 секунд
                logger.warning(f"Сетевая ошибка при отправке сообщения пользователю {chat_id} (попытка {attempt + 1}/{max_retries}): {e}. Повтор через {wait_time} сек.")
                await asyncio.sleep(wait_time)
                continue
            else:
                logger.error(f"Не удалось отправить сообщение пользователю {chat_id} после {max_retries} попыток: {e}")
                return False
        except TelegramAPIError as e:
            # API ошибки (например, пользователь заблокировал бота) - не повторяем
            logger.error(f"API ошибка при отправке сообщения пользователю {chat_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Неожиданная ошибка при отправке сообщения пользователю {chat_id}: {e}")
            return False
    return False


async def check_and_send_reminders(bot: Bot):
    """Проверка и отправка напоминаний"""
    now = datetime.now()
    current_time = (now.year, now.month, now.day, now.hour, now.minute)
    
    users = await get_all_registered_users()
    
    if not users:
        logger.debug("Нет зарегистрированных пользователей для рассылки")
        return
    
    logger.info(f"Проверка напоминаний. Текущее время: {current_time}, зарегистрированных пользователей: {len(users)}")
    
    # Проверка напоминаний для каждого эфира
    for broadcast in BROADCASTS:
        reminders = broadcast["reminders"]
        
        # За сутки до эфира
        if current_time == reminders["day_before"]["date"]:
            message = get_day_before_message(broadcast)
            logger.info(f"Отправка напоминания 'за сутки до эфира' для эфира {broadcast['day']}")
            for user_id in users:
                await safe_send_message(bot, user_id, message)
                await asyncio.sleep(0.05)  # Защита от флуд-лимита
        
        # За час до эфира
        if current_time == reminders["hour_before"]["date"]:
            message = get_hour_before_message(broadcast)
            logger.info(f"Отправка напоминания 'за час до эфира' для эфира {broadcast['day']}")
            for user_id in users:
                await safe_send_message(bot, user_id, message)
                await asyncio.sleep(0.05)
        
        # За 5 минут до эфира
        # if current_time == reminders["5min_before"]["date"]:
        #     message = get_5min_before_message(broadcast)
        #     logger.info(f"Отправка напоминания 'за 5 минут до эфира' для эфира {broadcast['day']}")
        #     for user_id in users:
        #         await safe_send_message(bot, user_id, message)
        #         await asyncio.sleep(0.05)
        
        # После эфира
        if current_time == reminders["after"]["date"]:
            message = get_after_broadcast_message(broadcast)
            logger.info(f"Отправка сообщения 'после эфира' для эфира {broadcast['day']}")
            for user_id in users:
                await safe_send_message(bot, user_id, message)
                await asyncio.sleep(0.05)
    
    # Сообщение о завершении марафона
    if current_time == MARATHON_END_DATE:
        logger.info("Отправка сообщения о завершении марафона")
        for user_id in users:
            await safe_send_message(bot, user_id, MESSAGE_MARATHON_END)
            await asyncio.sleep(0.05)
    
    # Проведение розыгрыша
    # if current_time == RAFFLE_DATE:
    #     logger.info("Начало проведения розыгрыша")
    #     await conduct_raffle(bot)


async def conduct_raffle(bot: Bot):
    """Проведение розыгрыша призов"""
    try:
        logger.info("=== Начало проведения розыгрыша ===")
        
        # Получаем всех потенциальных участников с данными
        participants = await get_eligible_raffle_participants()
        
        if not participants:
            logger.warning("Нет участников для розыгрыша")
            return
        
        logger.info(f"Найдено {len(participants)} потенциальных участников")
        
        # Проверяем подписку всех участников
        eligible_participants = []
        for participant in participants:
            user_id = participant["user_id"]
            username = participant.get("username")
            
            # Проверяем, что у пользователя есть username (обязательное условие)
            if not username:
                logger.debug(f"Пользователь {user_id} исключен: нет username")
                continue
            
            # Проверяем подписку
            is_subscribed = await check_subscription(bot, user_id)
            
            # Обновляем статус подписки в базе
            await update_user_subscription_status(user_id, is_subscribed)
            
            if is_subscribed:
                eligible_participants.append(participant)
            else:
                logger.debug(f"Пользователь {user_id} (@{username}) исключен: не подписан на все каналы")
        
        logger.info(f"После проверки подписки осталось {len(eligible_participants)} участников")
        
        # Если участников нет вообще - отправляем сообщение об ошибке
        if len(eligible_participants) == 0:
            # logger.warning("Нет участников для розыгрыша")
            # error_message = "⚠️ Розыгрыш не может быть проведен: нет участников, соответствующих условиям."
            # users = await get_all_registered_users()
            # for user_id in users:
            #     # await safe_send_message(bot, user_id, error_message)
            #     await asyncio.sleep(0.05)
            return
        
        # Определяем количество победителей (максимум 3, но может быть меньше)
        num_winners = min(len(eligible_participants), 3)
        
        # Случайный выбор победителей
        if len(eligible_participants) > 3:
            winners = random.sample(eligible_participants, 3)
        else:
            # Если участников меньше 3, выбираем всех
            winners = eligible_participants.copy()
            # Перемешиваем для случайности
            random.shuffle(winners)
        
        # Формируем данные победителей
        prizes = [
            {"place": 1, "amount": "10 000 ₽"},
            {"place": 2, "amount": "5 000 ₽"},
            {"place": 3, "amount": "3 000 ₽"}
        ]
        
        winners_data = []
        for i in range(num_winners):
            winner = winners[i]
            prize = prizes[i]
            winner_data = {
                "prize_place": prize["place"],
                "prize_amount": prize["amount"],
                "user_id": winner["user_id"],
                "username": winner.get("username"),
                "first_name": winner.get("first_name")
            }
            winners_data.append(winner_data)
            
            # Формируем отображаемое имя (только username, так как только они участвуют)
            username = winner.get("username", "Неизвестный")
            display_name = f"@{username}" if username != "Неизвестный" else username
            
            logger.info(f"Победитель {prize['place']} места ({prize['amount']}): {display_name} (ID: {winner['user_id']})")
        
        # Сохраняем результаты в базу данных
        await save_raffle_winners(winners_data)
        logger.info("Результаты розыгрыша сохранены в базу данных")
        
        # Формируем сообщение с результатами
        message = get_raffle_message(winners_data)
        
        # Отправляем сообщение всем зарегистрированным пользователям
        users = await get_all_registered_users()
        logger.info(f"Отправка результатов розыгрыша {len(users)} пользователям")
        for user_id in users:
            await safe_send_message(bot, user_id, message)
            await asyncio.sleep(0.05)
        
        logger.info("=== Розыгрыш успешно завершен ===")
        
    except Exception as e:
        logger.error(f"Ошибка при проведении розыгрыша: {e}", exc_info=True)


async def scheduler_loop(bot: Bot):
    """Основной цикл планировщика"""
    while True:
        try:
            await check_and_send_reminders(bot)
        except Exception as e:
            logger.error(f"Ошибка в планировщике: {e}", exc_info=True)
        
        # Проверяем каждую минуту
        await asyncio.sleep(60)

