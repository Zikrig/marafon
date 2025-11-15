import asyncio
import logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.enums import ParseMode, ChatMemberStatus
from aiogram.exceptions import TelegramNetworkError, TelegramRetryAfter, TelegramAPIError

from config import (
    BOT_TOKEN,
    ADMIN_ID,
    ADMIN_IDS,
    CHANNEL_MAIN_ID,
    CHANNEL_OKSANA_ID,
    CHANNEL_NATALIA_ID,
    CHANNEL_MARIA_ID
)
from database import init_db, add_user, mark_subscribed, is_registered
from messages import MESSAGE_WELCOME, MESSAGE_REGISTRATION
from scheduler import scheduler_loop

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.MARKDOWN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)


# Состояния для команды /send
class SendMessageStates(StatesGroup):
    waiting_for_message = State()
    confirm_send = State()


async def safe_send_message(bot: Bot, chat_id: int, text: str, photo: str = None, max_retries: int = 3):
    """Безопасная отправка сообщения с повторными попытками при сетевых ошибках"""
    for attempt in range(max_retries):
        try:
            if photo:
                await bot.send_photo(chat_id=chat_id, photo=photo, caption=text)
            else:
                await bot.send_message(chat_id=chat_id, text=text)
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


async def safe_edit_message(callback: types.CallbackQuery, text: str, reply_markup=None):
    """Безопасное редактирование сообщения с обработкой медиа"""
    try:
        # Пытаемся отредактировать текст (отключаем Markdown для избежания ошибок парсинга)
        if callback.message.text:
            await callback.message.edit_text(text, reply_markup=reply_markup, parse_mode=None)
            return
        elif callback.message.caption:
            # Если сообщение с медиа - редактируем подпись
            await callback.message.edit_caption(caption=text, reply_markup=reply_markup, parse_mode=None)
            return
    except Exception as e:
        # Если редактирование не удалось - удаляем и отправляем новое
        logger.warning(f"Не удалось отредактировать сообщение: {e}. Отправляем новое.")
    
    # Если редактирование не удалось или сообщение без текста/подписи - отправляем новое
    try:
        await callback.message.delete()
    except Exception as delete_error:
        logger.debug(f"Не удалось удалить сообщение: {delete_error}")
    
    # Отправляем новое сообщение через бота напрямую
    try:
        chat_id = callback.message.chat.id
        # Явно отключаем парсинг, чтобы избежать ошибок с Markdown
        await bot.send_message(
            chat_id=chat_id, 
            text=text, 
            reply_markup=reply_markup, 
            parse_mode=None
        )
        logger.info(f"Сообщение успешно отправлено пользователю {chat_id}")
    except Exception as answer_error:
        logger.error(f"Не удалось отправить новое сообщение пользователю {callback.message.chat.id}: {answer_error}")
        # Пытаемся отправить без форматирования вообще
        try:
            # Экранируем все специальные символы
            escaped_text = text.replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace(']', '\\]').replace('(', '\\(').replace(')', '\\)')
            await bot.send_message(
                chat_id=chat_id, 
                text=escaped_text, 
                reply_markup=reply_markup, 
                parse_mode=None
            )
        except Exception as final_error:
            logger.error(f"Критическая ошибка при отправке сообщения: {final_error}")


async def check_subscription(user_id: int) -> bool:
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
    
    # Логируем ID каналов для отладки
    logger.debug(f"Проверка подписки для пользователя {user_id}. ID каналов: {channel_ids}")
    
    statuses = []
    for channel_name, channel_id in channel_ids.items():
        try:
            logger.debug(f"Проверка подписки на канал {channel_name} (ID: {channel_id})")
            member = await bot.get_chat_member(channel_id, user_id)
            is_subscribed = member.status in [
                ChatMemberStatus.MEMBER, 
                ChatMemberStatus.ADMINISTRATOR, 
                ChatMemberStatus.CREATOR
            ]
            statuses.append(is_subscribed)
            logger.debug(f"Канал {channel_name}: статус={member.status}, подписан={is_subscribed}")
        except Exception as e:
            logger.error(f"Ошибка проверки подписки на канал {channel_name} (ID: {channel_id}): {e}")
            # Если не можем проверить - считаем, что не подписан
            statuses.append(False)
    
    result = all(statuses)
    logger.debug(f"Результат проверки подписки: {result} (статусы: {statuses})")
    return result


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Обработчик команды /start"""
    try:
        user_id = message.from_user.id
        username = message.from_user.username
        first_name = message.from_user.first_name
        
        await add_user(user_id, username, first_name)
        logger.info(f"Пользователь {user_id} ({username or first_name}) выполнил /start")
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Участвовать", callback_data="participate")]
        ])
        
        # Используем безопасную отправку с повторными попытками
        try:
            await message.answer(MESSAGE_WELCOME, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
        except TelegramNetworkError as e:
            # При сетевой ошибке пробуем повторно
            logger.warning(f"Сетевая ошибка при отправке приветствия, повторная попытка: {e}")
            await asyncio.sleep(2)
            try:
                await message.answer(MESSAGE_WELCOME, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
            except Exception as e2:
                logger.error(f"Не удалось отправить приветственное сообщение после повторной попытки: {e2}")
        except Exception as e:
            logger.error(f"Ошибка при отправке приветственного сообщения: {e}")
    except Exception as e:
        logger.error(f"Ошибка в обработчике /start: {e}")


@dp.callback_query(lambda c: c.data == "participate")
async def process_participate(callback: types.CallbackQuery):
    """Обработчик нажатия кнопки 'Участвовать'"""
    try:
        user_id = callback.from_user.id
        
        # Проверяем подписку
        if await check_subscription(user_id):
            await mark_subscribed(user_id)
            # Убираем кнопку из старого сообщения
            try:
                if callback.message.text:
                    await callback.message.edit_reply_markup(reply_markup=None)
                elif callback.message.caption:
                    await callback.message.edit_reply_markup(reply_markup=None)
            except Exception as e:
                logger.debug(f"Не удалось убрать кнопку из сообщения: {e}")
            
            # Отправляем новое короткое сообщение о регистрации
            await callback.message.answer(
                MESSAGE_REGISTRATION,
                reply_markup=None,
                parse_mode=None
            )
            await callback.answer("Регистрация успешна! 🎉")
        else:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Я подписался(ась)", callback_data="check_subscription")]
            ])
            # Отправляем новое сообщение вместо редактирования
            await callback.message.answer(
                MESSAGE_WELCOME + "\n\n⚠️ Пожалуйста, подпишись на все каналы выше, затем нажми кнопку.",
                reply_markup=keyboard,
                parse_mode=ParseMode.MARKDOWN
            )
            await callback.answer("Пожалуйста, подпишись на все каналы")
    except Exception as e:
        logger.error(f"Ошибка в process_participate: {e}", exc_info=True)
        try:
            await callback.answer("Произошла ошибка. Попробуйте позже.", show_alert=True)
        except:
            pass


@dp.callback_query(lambda c: c.data == "check_subscription")
async def process_check_subscription(callback: types.CallbackQuery):
    """Обработчик проверки подписки"""
    try:
        user_id = callback.from_user.id
        
        if await check_subscription(user_id):
            await mark_subscribed(user_id)
            # Отправляем новое сообщение вместо редактирования
            # Используем parse_mode=None, так как в MESSAGE_REGISTRATION нет Markdown ссылок
            await callback.message.answer(
                MESSAGE_REGISTRATION,
                reply_markup=None,
                parse_mode=None
            )
            await callback.answer("Регистрация успешна! 🎉")
        else:
            await callback.answer(
                "❌ Ты еще не подписана на все каналы. Пожалуйста, подпишись и попробуй снова.",
                show_alert=True
            )
    except Exception as e:
        logger.error(f"Ошибка в process_check_subscription: {e}", exc_info=True)
        try:
            await callback.answer("Произошла ошибка. Попробуйте позже.", show_alert=True)
        except:
            pass


@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    """Команда для получения статистики (только для админа)"""
    # Если ADMIN_IDS не настроен, команда недоступна
    if not ADMIN_IDS:
        await message.answer("⚠️ Административная команда недоступна")
        return
    
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет прав для выполнения этой команды")
        return
    
    from database import get_all_registered_users
    users = await get_all_registered_users()
    await message.answer(f"📊 Всего зарегистрированных пользователей: {len(users)}")


@dp.message(Command("send"))
async def cmd_send(message: types.Message, state: FSMContext):
    """Команда для отправки сообщения всем пользователям (только для админа)"""
    # Проверка прав администратора
    if not ADMIN_IDS:
        await message.answer("⚠️ Административная команда недоступна")
        return
    
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет прав для выполнения этой команды")
        return
    
    # Кнопка отмены
    cancel_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отменить")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    
    await message.answer(
        "📤 Отправьте сообщение для рассылки всем зарегистрированным пользователям.\n\n"
        "Можно отправить текст или фото с подписью.\n"
        "Для отмены нажмите кнопку ниже.",
        reply_markup=cancel_keyboard
    )
    await state.set_state(SendMessageStates.waiting_for_message)


@dp.message(StateFilter(SendMessageStates.waiting_for_message), F.text == "❌ Отменить")
async def cancel_send(message: types.Message, state: FSMContext):
    """Отмена отправки сообщения"""
    await state.clear()
    await message.answer(
        "❌ Отправка отменена",
        reply_markup=types.ReplyKeyboardRemove()
    )


@dp.message(StateFilter(SendMessageStates.waiting_for_message))
async def process_message_to_send(message: types.Message, state: FSMContext):
    """Обработка сообщения для рассылки"""
    # Сохраняем данные сообщения
    message_data = {
        "text": message.text or message.caption or "",
        "photo": None
    }
    
    # Если есть фото
    if message.photo:
        # Берем фото наибольшего размера
        photo = message.photo[-1]
        message_data["photo"] = photo.file_id
    
    # Сохраняем в состояние
    await state.update_data(message_data=message_data)
    
    # Кнопки подтверждения
    confirm_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Отправить", callback_data="confirm_send")],
        [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_send")]
    ])
    
    # Показываем превью сообщения
    preview_text = "📋 Превью сообщения:\n\n"
    if message_data["photo"]:
        preview_text += "📷 [Фото]\n"
    preview_text += message_data["text"]
    preview_text += "\n\nОтправить это сообщение всем пользователям?"
    
    if message_data["photo"]:
        await message.answer_photo(
            photo=message_data["photo"],
            caption=preview_text,
            reply_markup=confirm_keyboard
        )
    else:
        await message.answer(
            preview_text,
            reply_markup=confirm_keyboard
        )
    
    await state.set_state(SendMessageStates.confirm_send)


@dp.callback_query(StateFilter(SendMessageStates.confirm_send), F.data == "cancel_send")
async def cancel_send_callback(callback: types.CallbackQuery, state: FSMContext):
    """Отмена отправки через callback"""
    await state.clear()
    await safe_edit_message(callback, "❌ Отправка отменена")
    await callback.answer("Отправка отменена")
    await callback.message.answer(
        "Команда отменена",
        reply_markup=types.ReplyKeyboardRemove()
    )


@dp.callback_query(StateFilter(SendMessageStates.confirm_send), F.data == "confirm_send")
async def confirm_send_callback(callback: types.CallbackQuery, state: FSMContext):
    """Подтверждение и отправка сообщения всем пользователям"""
    data = await state.get_data()
    message_data = data.get("message_data")
    
    if not message_data:
        await callback.answer("Ошибка: данные сообщения не найдены", show_alert=True)
        await state.clear()
        return
    
    await safe_edit_message(callback, "⏳ Отправка сообщений...")
    await callback.answer()
    
    # Получаем список всех зарегистрированных пользователей
    from database import get_all_registered_users
    users = await get_all_registered_users()
    
    if not users:
        await callback.message.answer("❌ Нет зарегистрированных пользователей для рассылки")
        await state.clear()
        return
    
    # Отправляем сообщения
    success_count = 0
    error_count = 0
    
    for user_id in users:
        try:
            if message_data["photo"]:
                await bot.send_photo(
                    chat_id=user_id,
                    photo=message_data["photo"],
                    caption=message_data["text"]
                )
            else:
                await bot.send_message(
                    chat_id=user_id,
                    text=message_data["text"]
                )
            success_count += 1
            await asyncio.sleep(0.05)  # Защита от флуд-лимита
        except Exception as e:
            error_count += 1
            logger.error(f"Ошибка отправки сообщения пользователю {user_id}: {e}")
    
    # Результат
    result_text = (
        f"✅ Рассылка завершена!\n\n"
        f"📊 Успешно отправлено: {success_count}\n"
        f"❌ Ошибок: {error_count}\n"
        f"📈 Всего пользователей: {len(users)}"
    )
    
    await safe_edit_message(callback, result_text)
    await callback.message.answer(
        "Рассылка завершена",
        reply_markup=types.ReplyKeyboardRemove()
    )
    
    await state.clear()


async def main():
    """Главная функция"""
    # Проверка конфигурации
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN не установлен! Проверьте файл .env")
        return
    
    # Инициализация базы данных
    await init_db()
    logger.info("База данных инициализирована")
    
    # Запуск планировщика в фоне
    asyncio.create_task(scheduler_loop(bot))
    logger.info("Планировщик запущен")
    
    # Запуск бота
    logger.info("Бот запущен")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен")

