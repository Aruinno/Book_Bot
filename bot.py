import logging
import os
from typing import Dict, List

import requests
from aiogram import Bot, Dispatcher, types
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)
from aiogram.utils import executor
from dotenv import load_dotenv

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is not set in .env")

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

BOOKS_PER_PAGE = 5
user_search_state: Dict[int, Dict[str, int]] = {}

keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
keyboard.add(
    KeyboardButton("📚 Фэнтези"),
    KeyboardButton("🧠 Психология"),
)
keyboard.add(
    KeyboardButton("💼 Бизнес"),
    KeyboardButton("🔥 Боевик"),
)


def normalize_query(query: str) -> str:
    text = query.lower().strip()
    genre_map = {
        "фэнтези": "fantasy books",
        "бизнес": "business books",
        "психология": "psychology books",
        "боевик": "action books",
    }
    for ru_genre, en_query in genre_map.items():
        if ru_genre in text:
            return en_query
    return text


def short_text(text: str, max_len: int = 220) -> str:
    normalized = " ".join(text.split())
    if len(normalized) <= max_len:
        return normalized
    return normalized[: max_len - 3].rstrip() + "..."


def search_books(query: str, start_index: int = 0, max_results: int = BOOKS_PER_PAGE) -> List[dict]:
    url = "https://www.googleapis.com/books/v1/volumes"
    params = {
        "q": query,
        "startIndex": start_index,
        "maxResults": max_results,
        "printType": "books",
        "langRestrict": "ru",
    }
    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()
    data = response.json()

    books = []
    for item in data.get("items", []):
        info = item.get("volumeInfo", {})
        title = info.get("title", "Без названия")
        author = ", ".join(info.get("authors", ["Автор неизвестен"]))
        rating = info.get("averageRating")
        description = info.get("description", "Описание отсутствует.")
        published = info.get("publishedDate", "—")

        books.append(
            {
                "title": title,
                "author": author,
                "rating": rating,
                "description": short_text(description),
                "published": published,
            }
        )

    return books


def format_books_response(books: List[dict]) -> str:
    lines = ["📚 Вот что я нашел:\n"]
    for book in books:
        rating_text = f"{book['rating']}/5" if book["rating"] is not None else "нет оценки"
        lines.append(f"📖 {book['title']}")
        lines.append(f"✍ Автор: {book['author']}")
        lines.append(f"⭐ Оценка: {rating_text}")
        lines.append(f"🗓 Дата: {book['published']}")
        lines.append(f"📝 Краткий отзыв: {book['description']}")
        lines.append("───────────────\n")
    return "\n".join(lines)


def more_books_markup() -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("Еще книги", callback_data="more_books"))
    return markup


@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await message.answer(
        "Привет! Я помогу подобрать книги.\n"
        "Выбери жанр кнопкой или напиши свой запрос.",
        reply_markup=keyboard,
    )


@dp.message_handler(content_types=["text"])
async def handle_message(message: types.Message):
    query = normalize_query(message.text)
    user_id = message.from_user.id
    user_search_state[user_id] = {"query": query, "offset": BOOKS_PER_PAGE}

    await message.answer("🔎 Ищу книги...")

    try:
        books = search_books(query=query, start_index=0)
    except requests.RequestException:
        await message.answer("Не удалось получить данные о книгах. Попробуй позже.")
        return

    if not books:
        await message.answer("😔 Ничего не найдено. Попробуй изменить запрос.")
        return

    await message.answer(format_books_response(books), reply_markup=more_books_markup())


@dp.callback_query_handler(lambda c: c.data == "more_books")
async def handle_more_books(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    state = user_search_state.get(user_id)

    if not state:
        await callback_query.answer("Сначала отправь запрос на книги.", show_alert=True)
        return

    query = state["query"]
    offset = state["offset"]

    await callback_query.answer("Подбираю еще книги...")

    try:
        books = search_books(query=query, start_index=offset)
    except requests.RequestException:
        await bot.send_message(
            callback_query.message.chat.id,
            "Не удалось получить новые книги. Попробуй позже.",
        )
        return

    if not books:
        await bot.send_message(
            callback_query.message.chat.id,
            "Больше книг по этому запросу не нашлось. Напиши новый запрос.",
        )
        return

    state["offset"] = offset + BOOKS_PER_PAGE
    await bot.send_message(
        callback_query.message.chat.id,
        format_books_response(books),
        reply_markup=more_books_markup(),
    )


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
