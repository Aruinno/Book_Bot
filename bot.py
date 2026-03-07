import asyncio
import json
import logging
import os
import re
from html import escape
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
BOOKS_FILE = BASE_DIR / "books.json"
USERS_FILE = BASE_DIR / "users.json"
BOOKS_PER_PAGE = 4

SUPPORTED_LANGUAGES = ("kk", "ru", "en")
OPEN_LIBRARY_URL = "https://openlibrary.org/search.json"
OPEN_LIBRARY_LANGUAGE_CODES = {
    "kk": "kaz",
    "ru": "rus",
    "en": "eng",
}
OPEN_LIBRARY_TO_APP_LANGUAGE = {
    "kaz": "kk",
    "rus": "ru",
    "eng": "en",
}

LEGACY_GENRE_MAP = {
    "fantasy books": "fantasy",
    "business books": "business",
    "psychology books": "psychology",
    "detective novels": "detective",
    "romance novels": "romance",
    "self development books": "self_development",
}

GENRE_ORDER = [
    "fantasy",
    "psychology",
    "business",
    "detective",
    "romance",
    "self_development",
]

GENRES = {
    "fantasy": {
        "icon": "📚",
        "labels": {
            "kk": "Фэнтези",
            "ru": "Фэнтези",
            "en": "Fantasy",
        },
        "queries": {
            "kk": "фэнтези кітаптары",
            "ru": "книги фэнтези",
            "en": "fantasy books",
        },
        "aliases": [
            "fantasy",
            "фэнтези",
            "қиял ғажайып",
            "қиял-ғажайып",
        ],
    },
    "psychology": {
        "icon": "🧠",
        "labels": {
            "kk": "Психология",
            "ru": "Психология",
            "en": "Psychology",
        },
        "queries": {
            "kk": "психология кітаптары",
            "ru": "книги по психологии",
            "en": "psychology books",
        },
        "aliases": [
            "psychology",
            "психология",
            "психология кітаптары",
        ],
    },
    "business": {
        "icon": "💼",
        "labels": {
            "kk": "Бизнес",
            "ru": "Бизнес",
            "en": "Business",
        },
        "queries": {
            "kk": "бизнес кітаптары",
            "ru": "книги про бизнес",
            "en": "business books",
        },
        "aliases": [
            "business",
            "бизнес",
            "кәсіп",
            "кәсіпкерлік",
        ],
    },
    "detective": {
        "icon": "🕵️",
        "labels": {
            "kk": "Детектив",
            "ru": "Детектив",
            "en": "Detective",
        },
        "queries": {
            "kk": "детектив кітаптары",
            "ru": "детективные книги",
            "en": "detective novels",
        },
        "aliases": [
            "detective",
            "детектив",
            "mystery",
            "mystery books",
        ],
    },
    "romance": {
        "icon": "💖",
        "labels": {
            "kk": "Романтика",
            "ru": "Романтика",
            "en": "Romance",
        },
        "queries": {
            "kk": "романтика кітаптары",
            "ru": "романтические книги",
            "en": "romance novels",
        },
        "aliases": [
            "romance",
            "романтика",
            "махаббат",
            "love stories",
        ],
    },
    "self_development": {
        "icon": "🚀",
        "labels": {
            "kk": "Өзін-өзі дамыту",
            "ru": "Саморазвитие",
            "en": "Self-development",
        },
        "queries": {
            "kk": "өзін өзі дамыту кітаптары",
            "ru": "книги по саморазвитию",
            "en": "self development books",
        },
        "aliases": [
            "self development",
            "self-development",
            "self help",
            "саморазвитие",
            "өзін өзі дамыту",
            "өзін-өзі дамыту",
        ],
    },
}

TEXTS = {
    "kk": {
        "choose_language": "Тілді таңдаңыз:",
        "language_saved": "Тіл сақталды. Енді жанрды таңдаңыз немесе өз сұрауыңызды жазыңыз.",
        "genre_prompt": "Пернетақтадан жанрды таңдаңыз немесе қалаған кітабыңызды жазыңыз.",
        "searching": "Кітаптарды іздеп жатырмын...",
        "nothing_found": "Бұл сұрау бойынша кітаптар табылмады. Басқа жанрды немесе атауды жіберіңіз.",
        "load_error": "Қазір кітаптарды жүктеу мүмкін болмады. Кейінірек қайталап көріңіз.",
        "more_books": "Тағы кітаптар",
        "no_more_books": "Бұл сұрау бойынша басқа кітаптар табылмады.",
        "select_language_first": "Алдымен /start немесе /language арқылы тілді таңдаңыз.",
        "change_language": "🌐 Тілді өзгерту",
        "author": "Автор",
        "genre": "Жанр",
        "rating": "Рейтинг",
        "published": "Жылы",
        "description": "Сипаттама",
        "unknown_author": "Белгісіз автор",
        "no_rating": "жоқ",
        "no_description": "Сипаттама жоқ.",
        "language_updated": "Тіл мәзірі ашылды. Жаңа тілді таңдаңыз.",
        "free_text_hint": "Кез келген жанрды, кітап атауын немесе авторды жаза аласыз.",
    },
    "ru": {
        "choose_language": "Выбери язык:",
        "language_saved": "Язык сохранен. Теперь выбери жанр или напиши свой запрос.",
        "genre_prompt": "Выбери жанр на клавиатуре или напиши любой запрос по книгам.",
        "searching": "Ищу книги...",
        "nothing_found": "По этому запросу ничего не нашлось. Попробуй другой жанр или название.",
        "load_error": "Сейчас не удалось получить книги. Попробуй позже.",
        "more_books": "Еще книги",
        "no_more_books": "По этому запросу больше книг не найдено.",
        "select_language_first": "Сначала выбери язык через /start или /language.",
        "change_language": "🌐 Изменить язык",
        "author": "Автор",
        "genre": "Жанр",
        "rating": "Рейтинг",
        "published": "Год",
        "description": "Описание",
        "unknown_author": "Автор неизвестен",
        "no_rating": "нет",
        "no_description": "Описание отсутствует.",
        "language_updated": "Меню выбора языка открыто. Выбери новый язык.",
        "free_text_hint": "Можно написать любой жанр, название книги или автора.",
    },
    "en": {
        "choose_language": "Choose a language:",
        "language_saved": "Language saved. Now choose a genre or type your own request.",
        "genre_prompt": "Choose a genre from the keyboard or type any book request.",
        "searching": "Looking for books...",
        "nothing_found": "No books were found for this request. Try another genre or title.",
        "load_error": "I couldn't load books right now. Try again later.",
        "more_books": "More books",
        "no_more_books": "No more books were found for this request.",
        "select_language_first": "Choose a language first with /start or /language.",
        "change_language": "🌐 Change language",
        "author": "Author",
        "genre": "Genre",
        "rating": "Rating",
        "published": "Year",
        "description": "Description",
        "unknown_author": "Unknown author",
        "no_rating": "none",
        "no_description": "No description available.",
        "language_updated": "Language menu is open. Choose a new language.",
        "free_text_hint": "You can type any genre, book title, or author.",
    },
}

LANGUAGE_LABELS = {
    "kk": "Қазақша",
    "ru": "Русский",
    "en": "English",
}

LANGUAGE_ALIASES = {
    "kk": {"қазақша", "қазақ", "kazakh", "kaz", "kk", "kz"},
    "ru": {"русский", "рус", "russian", "ru"},
    "en": {"english", "eng", "en"},
}

CHANGE_LANGUAGE_ALIASES = {
    "change language",
    "изменить язык",
    "тілді өзгерту",
}

router = Router()
cover_cache: dict[str, str | None] = {}
CYRILLIC_RE = re.compile(r"[А-Яа-яЁёӘәҒғҚқҢңӨөҰұҮүІі]")
KAZAKH_SPECIFIC_RE = re.compile(r"[ӘәҒғҚқҢңӨөҰұҮүІі]")
KAZAKH_LATIN_MARKERS = (
    "qazaq",
    "qazaqstan",
    "kazakh",
    "kazakhstan",
    "mahabbat",
    "adebiet",
    "psikholog",
    "isker",
    "tili",
    "zhumb",
    "shytyr",
    "ertegi",
    "ghashyq",
    "qylmys",
    "jetistik",
)


def normalize_text(value: str) -> str:
    text = re.sub(r"[^\w\s]+", " ", value.lower(), flags=re.UNICODE)
    return " ".join(text.replace("_", " ").split())


def short_text(text: str | None, max_len: int = 340) -> str:
    normalized = " ".join((text or "").split())
    if not normalized:
        return ""
    if len(normalized) <= max_len:
        return normalized
    return normalized[: max_len - 3].rstrip() + "..."


def sanitize_image_url(url: str | None) -> str | None:
    if not url or not isinstance(url, str):
        return None
    cleaned = url.strip()
    if not cleaned:
        return None
    if cleaned.startswith("http://"):
        return "https://" + cleaned[len("http://") :]
    return cleaned


def detect_open_library_language(doc: dict[str, Any], requested_language: str) -> str:
    for code in doc.get("language", []) or []:
        app_language = OPEN_LIBRARY_TO_APP_LANGUAGE.get(code)
        if app_language:
            return app_language
    return requested_language


def text_matches_language(text: str | None, language: str) -> bool:
    sample = str(text or "").strip()
    if not sample:
        return False

    if language == "en":
        return True

    if language == "ru":
        return bool(CYRILLIC_RE.search(sample))

    normalized = normalize_text(sample)
    if KAZAKH_SPECIFIC_RE.search(sample):
        return True
    return any(marker in normalized for marker in KAZAKH_LATIN_MARKERS)


def title_matches_user_language(title: str | None, language: str) -> bool:
    return text_matches_language(title, language)


def description_matches_user_language(description: str | None, language: str) -> bool:
    sample = str(description or "").strip()
    if not sample:
        return False

    if language == "en":
        return True
    if language == "ru":
        return bool(CYRILLIC_RE.search(sample))
    return bool(KAZAKH_SPECIFIC_RE.search(sample))


def book_matches_user_language(book: dict[str, Any], language: str) -> bool:
    if book.get("language") != language:
        return False

    return title_matches_user_language(book.get("title"), language)


def language_priority(book_language: str, preferred_language: str) -> int:
    order = {
        "kk": ["kk", "ru", "en"],
        "ru": ["ru", "en", "kk"],
        "en": ["en", "ru", "kk"],
    }
    try:
        return len(order[preferred_language]) - order[preferred_language].index(book_language)
    except (KeyError, ValueError):
        return 0


def coerce_rating(value: Any) -> float | None:
    if value in (None, "", 0):
        return None
    try:
        return round(float(value), 1)
    except (TypeError, ValueError):
        return None


def compute_quality_score(book: dict[str, Any]) -> float:
    title_norm = normalize_text(book["title"])
    description_norm = normalize_text(book.get("description") or "")
    score = float(book.get("rating") or 0) * 20

    if book.get("author"):
        score += 8
    if book.get("image_url"):
        score += 18
    if book.get("description"):
        score += 4
    if book_matches_user_language(book, book.get("language") or ""):
        score += 12

    low_value_markers = (
        "magazine",
        "catalog",
        "catalogue",
        "bibliography",
        "index",
        "publishers weekly",
        "subject guide",
        "accessions",
    )
    if any(marker in title_norm for marker in low_value_markers):
        score -= 15
    if "description unavailable" in description_norm or "описание отсутствует" in description_norm:
        score -= 5

    return score


def normalize_book_record(raw_book: dict[str, Any]) -> dict[str, Any]:
    raw_genre = normalize_text(str(raw_book.get("genre", "")))
    genre_key = str(raw_book.get("genre_key") or LEGACY_GENRE_MAP.get(raw_genre) or "").strip()
    language = str(raw_book.get("language") or "en").strip().lower()

    if language not in SUPPORTED_LANGUAGES:
        language = "en"
    if genre_key not in GENRES:
        genre_key = "fantasy" if raw_genre == "fantasy books" else ""

    title = str(raw_book.get("title") or "Untitled").strip()
    author = str(raw_book.get("author") or "").strip()
    description = str(raw_book.get("description") or "").strip()

    book = {
        "title": title,
        "author": author,
        "genre_key": genre_key,
        "language": language,
        "description": description,
        "rating": coerce_rating(raw_book.get("rating")),
        "published": str(raw_book.get("published") or raw_book.get("publishedDate") or "").strip(),
        "image_url": sanitize_image_url(
            raw_book.get("image_url")
            or raw_book.get("thumbnail")
            or raw_book.get("cover_url")
        ),
        "info_url": str(raw_book.get("info_url") or raw_book.get("infoLink") or "").strip(),
    }
    book["quality_score"] = compute_quality_score(book)
    return book


def load_books_catalog() -> list[dict[str, Any]]:
    if not BOOKS_FILE.exists():
        return []

    try:
        with BOOKS_FILE.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        logging.exception("Failed to load books catalog")
        return []

    if not isinstance(payload, list):
        return []

    catalog: list[dict[str, Any]] = []
    for raw_book in payload:
        if not isinstance(raw_book, dict):
            continue
        book = normalize_book_record(raw_book)
        if not book["genre_key"]:
            continue
        catalog.append(book)

    return dedupe_books(catalog)


def load_user_store() -> dict[str, dict[str, Any]]:
    if not USERS_FILE.exists():
        return {}

    try:
        with USERS_FILE.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        logging.exception("Failed to load users data")
        return {}

    if isinstance(payload, list):
        return {
            str(user_id): {"language": None, "search": None}
            for user_id in payload
            if isinstance(user_id, int)
        }

    if not isinstance(payload, dict):
        return {}

    store: dict[str, dict[str, Any]] = {}
    for key, value in payload.items():
        if isinstance(value, dict):
            store[str(key)] = {
                "language": value.get("language"),
                "search": value.get("search"),
            }
    return store


def save_user_store() -> None:
    with USERS_FILE.open("w", encoding="utf-8") as handle:
        json.dump(USER_STORE, handle, ensure_ascii=False, indent=2)


def get_user_profile(user_id: int) -> dict[str, Any]:
    key = str(user_id)
    if key not in USER_STORE:
        USER_STORE[key] = {"language": None, "search": None}
    return USER_STORE[key]


def get_user_language(user_id: int) -> str | None:
    language = get_user_profile(user_id).get("language")
    if language in SUPPORTED_LANGUAGES:
        return language
    return None


def set_user_language(user_id: int, language: str) -> None:
    profile = get_user_profile(user_id)
    profile["language"] = language
    save_user_store()


def set_search_state(user_id: int, state: dict[str, Any] | None) -> None:
    profile = get_user_profile(user_id)
    profile["search"] = state
    save_user_store()


def get_search_state(user_id: int) -> dict[str, Any] | None:
    state = get_user_profile(user_id).get("search")
    return state if isinstance(state, dict) else None


def dedupe_books(books: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique_books: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    for book in books:
        key = (
            normalize_text(book["title"]),
            normalize_text(book.get("author") or ""),
            book.get("language") or "",
        )
        if key in seen:
            continue
        seen.add(key)
        unique_books.append(book)

    return unique_books


def resolve_language(text: str) -> str | None:
    normalized = normalize_text(text)
    for code, aliases in LANGUAGE_ALIASES.items():
        if normalized in aliases or normalized == normalize_text(LANGUAGE_LABELS[code]):
            return code
    return None


def wants_language_change(text: str) -> bool:
    normalized = normalize_text(text)
    return normalized in CHANGE_LANGUAGE_ALIASES


def resolve_genre_key(text: str) -> str | None:
    normalized = normalize_text(text)
    for genre_key, config in GENRES.items():
        aliases = list(config["aliases"])
        aliases.extend(config["labels"].values())
        aliases.extend(config["queries"].values())
        for alias in aliases:
            alias_normalized = normalize_text(alias)
            if normalized == alias_normalized or alias_normalized in normalized:
                return genre_key
    return None


def build_language_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=LANGUAGE_LABELS["kk"], callback_data="lang:kk"),
                InlineKeyboardButton(text=LANGUAGE_LABELS["ru"], callback_data="lang:ru"),
            ],
            [
                InlineKeyboardButton(text=LANGUAGE_LABELS["en"], callback_data="lang:en"),
            ],
        ]
    )


def build_genre_keyboard(language: str) -> ReplyKeyboardMarkup:
    rows: list[list[KeyboardButton]] = []
    current_row: list[KeyboardButton] = []

    for genre_key in GENRE_ORDER:
        label = GENRES[genre_key]["labels"][language]
        icon = GENRES[genre_key]["icon"]
        current_row.append(KeyboardButton(text=f"{icon} {label}"))
        if len(current_row) == 2:
            rows.append(current_row)
            current_row = []

    if current_row:
        rows.append(current_row)

    rows.append([KeyboardButton(text=TEXTS[language]["change_language"])])
    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
        input_field_placeholder=TEXTS[language]["genre_prompt"],
    )


def build_more_books_markup(language: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=TEXTS[language]["more_books"],
                    callback_data="more_books",
                )
            ]
        ]
    )


def format_rating(value: float | None, language: str) -> str:
    if value is None:
        return TEXTS[language]["no_rating"]
    return f"{value}/5"


def format_book_caption(book: dict[str, Any], language: str) -> str:
    texts = TEXTS[language]
    author = book.get("author") or texts["unknown_author"]
    raw_description = (
        book.get("description")
        if description_matches_user_language(book.get("description"), language)
        else ""
    )
    description = short_text(raw_description or texts["no_description"])
    genre_key = book.get("genre_key")
    genre_label = GENRES[genre_key]["labels"][language] if genre_key in GENRES else ""

    lines = [
        f"<b>{escape(book['title'])}</b>",
        f"{texts['author']}: {escape(author)}",
    ]

    if genre_label:
        lines.append(f"{texts['genre']}: {escape(genre_label)}")

    if book.get("published"):
        lines.append(f"{texts['published']}: {escape(book['published'])}")

    lines.append(f"{texts['rating']}: {escape(format_rating(book.get('rating'), language))}")
    lines.append(f"{texts['description']}: {escape(description)}")

    caption = "\n".join(lines)
    if len(caption) <= 1024:
        return caption

    shorter_description = short_text(description, max_len=160)
    lines[-1] = f"{texts['description']}: {escape(shorter_description)}"
    return "\n".join(lines)[:1024]


def get_local_books_by_genre(genre_key: str, language: str) -> list[dict[str, Any]]:
    books = [
        book
        for book in BOOKS_CATALOG
        if book["genre_key"] == genre_key and book_matches_user_language(book, language)
    ]
    books.sort(
        key=lambda book: (
            book["quality_score"],
            book.get("rating") or 0,
            book["title"],
        ),
        reverse=True,
    )
    return books


def paginate_books(books: list[dict[str, Any]], offset: int) -> tuple[list[dict[str, Any]], int]:
    page = books[offset : offset + BOOKS_PER_PAGE]
    return page, len(books)


def open_library_to_book(doc: dict[str, Any], language: str, fallback_genre: str | None = None) -> dict[str, Any]:
    first_sentence = doc.get("first_sentence")
    if isinstance(first_sentence, list):
        first_sentence = first_sentence[0] if first_sentence else None
    if isinstance(first_sentence, dict):
        first_sentence = first_sentence.get("value")

    subjects = doc.get("subject") or []
    description = (
        first_sentence
        or short_text(", ".join(subjects[:6]), 260)
        or ""
    )

    cover_id = doc.get("cover_i")
    image_url = f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg" if cover_id else None

    genre_key = fallback_genre or resolve_genre_key(" ".join(subjects[:12]))

    return {
        "title": str(doc.get("title") or doc.get("title_suggest") or "Untitled").strip(),
        "author": ", ".join(doc.get("author_name", [])[:3]).strip(),
        "genre_key": genre_key or "",
        "language": detect_open_library_language(doc, language),
        "description": description,
        "rating": coerce_rating(doc.get("ratings_average")),
        "published": str(doc.get("first_publish_year") or "").strip(),
        "image_url": sanitize_image_url(image_url),
        "info_url": f"https://openlibrary.org{doc['key']}" if doc.get("key") else "",
        "subjects": [subject for subject in subjects if isinstance(subject, str)],
        "quality_score": 0,
    }


def fetch_open_library_sync(
    query: str,
    language_code: str | None,
    offset: int,
    limit: int,
    fallback_genre: str | None = None,
    user_language: str = "en",
    strict_language: bool = True,
) -> tuple[list[dict[str, Any]], int]:
    params: dict[str, Any] = {
        "q": query,
        "offset": offset,
        "limit": limit,
    }
    if language_code:
        params["language"] = language_code

    url = f"{OPEN_LIBRARY_URL}?{urlencode(params)}"
    with urlopen(url, timeout=20) as response:
        payload = json.load(response)

    docs = payload.get("docs", [])
    books: list[dict[str, Any]] = []
    for doc in docs:
        if not isinstance(doc, dict):
            continue

        book = open_library_to_book(doc, user_language, fallback_genre=fallback_genre)
        if strict_language and not book_matches_user_language(book, user_language):
            continue
        books.append(book)

    return dedupe_books(books), int(payload.get("numFound") or len(books))


async def search_open_library(
    query: str,
    language: str,
    offset: int = 0,
    limit: int = BOOKS_PER_PAGE,
    fallback_genre: str | None = None,
    allow_fallback_languages: bool = False,
) -> tuple[list[dict[str, Any]], int] | None:
    attempts = [OPEN_LIBRARY_LANGUAGE_CODES.get(language)]
    if allow_fallback_languages:
        attempts.append(None)
        if language == "kk":
            attempts.extend(["rus", "eng"])
        elif language == "ru":
            attempts.append("eng")

    last_error: Exception | None = None
    checked: set[str | None] = set()

    for language_code in attempts:
        if language_code in checked:
            continue
        checked.add(language_code)

        try:
            books, total = await asyncio.to_thread(
                fetch_open_library_sync,
                query,
                language_code,
                offset,
                limit,
                fallback_genre,
                language,
                not allow_fallback_languages,
            )
        except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError) as error:
            last_error = error
            continue

        if books:
            return books, total

    if last_error is not None:
        logging.warning("Open Library request failed: %s", last_error)
        return None

    return [], 0


async def find_cover_for_book(book: dict[str, Any], language: str) -> str | None:
    cache_key = normalize_text(f"{book.get('title', '')} {book.get('author', '')}")
    if cache_key in cover_cache:
        return cover_cache[cache_key]

    query = " ".join(part for part in [book.get("title"), book.get("author")] if part).strip()
    if not query:
        cover_cache[cache_key] = None
        return None

    result = await search_open_library(
        query,
        language,
        offset=0,
        limit=1,
        allow_fallback_languages=True,
    )
    if not result:
        cover_cache[cache_key] = None
        return None

    books, _ = result
    image_url = books[0].get("image_url") if books else None
    cover_cache[cache_key] = image_url
    return image_url


async def send_books_page(
    bot: Bot,
    chat_id: int,
    books: list[dict[str, Any]],
    language: str,
    has_more: bool,
) -> None:
    for book in books:
        caption = format_book_caption(book, language)
        image_url = book.get("image_url") or await find_cover_for_book(book, language)

        if image_url:
            try:
                await bot.send_photo(chat_id=chat_id, photo=image_url, caption=caption)
                continue
            except TelegramBadRequest:
                logging.info("Falling back to text for %s", book["title"])

        await bot.send_message(chat_id=chat_id, text=caption)

    if has_more:
        await bot.send_message(
            chat_id=chat_id,
            text=TEXTS[language]["free_text_hint"],
            reply_markup=build_more_books_markup(language),
        )


async def fetch_page_for_query(
    query: str,
    language: str,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int, dict[str, Any]] | None:
    genre_key = resolve_genre_key(query)

    if genre_key:
        local_books = get_local_books_by_genre(genre_key, language)
        page, total = paginate_books(local_books, offset)
        if page:
            return page, total, {"kind": "local_genre", "value": genre_key}

        fallback_query = GENRES[genre_key]["queries"][language]
        result = await search_open_library(
            fallback_query,
            language,
            offset=offset,
            limit=BOOKS_PER_PAGE,
            fallback_genre=genre_key,
        )
        if result is None:
            return None

        books, total = result
        return books, total, {"kind": "open_library", "value": fallback_query}

    result = await search_open_library(query, language, offset=offset, limit=BOOKS_PER_PAGE)
    if result is None:
        return None

    books, total = result
    return books, total, {"kind": "open_library", "value": query}


async def ask_language(message: Message, language: str | None = None) -> None:
    text_language = language if language in SUPPORTED_LANGUAGES else "ru"
    await message.answer(
        TEXTS[text_language]["choose_language"],
        reply_markup=build_language_markup(),
    )


@router.message(CommandStart())
async def start_command(message: Message) -> None:
    set_search_state(message.from_user.id, None)
    await message.answer(
        TEXTS["ru"]["choose_language"],
        reply_markup=ReplyKeyboardRemove(),
    )
    await message.answer(
        TEXTS["ru"]["free_text_hint"],
        reply_markup=build_language_markup(),
    )


@router.message(Command("language"))
async def language_command(message: Message) -> None:
    current_language = get_user_language(message.from_user.id) or "ru"
    await message.answer(
        TEXTS[current_language]["language_updated"],
        reply_markup=build_language_markup(),
    )


@router.callback_query(F.data.startswith("lang:"))
async def language_selected(callback: CallbackQuery) -> None:
    language = callback.data.split(":", maxsplit=1)[1]
    if language not in SUPPORTED_LANGUAGES:
        await callback.answer()
        return

    set_user_language(callback.from_user.id, language)
    set_search_state(callback.from_user.id, None)

    await callback.message.answer(
        f"{TEXTS[language]['language_saved']}\n\n"
        f"{TEXTS[language]['genre_prompt']}",
        reply_markup=build_genre_keyboard(language),
    )
    await callback.answer()


@router.callback_query(F.data == "more_books")
async def more_books(callback: CallbackQuery, bot: Bot) -> None:
    language = get_user_language(callback.from_user.id)
    if not language:
        await callback.answer(TEXTS["ru"]["select_language_first"], show_alert=True)
        return

    search_state = get_search_state(callback.from_user.id)
    if not search_state:
        await callback.answer(TEXTS[language]["select_language_first"], show_alert=True)
        return

    offset = int(search_state.get("offset") or 0)
    query_value = str(search_state.get("value") or "")
    search_kind = search_state.get("kind")

    await callback.answer(TEXTS[language]["searching"])

    if search_kind == "local_genre":
        books = get_local_books_by_genre(query_value, language)
        page, total = paginate_books(books, offset)
        state_descriptor = {"kind": "local_genre", "value": query_value}
    else:
        result = await search_open_library(query_value, language, offset=offset, limit=BOOKS_PER_PAGE)
        if result is None:
            await bot.send_message(callback.message.chat.id, TEXTS[language]["load_error"])
            return
        page, total = result
        state_descriptor = {"kind": "open_library", "value": query_value}

    if not page:
        await bot.send_message(callback.message.chat.id, TEXTS[language]["no_more_books"])
        return

    next_offset = offset + len(page)
    has_more = next_offset < total
    set_search_state(
        callback.from_user.id,
        {
            **state_descriptor,
            "offset": next_offset,
        },
    )
    await send_books_page(bot, callback.message.chat.id, page, language, has_more)


@router.message(F.text)
async def text_message(message: Message, bot: Bot) -> None:
    if message.text.startswith("/"):
        return

    typed_language = resolve_language(message.text)
    if typed_language:
        set_user_language(message.from_user.id, typed_language)
        set_search_state(message.from_user.id, None)
        await message.answer(
            f"{TEXTS[typed_language]['language_saved']}\n\n"
            f"{TEXTS[typed_language]['genre_prompt']}",
            reply_markup=build_genre_keyboard(typed_language),
        )
        return

    language = get_user_language(message.from_user.id)
    if not language:
        await ask_language(message)
        return

    if wants_language_change(message.text):
        await message.answer(
            TEXTS[language]["language_updated"],
            reply_markup=build_language_markup(),
        )
        return

    await message.answer(TEXTS[language]["searching"])
    result = await fetch_page_for_query(message.text, language, offset=0)

    if result is None:
        await message.answer(TEXTS[language]["load_error"])
        return

    page, total, state_descriptor = result
    if not page:
        await message.answer(TEXTS[language]["nothing_found"])
        return

    next_offset = len(page)
    has_more = next_offset < total
    set_search_state(
        message.from_user.id,
        {
            **state_descriptor,
            "offset": next_offset,
        },
    )
    await send_books_page(bot, message.chat.id, page, language, has_more)


async def main() -> None:
    load_dotenv()
    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is not set in .env")

    bot = Bot(
        token=bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()
    dp.include_router(router)
    await dp.start_polling(bot)


BOOKS_CATALOG = load_books_catalog()
USER_STORE = load_user_store()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
