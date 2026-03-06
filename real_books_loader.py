import json
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

BASE_DIR = Path(__file__).resolve().parent
BOOKS_FILE = BASE_DIR / "books.json"
OPEN_LIBRARY_URL = "https://openlibrary.org/search.json"
BOOKS_PER_QUERY = 20

LANGUAGE_CODES = {
    "kk": "kaz",
    "ru": "rus",
    "en": "eng",
}

OPEN_LIBRARY_TO_APP_LANGUAGE = {
    "kaz": "kk",
    "rus": "ru",
    "eng": "en",
}

GENRE_QUERIES = {
    "fantasy": {
        "kk": "фэнтези кітаптары",
        "ru": "книги фэнтези",
        "en": "fantasy books",
    },
    "psychology": {
        "kk": "психология кітаптары",
        "ru": "книги по психологии",
        "en": "psychology books",
    },
    "business": {
        "kk": "бизнес кітаптары",
        "ru": "книги про бизнес",
        "en": "business books",
    },
    "detective": {
        "kk": "детектив кітаптары",
        "ru": "детективные книги",
        "en": "detective novels",
    },
    "romance": {
        "kk": "романтика кітаптары",
        "ru": "романтические книги",
        "en": "romance novels",
    },
    "self_development": {
        "kk": "өзін өзі дамыту кітаптары",
        "ru": "книги по саморазвитию",
        "en": "self development books",
    },
}


def short_text(text: str | None, max_len: int = 260) -> str:
    normalized = " ".join((text or "").split())
    if not normalized:
        return ""
    if len(normalized) <= max_len:
        return normalized
    return normalized[: max_len - 3].rstrip() + "..."


def detect_language(doc: dict[str, Any], requested_language: str) -> str:
    for code in doc.get("language", []) or []:
        app_language = OPEN_LIBRARY_TO_APP_LANGUAGE.get(code)
        if app_language:
            return app_language
    return requested_language


def doc_to_book(doc: dict[str, Any], genre_key: str, requested_language: str) -> dict[str, Any]:
    first_sentence = doc.get("first_sentence")
    if isinstance(first_sentence, list):
        first_sentence = first_sentence[0] if first_sentence else None
    if isinstance(first_sentence, dict):
        first_sentence = first_sentence.get("value")

    subjects = doc.get("subject") or []
    cover_id = doc.get("cover_i")

    return {
        "title": str(doc.get("title") or doc.get("title_suggest") or "Untitled").strip(),
        "author": ", ".join(doc.get("author_name", [])[:3]).strip(),
        "genre_key": genre_key,
        "language": detect_language(doc, requested_language),
        "description": first_sentence or short_text(", ".join(subjects[:6])),
        "rating": doc.get("ratings_average"),
        "published": doc.get("first_publish_year"),
        "image_url": f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg" if cover_id else "",
        "info_url": f"https://openlibrary.org{doc['key']}" if doc.get("key") else "",
    }


def fetch_books(query: str, requested_language: str, genre_key: str) -> list[dict[str, Any]]:
    params = {
        "q": query,
        "limit": BOOKS_PER_QUERY,
        "language": LANGUAGE_CODES[requested_language],
    }

    with urlopen(f"{OPEN_LIBRARY_URL}?{urlencode(params)}", timeout=20) as response:
        payload = json.load(response)

    books: list[dict[str, Any]] = []
    for doc in payload.get("docs", []):
        if not isinstance(doc, dict):
            continue
        books.append(doc_to_book(doc, genre_key, requested_language))

    return books


def dedupe_books(books: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique_books: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    for book in books:
        key = (
            book["title"].strip().lower(),
            book["author"].strip().lower(),
            book["language"],
        )
        if key in seen:
            continue
        seen.add(key)
        unique_books.append(book)

    return unique_books


def main() -> None:
    books: list[dict[str, Any]] = []

    for genre_key, language_queries in GENRE_QUERIES.items():
        for language, query in language_queries.items():
            try:
                fetched_books = fetch_books(query, language, genre_key)
            except Exception as error:  # noqa: BLE001
                print(f"Failed to fetch {genre_key}/{language}: {error}")
                continue

            print(f"{genre_key}/{language}: {len(fetched_books)} books")
            books.extend(fetched_books)

    result = dedupe_books(books)
    with BOOKS_FILE.open("w", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2)

    print(f"Saved {len(result)} books to {BOOKS_FILE}")


if __name__ == "__main__":
    main()
