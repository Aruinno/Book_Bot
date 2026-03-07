import json
import re
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE_DIR = Path(__file__).resolve().parent
BOOKS_FILE = BASE_DIR / "books.json"

OPEN_LIBRARY_URL = "https://openlibrary.org/search.json"
GOOGLE_BOOKS_URL = "https://www.googleapis.com/books/v1/volumes"

OPEN_LIBRARY_PER_QUERY = 36
OPEN_LIBRARY_OFFSETS = (0, 36)
GOOGLE_BOOKS_PER_QUERY = 20
GOOGLE_BOOKS_START_INDEXES = (0, 20)

REQUEST_TIMEOUT = 20
MAX_SUBJECTS = 10
MAX_SOURCE_QUERIES = 6
RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}
GOOGLE_BOOKS_DELAY_SECONDS = 1.0

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

LANGUAGE_BUILD_ORDER = ("kk", "ru", "en")
LANGUAGE_TARGETS = {
    "kk": 24,
    "ru": 48,
    "en": 52,
}
LANGUAGE_COVER_TARGETS = {
    "kk": 16,
    "ru": 36,
    "en": 40,
}

GENRE_ORDER = [
    "fantasy",
    "psychology",
    "business",
    "detective",
    "romance",
    "self_development",
]

OPEN_LIBRARY_QUERY_VARIANTS = {
    "fantasy": [
        'subject:fantasy',
        'subject:"fantasy fiction"',
        'subject:magic',
        'subject:dragons',
    ],
    "psychology": [
        'subject:psychology',
        'subject:psychoanalysis',
        'subject:"cognitive psychology"',
        'subject:emotions',
    ],
    "business": [
        'subject:business',
        'subject:entrepreneurship',
        'subject:management',
        'subject:marketing',
    ],
    "detective": [
        'subject:detective',
        'subject:"detective and mystery stories"',
        'subject:"crime fiction"',
        'subject:"private investigators"',
    ],
    "romance": [
        'subject:romance',
        'subject:"romance fiction"',
        'subject:"love stories"',
        'subject:courtship',
    ],
    "self_development": [
        'subject:"self-help techniques"',
        'subject:"self-confidence"',
        'subject:"motivation (psychology)"',
        'subject:"self-realization"',
    ],
}

GOOGLE_QUERY_VARIANTS = {
    "fantasy": {
        "ru": ["фэнтези", "магия", "драконы"],
        "kk": ["қиял-ғажайып", "ертегілер", "сиқыр"],
    },
    "psychology": {
        "ru": ["психология", "эмоциональный интеллект", "психоанализ"],
        "kk": ["қазақ психологиясы", "психология", "ұлттық психология"],
    },
    "business": {
        "ru": ["бизнес", "предпринимательство", "маркетинг"],
        "kk": ["кәсіпкерлік", "бизнес", "маркетинг"],
    },
    "detective": {
        "ru": ["детектив", "триллер", "частный детектив"],
        "kk": ["жұмбақ", "шытырман оқиға", "құпия"],
    },
    "romance": {
        "ru": ["любовный роман", "история любви", "романтика"],
        "kk": ["махаббат роман", "махаббат", "ғашықтар"],
    },
    "self_development": {
        "ru": ["саморазвитие", "личностный рост", "мотивация"],
        "kk": ["өзін-өзі дамыту", "эмоционалды интеллект", "жетістік"],
    },
}

GENRE_KEYWORDS = {
    "fantasy": [
        "fantasy",
        "magic",
        "wizard",
        "dragon",
        "fairy",
        "myth",
        "легенда",
        "маг",
        "дракон",
        "фэнтези",
        "қиял",
        "ертегі",
        "сиқыр",
    ],
    "psychology": [
        "psychology",
        "cognitive",
        "emotion",
        "therapy",
        "mind",
        "psychology",
        "психология",
        "эмоция",
        "тұлға",
        "психолог",
    ],
    "business": [
        "business",
        "entrepreneur",
        "management",
        "marketing",
        "finance",
        "leadership",
        "бизнес",
        "предприним",
        "маркетинг",
        "менеджмент",
        "кәсіп",
        "қаржы",
    ],
    "detective": [
        "detective",
        "mystery",
        "murder",
        "investigation",
        "crime fiction",
        "детектив",
        "тайна",
        "убийств",
        "жұмбақ",
        "құпия",
        "шытырман",
    ],
    "romance": [
        "romance",
        "love",
        "courtship",
        "relationship",
        "романтика",
        "любов",
        "роман",
        "махаббат",
        "ғашық",
        "сезім",
    ],
    "self_development": [
        "self help",
        "self development",
        "motivation",
        "success",
        "productivity",
        "саморазвит",
        "личностный рост",
        "мотивац",
        "өзін",
        "жетістік",
        "эмоционалды интеллект",
    ],
}

LOW_VALUE_TITLE_MARKERS = (
    "publishers weekly",
    "catalog",
    "catalogue",
    "bibliography",
    "subject guide",
    "accessions",
)

CYRILLIC_RE = re.compile(r"[А-Яа-яЁёӘәҒғҚқҢңӨөҰұҮүІі]")
KAZAKH_SPECIFIC_RE = re.compile(r"[ӘәҒғҚқҢңӨөҰұҮүІі]")
KAZAKH_LATIN_MARKERS = (
    "qazaq",
    "kazakh",
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

SEARCH_FIELDS = (
    "key",
    "title",
    "title_suggest",
    "author_name",
    "language",
    "first_publish_year",
    "ratings_average",
    "ratings_count",
    "cover_i",
    "subject",
    "first_sentence",
    "edition_count",
    "number_of_pages_median",
    "ebook_access",
)


def normalize_text(value: str) -> str:
    cleaned = "".join(char.lower() if char.isalnum() or char.isspace() else " " for char in value)
    return " ".join(cleaned.replace("_", " ").split())


def short_text(text: str | None, max_len: int = 320) -> str:
    normalized = " ".join((text or "").split())
    if not normalized:
        return ""
    if len(normalized) <= max_len:
        return normalized
    return normalized[: max_len - 3].rstrip() + "..."


def sanitize_image_url(url: str | None) -> str:
    if not url:
        return ""
    cleaned = str(url).strip()
    if cleaned.startswith("http://"):
        return "https://" + cleaned[len("http://") :]
    return cleaned


def coerce_float(value: Any) -> float | None:
    if value in (None, "", 0):
        return None
    try:
        return round(float(value), 1)
    except (TypeError, ValueError):
        return None


def coerce_int(value: Any) -> int | None:
    if value in (None, "", 0):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def extract_year(value: Any) -> int | str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = re.search(r"(1[5-9]\d{2}|20\d{2})", text)
    if not match:
        return ""
    return int(match.group(1))


def normalize_subjects(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []

    subjects: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            continue
        subject = " ".join(value.split()).strip()
        if not subject:
            continue
        key = subject.casefold()
        if key in seen:
            continue
        seen.add(key)
        subjects.append(subject)
        if len(subjects) >= MAX_SUBJECTS:
            break
    return subjects


def title_matches_language(title: str | None, language: str) -> bool:
    sample = str(title or "").strip()
    if not sample:
        return False
    if language == "en":
        return True
    if language == "ru":
        return bool(CYRILLIC_RE.search(sample))
    if KAZAKH_SPECIFIC_RE.search(sample):
        return True
    normalized = normalize_text(sample)
    return any(marker in normalized for marker in KAZAKH_LATIN_MARKERS)


def book_matches_language(book: dict[str, Any], language: str) -> bool:
    return book.get("language") == language and title_matches_language(book.get("title"), language)


def description_score(text: str) -> int:
    normalized = short_text(text, 800)
    if not normalized:
        return 0

    score = len(normalized.split())
    if any(marker in normalized for marker in ".!?;:"):
        score += 8
    if "," in normalized:
        score += 4
    if len(normalized) >= 120:
        score += 5
    return score


def detect_open_library_language(doc: dict[str, Any], requested_language: str) -> str:
    for code in doc.get("language", []) or []:
        app_language = OPEN_LIBRARY_TO_APP_LANGUAGE.get(code)
        if app_language:
            return app_language
    return requested_language


def pick_first_sentence(doc: dict[str, Any]) -> str:
    first_sentence = doc.get("first_sentence")
    if isinstance(first_sentence, list):
        first_sentence = first_sentence[0] if first_sentence else None
    if isinstance(first_sentence, dict):
        first_sentence = first_sentence.get("value")
    if isinstance(first_sentence, str):
        return short_text(first_sentence.strip(), 320)
    return ""


def pick_description(base_text: str | None, subjects: list[str]) -> str:
    return short_text(base_text or ", ".join(subjects[:6]), 280)


def book_key(book: dict[str, Any]) -> tuple[str, str, str]:
    return (
        normalize_text(book.get("title") or ""),
        normalize_text(book.get("author") or ""),
        str(book.get("language") or ""),
    )


def genre_match_score(book: dict[str, Any], genre_key: str) -> int:
    haystack = normalize_text(
        " ".join(
            [
                str(book.get("title") or ""),
                str(book.get("description") or ""),
                " ".join(book.get("subjects") or []),
            ]
        )
    )
    return sum(1 for keyword in GENRE_KEYWORDS.get(genre_key, []) if keyword in haystack)


def should_skip_book(book: dict[str, Any]) -> bool:
    title_norm = normalize_text(book.get("title") or "")
    if not title_norm or title_norm == "untitled":
        return True
    return any(marker in title_norm for marker in LOW_VALUE_TITLE_MARKERS)


def merge_subjects(current: list[str], incoming: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for value in [*current, *incoming]:
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        merged.append(value)
        if len(merged) >= MAX_SUBJECTS:
            break
    return merged


def merge_queries(current: list[str], incoming: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for value in [*current, *incoming]:
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        merged.append(value)
        if len(merged) >= MAX_SOURCE_QUERIES:
            break
    return merged


def choose_published_year(current: Any, incoming: Any) -> int | str:
    current_year = extract_year(current)
    incoming_year = extract_year(incoming)
    if isinstance(current_year, int) and isinstance(incoming_year, int):
        return min(current_year, incoming_year)
    if isinstance(current_year, int):
        return current_year
    if isinstance(incoming_year, int):
        return incoming_year
    return ""


def merge_book_records(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    if description_score(incoming.get("description") or "") > description_score(existing.get("description") or ""):
        existing["description"] = incoming["description"]

    incoming_rating = incoming.get("rating")
    existing_rating = existing.get("rating")
    if incoming_rating is not None and (
        existing_rating is None
        or incoming_rating > existing_rating
        or (
            incoming_rating == existing_rating
            and (incoming.get("ratings_count") or 0) > (existing.get("ratings_count") or 0)
        )
    ):
        existing["rating"] = incoming_rating

    if (incoming.get("ratings_count") or 0) > (existing.get("ratings_count") or 0):
        existing["ratings_count"] = incoming.get("ratings_count")

    if not existing.get("image_url") and incoming.get("image_url"):
        existing["image_url"] = incoming["image_url"]
    if not existing.get("info_url") and incoming.get("info_url"):
        existing["info_url"] = incoming["info_url"]
    if not existing.get("work_key") and incoming.get("work_key"):
        existing["work_key"] = incoming["work_key"]
    if (existing.get("edition_count") or 0) < (incoming.get("edition_count") or 0):
        existing["edition_count"] = incoming.get("edition_count")
    if not existing.get("pages_median") and incoming.get("pages_median"):
        existing["pages_median"] = incoming["pages_median"]
    if (not existing.get("ebook_access") or existing.get("ebook_access") == "no_ebook") and incoming.get("ebook_access"):
        existing["ebook_access"] = incoming["ebook_access"]

    existing["published"] = choose_published_year(existing.get("published"), incoming.get("published"))
    existing["subjects"] = merge_subjects(existing.get("subjects") or [], incoming.get("subjects") or [])
    existing["source_queries"] = merge_queries(existing.get("source_queries") or [], incoming.get("source_queries") or [])
    if not existing.get("author") and incoming.get("author"):
        existing["author"] = incoming["author"]

    return existing


def fetch_json(base_url: str, params: dict[str, Any], delay_seconds: float = 0.0, retries: int = 3) -> dict[str, Any]:
    url = f"{base_url}?{urlencode(params)}"
    request = Request(url, headers={"User-Agent": "BookBotCatalogBuilder/3.0"})

    if delay_seconds:
        time.sleep(delay_seconds)

    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                payload = json.load(response)
            if not isinstance(payload, dict):
                raise ValueError("Expected JSON object response")
            return payload
        except HTTPError as error:
            last_error = error
            if error.code not in RETRYABLE_HTTP_CODES or attempt == retries - 1:
                raise
            time.sleep((attempt + 1) * 1.5)
        except (URLError, TimeoutError, OSError, json.JSONDecodeError, ValueError) as error:
            last_error = error
            if attempt == retries - 1:
                raise
            time.sleep((attempt + 1) * 1.5)

    if last_error:
        raise last_error
    raise RuntimeError("Failed to fetch JSON payload")


def fetch_open_library_docs(query: str, language_code: str, offset: int = 0) -> list[dict[str, Any]]:
    params = {
        "q": query,
        "language": language_code,
        "offset": offset,
        "limit": OPEN_LIBRARY_PER_QUERY,
        "fields": ",".join(SEARCH_FIELDS),
    }
    payload = fetch_json(OPEN_LIBRARY_URL, params)
    docs = payload.get("docs", [])
    return [doc for doc in docs if isinstance(doc, dict)]


def fetch_google_books_items(query: str, language: str, start_index: int = 0) -> list[dict[str, Any]]:
    params = {
        "q": query,
        "langRestrict": language,
        "printType": "books",
        "maxResults": GOOGLE_BOOKS_PER_QUERY,
        "startIndex": start_index,
    }
    payload = fetch_json(GOOGLE_BOOKS_URL, params, delay_seconds=GOOGLE_BOOKS_DELAY_SECONDS)
    items = payload.get("items", [])
    return [item for item in items if isinstance(item, dict)]


def open_library_doc_to_book(doc: dict[str, Any], genre_key: str, requested_language: str, source_query: str) -> dict[str, Any]:
    subjects = normalize_subjects(doc.get("subject"))
    cover_id = doc.get("cover_i")
    first_sentence = pick_first_sentence(doc)
    work_key = str(doc.get("key") or "").strip()

    return {
        "title": str(doc.get("title") or doc.get("title_suggest") or "Untitled").strip(),
        "author": ", ".join(doc.get("author_name", [])[:3]).strip(),
        "genre_key": genre_key,
        "language": detect_open_library_language(doc, requested_language),
        "description": pick_description(first_sentence, subjects),
        "rating": coerce_float(doc.get("ratings_average")),
        "ratings_count": coerce_int(doc.get("ratings_count")),
        "published": extract_year(doc.get("first_publish_year")),
        "image_url": f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg" if cover_id else "",
        "info_url": f"https://openlibrary.org{work_key}" if work_key else "",
        "subjects": subjects,
        "edition_count": coerce_int(doc.get("edition_count")),
        "pages_median": coerce_int(doc.get("number_of_pages_median")),
        "ebook_access": str(doc.get("ebook_access") or "").strip(),
        "work_key": work_key,
        "source_queries": [source_query],
    }


def google_item_to_book(item: dict[str, Any], genre_key: str, requested_language: str, source_query: str) -> dict[str, Any] | None:
    info = item.get("volumeInfo")
    if not isinstance(info, dict):
        return None

    language = str(info.get("language") or "").strip().lower()
    if language != requested_language:
        return None

    title = str(info.get("title") or "").strip()
    if not title_matches_language(title, requested_language):
        return None

    subjects = normalize_subjects(info.get("categories"))
    description = pick_description(info.get("description"), subjects)
    image_links = info.get("imageLinks") or {}
    image_url = sanitize_image_url(image_links.get("thumbnail") or image_links.get("smallThumbnail"))
    if requested_language in {"ru", "kk"} and not image_url:
        return None

    return {
        "title": title,
        "author": ", ".join(info.get("authors", [])[:3]).strip(),
        "genre_key": genre_key,
        "language": requested_language,
        "description": description,
        "rating": coerce_float(info.get("averageRating")),
        "ratings_count": coerce_int(info.get("ratingsCount")),
        "published": extract_year(info.get("publishedDate")),
        "image_url": image_url,
        "info_url": str(info.get("infoLink") or item.get("selfLink") or "").strip(),
        "subjects": subjects,
        "edition_count": None,
        "pages_median": coerce_int(info.get("pageCount")),
        "ebook_access": "available" if item.get("saleInfo", {}).get("isEbook") else "",
        "work_key": f"/google/{item.get('id')}" if item.get("id") else "",
        "source_queries": [source_query],
    }


def count_bucket(
    catalog: dict[tuple[str, str, str], dict[str, Any]],
    genre_key: str,
    language: str,
    require_cover: bool = False,
) -> int:
    count = 0
    for book in catalog.values():
        if book.get("genre_key") != genre_key:
            continue
        if not book_matches_language(book, language):
            continue
        if require_cover and not book.get("image_url"):
            continue
        count += 1
    return count


def bucket_ready(catalog: dict[tuple[str, str, str], dict[str, Any]], genre_key: str, language: str) -> bool:
    return (
        count_bucket(catalog, genre_key, language) >= LANGUAGE_TARGETS[language]
        and count_bucket(catalog, genre_key, language, require_cover=True) >= LANGUAGE_COVER_TARGETS[language]
    )


def add_books_to_catalog(
    catalog: dict[tuple[str, str, str], dict[str, Any]],
    books: list[dict[str, Any]],
) -> int:
    before = len(catalog)
    for book in books:
        key = book_key(book)
        if key in catalog:
            catalog[key] = merge_book_records(catalog[key], book)
        else:
            catalog[key] = book
    return len(catalog) - before


def fetch_open_library_books(query: str, requested_language: str, genre_key: str) -> list[dict[str, Any]]:
    collected: dict[tuple[str, str, str], dict[str, Any]] = {}
    language_code = LANGUAGE_CODES[requested_language]

    for offset in OPEN_LIBRARY_OFFSETS:
        docs = fetch_open_library_docs(query, language_code, offset=offset)
        for doc in docs:
            book = open_library_doc_to_book(doc, genre_key, requested_language, query)
            if should_skip_book(book):
                continue
            if book.get("language") != requested_language:
                continue
            if requested_language in {"ru", "kk"} and not book_matches_language(book, requested_language):
                continue
            if genre_match_score(book, genre_key) == 0:
                continue

            key = book_key(book)
            if key in collected:
                collected[key] = merge_book_records(collected[key], book)
            else:
                collected[key] = book

    return list(collected.values())


def fetch_google_books(query: str, requested_language: str, genre_key: str) -> list[dict[str, Any]]:
    collected: dict[tuple[str, str, str], dict[str, Any]] = {}

    for start_index in GOOGLE_BOOKS_START_INDEXES:
        items = fetch_google_books_items(query, requested_language, start_index=start_index)
        for item in items:
            book = google_item_to_book(item, genre_key, requested_language, query)
            if not book:
                continue
            if should_skip_book(book):
                continue
            if genre_match_score(book, genre_key) == 0:
                continue

            key = book_key(book)
            if key in collected:
                collected[key] = merge_book_records(collected[key], book)
            else:
                collected[key] = book

    return list(collected.values())


def sort_catalog(books: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def sort_key(book: dict[str, Any]) -> tuple[Any, ...]:
        genre_index = GENRE_ORDER.index(book.get("genre_key")) if book.get("genre_key") in GENRE_ORDER else len(GENRE_ORDER)
        language_index = LANGUAGE_BUILD_ORDER.index(book.get("language")) if book.get("language") in LANGUAGE_BUILD_ORDER else len(LANGUAGE_BUILD_ORDER)
        cover_rank = 0 if book.get("image_url") else 1
        rating = -(book.get("rating") or 0)
        ratings_count = -(book.get("ratings_count") or 0)
        title = normalize_text(book.get("title") or "")
        return genre_index, language_index, cover_rank, rating, ratings_count, title

    return sorted(books, key=sort_key)


def build_catalog() -> tuple[list[dict[str, Any]], int]:
    catalog: dict[tuple[str, str, str], dict[str, Any]] = {}
    successful_queries = 0

    for genre_key in GENRE_ORDER:
        for language in LANGUAGE_BUILD_ORDER:
            if language in {"ru", "kk"}:
                for query in GOOGLE_QUERY_VARIANTS[genre_key][language]:
                    if bucket_ready(catalog, genre_key, language):
                        break
                    try:
                        books = fetch_google_books(query, language, genre_key)
                    except Exception as error:  # noqa: BLE001
                        print(f"Failed Google Books fetch {genre_key}/{language} '{query}': {error}")
                        continue

                    successful_queries += 1
                    added = add_books_to_catalog(catalog, books)
                    print(
                        f"google {genre_key}/{language}: +{added} unique from '{query}' "
                        f"(bucket {count_bucket(catalog, genre_key, language)} total, "
                        f"{count_bucket(catalog, genre_key, language, require_cover=True)} with cover)"
                    )

            for query in OPEN_LIBRARY_QUERY_VARIANTS[genre_key]:
                if bucket_ready(catalog, genre_key, language):
                    break
                try:
                    books = fetch_open_library_books(query, language, genre_key)
                except Exception as error:  # noqa: BLE001
                    print(f"Failed Open Library fetch {genre_key}/{language} '{query}': {error}")
                    continue

                successful_queries += 1
                added = add_books_to_catalog(catalog, books)
                print(
                    f"openlib {genre_key}/{language}: +{added} unique from '{query}' "
                    f"(bucket {count_bucket(catalog, genre_key, language)} total, "
                    f"{count_bucket(catalog, genre_key, language, require_cover=True)} with cover)"
                )

    return sort_catalog(list(catalog.values())), successful_queries


def write_catalog(books: list[dict[str, Any]]) -> None:
    with BOOKS_FILE.open("w", encoding="utf-8") as handle:
        json.dump(books, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def main() -> None:
    previous_count = 0
    if BOOKS_FILE.exists():
        try:
            with BOOKS_FILE.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
            if isinstance(payload, list):
                previous_count = len(payload)
        except (OSError, json.JSONDecodeError):
            previous_count = 0

    books, successful_queries = build_catalog()
    if successful_queries == 0 or not books:
        print("No successful queries completed. Existing books.json was left unchanged.")
        return

    write_catalog(books)
    line_count = sum(1 for _ in BOOKS_FILE.open("r", encoding="utf-8"))
    print(
        f"Saved {len(books)} books to {BOOKS_FILE} "
        f"(previously {previous_count}, now {line_count} lines)"
    )


if __name__ == "__main__":
    main()
