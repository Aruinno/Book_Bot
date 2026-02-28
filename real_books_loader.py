import requests
import json

queries = [
    "fantasy books",
    "business books",
    "psychology books",
    "detective novels",
    "romance novels",
    "self development books"
]

books = []

for query in queries:
    for start in range(0, 200, 40):
        url = f"https://www.googleapis.com/books/v1/volumes?q={query}&startIndex={start}&maxResults=40"
        response = requests.get(url)
        data = response.json()

        if "items" in data:
            for item in data["items"]:
                info = item.get("volumeInfo", {})

                book = {
                    "title": info.get("title"),
                    "author": ", ".join(info.get("authors", [])),
                    "genre": query,
                    "description": info.get("description", "Описание отсутствует"),
                    "rating": info.get("averageRating", 0)
                }

                books.append(book)

print(f"Собрано книг: {len(books)}")

with open("books.json", "w", encoding="utf-8") as f:
    json.dump(books, f, ensure_ascii=False, indent=2)

print("Файл books.json создан ✅")