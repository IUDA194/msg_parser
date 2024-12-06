import aiohttp
import asyncio
import time
import sqlite3

# Инициализация базы данных
def init_db():
    conn = sqlite3.connect("messages.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id TEXT PRIMARY KEY,
            text TEXT,
            link TEXT
        )
    """)
    conn.commit()
    conn.close()

def message_exists(message_id: str, text: str) -> bool:
    conn = sqlite3.connect("messages.db")
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM messages WHERE id = ? OR text = ?", (message_id, text))
    exists = cursor.fetchone() is not None
    conn.close()
    return exists

# Добавление сообщения в БД
def save_message(message_id: str, text: str, link: str):
    conn = sqlite3.connect("messages.db")
    cursor = conn.cursor()
    cursor.execute("INSERT INTO messages (id, text, link) VALUES (?, ?, ?)", (message_id, text, link))
    conn.commit()
    conn.close()

async def search_posts(
    token: str,
    query: str,
    limit: int = 20,
    offset: int = 0,
    peer_type: str = "all",
    start_date: int = int(time.time()) - 14 * 24 * 60 * 60,  # Последние 2 недели
    end_date: int = int(time.time()),
    country: str = None,
    language: str = None,
    category: str = None,
    hide_forwards: int = 0,
    hide_deleted: int = 0,
    strong_search: int = 0,
    minus_words: str = None,
    extended_syntax: int = 0,
    extended: int = 0,
):
    url = "https://api.tgstat.ru/posts/search"
    params = {
        "token": token,
        "q": query,
        "limit": limit,
        "offset": offset,
        "peerType": peer_type,
        "startDate": start_date,
        "endDate": end_date,
        "country": country,
        "language": language,
        "category": category,
        "hideForwards": hide_forwards,
        "hideDeleted": hide_deleted,
        "strongSearch": strong_search,
        "minusWords": minus_words,
        "extendedSyntax": extended_syntax,
        "extended": extended,
    }

    params = {k: v for k, v in params.items() if v is not None}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            if response.status == 200:
                return await response.json()
            else:
                return {"error": f"HTTP {response.status}", "message": await response.text()}

async def send_message(token: str, chat_id: str, text: str, button_url: str):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    max_length = 4096 

    if len(text) > max_length:
        text = text[:max_length - 100] + "\n...\n[Сообщение обрезано, см. источник]"

    payload = {
        "chat_id": chat_id,
        "text": text,
        "reply_markup": {
            "inline_keyboard": [[
                {"text": "К источнику", "url": button_url}
            ]]
        }
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            if response.status == 200:
                return await response.json()
            else:
                return {"error": f"HTTP {response.status}", "message": await response.text()}


async def main():
    init_db()

    tg_token = "8040833673:AAFm9Ak3azSy4ODcJjtQABDQ3wT0j0ERvU0"
    tg_chat_id = "-1002262993551"
    api_token = "88d38c3ee83724f381015d05960d1405"
    query = """"обмен тасками" | "обмена тасками" | "обмен тасков" | "обмен таскам" | "обмену тасками" | "обмене тасками" | "обмен трафиком" | "обмена трафиком" | "обмен трафиков" | "обмен трафика" | "обмену трафиком" | "обмене трафиком" | "перелив аудитории" | "взаимные таски" | "взаимных тасок" | "взаимных тасков" | "партнерские таски" | "партнерских тасок" | "партнерских тасков" | "обмен аудиториями" | "обмен аудиторией" | "обмен задачами и трафиком" | "взаимные задачи для перелива аудитории" | "ищем партнеров для обмена трафиком" | "ищем партнеров для обмена тасками" | "обмін тасками" | "обміну тасками" | "обмін тасків" | "обміну таскам" | "обміну тасками" | "обміні тасками" | "обмін трафіком" | "обміну трафіком" | "обмін трафіків" | "обмін трафіка" | "обміну трафіком" | "обміні трафіком" | "перелив аудиторії" | "взаємні таски" | "взаємних тасок" | "взаємних тасків" | "партнерські таски" | "партнерських тасок" | "партнерських тасків" | "обмін аудиторіями" | "обмін аудиторією" | "обмін задачами і трафіком" | "взаємні задачі для переливу аудиторії" | "шукаємо партнерів для обміну трафіком" | "шукаємо партнерів для обміну тасками" | "task exchange" | "exchange of tasks" | "task swapping" | "exchange to tasks" | "exchange for tasks" | "in task exchange" | "traffic exchange" | "exchange of traffic" | "traffic swapping" | "exchange of traffic flows" | "traffic flow exchange" | "in traffic exchange" | "audience transfer" | "mutual tasks" | "mutual exchanges" | "mutual task swaps" | "partner tasks" | "partner exchanges" | "partner task swaps" | "audience exchange" | "exchange of audience" | "exchange of tasks and traffic" | "mutual tasks for audience transfer" | "looking for partners to exchange traffic" | "looking for partners to exchange tasks" """

    offset_count = 50
    offset = 0

    while True:
        try:
            search_result = await search_posts(token=api_token, query=query, extended_syntax=1, limit=50, offset=offset)

            if search_result:
                if search_result.get('response').get('items'):
                    print("got result")
                    for item in search_result.get('response').get('items'):
                        message_id = item.get("id", None)
                        text = item.get("text", "Без текста")
                        link = item.get("link", "#")

                        if message_id and not message_exists(message_id, text):
                            msg = await send_message(token=tg_token, chat_id=tg_chat_id, text=text, button_url=link)
                            print("wait 2s")
                            await asyncio.sleep(2)
                            save_message(message_id, text, link)
            else:
                break
        except:
            print("wait 10 min")
            await asyncio.sleep(600)
            offset = 0
            offset_count = 0
            
        offset += offset_count

# Запуск асинхронного кода
if __name__ == "__main__":
    asyncio.run(main())
