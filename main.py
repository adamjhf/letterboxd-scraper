import asyncio
import time
from datetime import datetime

import aiohttp
import psycopg
import psycopg_pool
from lxml.html import HtmlElement, document_fromstring

LIMIT_PER_HOST = 60
MAX_USERS_CONCURRENCY = 40
MAX_POPULAR_USER_CONCURRENCY = 4
BASE_URL = "http://letterboxd.com/"
USER_RATING_URL = (
    BASE_URL +
    "{user}/films/rated/0.5-5.0/by/rated-date/page/{page}/")
ALLTIME_POPULAR_URL = BASE_URL + "members/popular/this/all-time/page/{page}/"
WEEKLY_POPULAR_URL = BASE_URL + "members/popular/this/week/page/{page}/"

pool: psycopg_pool.ConnectionPool = psycopg_pool.ConnectionPool("", open=False)
user_queue: asyncio.Queue[str] = asyncio.Queue()


async def get_popular_users(client: aiohttp.ClientSession,
                            sem: asyncio.Semaphore) -> None:
    users_to_update = db_get_users_to_update()
    if len(users_to_update) > 0:
        print(f"{datetime.now()}: queueing {len(users_to_update)} users")
    for u in users_to_update:
        await user_queue.put(u)
    user_cache = db_get_cached_users()
    urls = [
        url.format(page=page) for page in range(1, 257)
        for url in [ALLTIME_POPULAR_URL, WEEKLY_POPULAR_URL]
    ]
    pu = [
        put_users(client, url, users_to_update + user_cache, sem)
        for url in urls
    ]
    await asyncio.gather(*pu)
    print(f"{datetime.now()}: completed getting popular users")


async def consume_users(client: aiohttp.ClientSession,
                        sem: asyncio.Semaphore) -> None:
    while True:
        user = await user_queue.get()
        asyncio.create_task(start_get_user_ratings(client, user, sem))


async def start_get_user_ratings(client: aiohttp.ClientSession, user: str,
                                 sem: asyncio.Semaphore) -> None:
    try:
        async with sem:
            url = USER_RATING_URL.format(user=user, page=1)
            doc = await fetch(client, url)
            pages = doc.cssselect("div.paginate-pages a")
            total_pages = 1 if len(pages) == 0 else int(pages[-1].text_content())
            gur = [
                get_user_ratings(client, user, page)
                for page in range(2, total_pages + 1)
            ]
            results = await asyncio.gather(get_user_ratings(client, user, 1, doc),
                                           *gur)
            ratings = [rating for result in results for rating in result]
            db_add_user_ratings(user, ratings)
    finally:
        user_queue.task_done()


async def get_user_ratings(client: aiohttp.ClientSession,
                           user: str,
                           page: int,
                           doc: HtmlElement = None) -> list[tuple[str, float]]:
    if doc is None:
        url = USER_RATING_URL.format(user=user, page=page)
        doc = await fetch(client, url)
    list_items = doc.cssselect("ul.poster-list > li.poster-container")
    ratings: list[tuple[str, float]] = []
    for li in list_items:
        film_id = li.cssselect("div.film-poster")[0].get("data-film-slug")
        stars = li.cssselect("p > span.rating")[0].text_content()
        rating = stars_to_rating(stars)
        ratings.append((film_id, rating))
    return ratings


async def put_users(client: aiohttp.ClientSession, url: str,
                    existing_users: list[str], sem: asyncio.Semaphore) -> None:
    async with sem:
        doc = await fetch(client, url)
        els = doc.cssselect("table.person-table a.name")
        users = [el.get("href").strip("/") for el in els]
        new_users = [u for u in users if u not in existing_users]
        if len(new_users) > 0:
            print(f"{datetime.now()}: queueing {len(new_users)} users")
        [await user_queue.put(u) for u in new_users]
        db_add_users(new_users)


async def fetch(client: aiohttp.ClientSession, url: str) -> HtmlElement:
    async with client.get(url) as resp:
        return document_fromstring(await resp.text())


def stars_to_rating(stars: str) -> float:
    return stars.count("★") + 0.5 * stars.count("½")


async def main():
    with pool:
        db_create_schema()
        http_conn = aiohttp.TCPConnector(limit_per_host=LIMIT_PER_HOST)
        timeout = aiohttp.ClientTimeout(total=None)
        async with aiohttp.ClientSession(connector=http_conn,
                                         timeout=timeout) as client:
            asyncio.create_task(
                consume_users(client,
                              asyncio.Semaphore(MAX_USERS_CONCURRENCY)))
            await get_popular_users(
                client, asyncio.Semaphore(MAX_POPULAR_USER_CONCURRENCY))
    await user_queue.join()


def db_get_users_to_update() -> list[str]:
    with pool.connection() as conn:
        cur = conn.execute(
            "SELECT user_name FROM users WHERE last_updated IS NULL")
        return [row[0] for row in cur]


def db_get_cached_users() -> list[str]:
    with pool.connection() as conn:
        cur = conn.execute(
            "SELECT user_name FROM users WHERE last_updated IS NOT NULL")
        return [row[0] for row in cur]


def db_add_users(users: list[str]) -> None:
    with pool.connection() as conn:
        conn.cursor().executemany(
            "INSERT INTO users (user_name) VALUES(%s) ON CONFLICT DO NOTHING",
            [[u] for u in users])


def db_add_user_ratings(user: str, ratings: list[tuple[str, float]]) -> None:
    start_time = time.time()
    with pool.connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM ratings WHERE user_name = %s", [user])
        with cur.copy("COPY ratings (user_name, film_id, rating) FROM STDIN") as copy:
            for r in ratings:
                copy.write_row((user, r[0], r[1]))
        db_update_user_time(user, conn)
    print(
        f"{datetime.now()}: added {user} ratings, db insert took {time.time() - start_time:.3f}s"
    )


def db_update_user_time(user: str, conn: psycopg.Connection) -> None:
    conn.execute(
        "UPDATE users SET last_updated = CURRENT_TIMESTAMP WHERE user_name = %s",
        [user])


def db_create_schema() -> None:
    with pool.connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_name    VARCHAR(255) PRIMARY KEY NOT NULL,
                last_updated TIMESTAMP NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ratings (
                user_name VARCHAR(255) NOT NULL,
                film_id   VARCHAR(255) NOT NULL,
                rating    NUMERIC(3, 1) NOT NULL,
                PRIMARY KEY (user_name, film_id)
            )
        """)


if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    print(f"{datetime.now()}: completed in {time.time() - start_time}s")
