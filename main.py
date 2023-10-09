import asyncio
import logging
import sys
import time
from datetime import datetime
from math import ceil

import aiohttp
import psycopg
import psycopg_pool
from lxml.html import HtmlElement, document_fromstring

LIMIT_PER_HOST = 60
MAX_CONCURRENCY = 45
MAX_POPULAR_USER_CONCURRENCY = 4
BASE_URL = "http://letterboxd.com/"
USER_RATING_URL = (BASE_URL +
                   "{user}/films/rated/0.5-5.0/by/rated-date/page/{page}/")
ALLTIME_POPULAR_URL = BASE_URL + "members/popular/this/all-time/page/{page}/"
WEEKLY_POPULAR_URL = BASE_URL + "members/popular/this/week/page/{page}/"
FILM_URL = BASE_URL + "ajax/poster/film/{film_id}/std/125x187/"

pool: psycopg_pool.ConnectionPool = psycopg_pool.ConnectionPool("", open=False)
user_queue: asyncio.Queue[str] = asyncio.Queue()

logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger()
logging.getLogger("asyncio").setLevel(logging.INFO)


async def get_popular_users(client: aiohttp.ClientSession,
                            sem: asyncio.Semaphore) -> None:
    users_to_update = db_get_users_to_update()
    if len(users_to_update) > 0:
        log.debug(f"queueing {len(users_to_update)} users")
    for u in users_to_update:
        await user_queue.put(u)
    user_cache = db_get_cached_users()
    urls = [
        url.format(page=page) for page in range(1, 257)
        for url in [ALLTIME_POPULAR_URL, WEEKLY_POPULAR_URL]
    ]
    tasks = [
        asyncio.create_task(
            put_users(client, url, users_to_update + user_cache, sem))
        for url in urls
    ]
    await asyncio.gather(*tasks)
    log.debug("completed getting popular users")


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
        tasks = [
            asyncio.create_task(get_user_ratings(client, user, page))
            for page in range(2, total_pages + 1)
        ]
        tasks.append(
            asyncio.create_task(get_user_ratings(client, user, 1, doc)))
        results = await asyncio.gather(*tasks)
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
        log.debug(f"queueing {len(new_users)} users")
    [await user_queue.put(u) for u in new_users]
    db_add_users(new_users)


async def get_new_films(client: aiohttp.ClientSession,
                        sem: asyncio.Semaphore) -> None:
    log.debug("getting films from db ratings")
    film_ids = db_get_new_films_from_ratings()
    log.debug(f"got {len(film_ids)} films from db ratings")
    batch_size = 2000
    for i in range(ceil(len(film_ids) / batch_size)):
        tasks = [
            asyncio.create_task(get_film(client, f, sem))
            for f in film_ids[i * batch_size:(i + 1) * batch_size]
        ]
        films = await asyncio.gather(*tasks)
        db_upsert_films(films)
        log.debug(f"loaded {len(films)} films")


async def get_film(client: aiohttp.ClientSession, film_id: str,
                   sem: asyncio.Semaphore) -> tuple[str, str, int | None, str]:
    async with sem:
        doc = await fetch(client, FILM_URL.format(film_id=film_id))
    div = doc.cssselect("div.film-poster")[0]
    film_name = div.get("data-film-name")
    try:
        year = int(div.get("data-film-release-year"))
    except ValueError:
        year = None
    img = div.cssselect("img.image")[0]
    poster_url = img.get("src")
    return (film_id, film_name, year, poster_url)


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
            conc = MAX_CONCURRENCY - MAX_POPULAR_USER_CONCURRENCY
            asyncio.create_task(consume_users(client, asyncio.Semaphore(conc)))
            await get_popular_users(
                client, asyncio.Semaphore(MAX_POPULAR_USER_CONCURRENCY))
            await user_queue.join()
            await get_new_films(client, asyncio.Semaphore(MAX_CONCURRENCY))


def db_upsert_films(films: list[tuple[str, str, int | None, str]]) -> None:
    with pool.connection() as conn:
        conn.cursor().executemany(
            """INSERT INTO films (film_id, film_name, year, poster_url)
               VALUES(%s, %s, %s, %s) ON CONFLICT DO NOTHING""", films)


def db_get_new_films_from_ratings() -> list[str]:
    with pool.connection() as conn:
        cur = conn.execute("""SELECT DISTINCT film_id FROM ratings
                              EXCEPT
                              SELECT film_id FROM films""")
        return [row[0] for row in cur]


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
        with cur.copy("COPY ratings (user_name, film_id, rating) FROM STDIN"
                      ) as copy:
            for r in ratings:
                copy.write_row((user, r[0], r[1]))
        db_update_user_time(user, conn)
    log.debug(
        f"added {user} ratings, db insert took {time.time() - start_time:.3f}s"
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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS films (
                film_id    VARCHAR(255) PRIMARY KEY NOT NULL,
                film_name  VARCHAR(255) NOT NULL,
                year       INT NULL,
                poster_url VARCHAR(255) NULL
            )
        """)


if __name__ == "__main__":
    log.info("starting scrape")
    start_time = time.time()
    asyncio.run(main())
    log.info(f"completed in {time.time() - start_time}s")
