import asyncio
import time

import aiohttp
from lxml.html import HtmlElement, document_fromstring

LIMIT_PER_HOST = 60
BASE_URL = "http://letterboxd.com/"
USER_RATING_URL = (
    BASE_URL +
    "{user}/films/rated/0.5-5.0/by/rated-date/size/large/page/{page}/")
POPULAR_MEMBERS_URL = BASE_URL + "members/popular/this/all-time/page/{page}/"

user_queue: asyncio.Queue[str] = asyncio.Queue()
ratings: list[tuple[str, str, float]] = []


async def get_popular_users(client: aiohttp.ClientSession) -> None:
    urls = [POPULAR_MEMBERS_URL.format(page=page) for page in range(1, 2)]
    pu = [put_users(client, url) for url in urls]
    await asyncio.gather(*pu)


async def consume_users(client: aiohttp.ClientSession) -> None:
    while True:
        user = await user_queue.get()
        asyncio.create_task(start_get_user_ratings(client, user))


async def start_get_user_ratings(client: aiohttp.ClientSession,
                                 user: str) -> None:
    url = USER_RATING_URL.format(user=user, page=1)
    doc = await fetch(client, url)
    pages = doc.cssselect("div.paginate-pages a")
    total_pages = 1 if len(pages) == 0 else int(pages[-1].text_content())
    gur = [
        get_user_ratings(client, user, page)
        for page in range(2, total_pages + 1)
    ]
    await asyncio.gather(get_user_ratings(client, user, 1, doc), *gur)
    user_queue.task_done()
    print(f"Got ratings for user {user}")


async def get_user_ratings(client: aiohttp.ClientSession,
                           user: str,
                           page: int,
                           doc: HtmlElement = None) -> None:
    if doc is None:
        url = USER_RATING_URL.format(user=user, page=page)
        doc = await fetch(client, url)
    list_items = doc.cssselect("ul.poster-list > li.poster-container")
    for li in list_items:
        film_id = li.cssselect("div.film-poster")[0].get("data-film-slug")
        stars = li.cssselect("p > span.rating")[0].text_content()
        rating = stars_to_rating(stars)
        ratings.append((user, film_id, rating))


async def put_users(client: aiohttp.ClientSession, url: str) -> None:
    doc = await fetch(client, url)
    users = doc.cssselect("table.person-table a.name")
    [await user_queue.put(u.get("href").strip("/")) for u in users]


async def fetch(client: aiohttp.ClientSession, url: str) -> HtmlElement:
    async with client.get(url) as resp:
        return document_fromstring(await resp.text())


def stars_to_rating(stars: str) -> float:
    return stars.count("★") + 0.5 * stars.count("½")


async def main():
    conn = aiohttp.TCPConnector(limit_per_host=LIMIT_PER_HOST)
    async with aiohttp.ClientSession(connector=conn) as client:
        asyncio.create_task(consume_users(client))
        await get_popular_users(client)
        await user_queue.join()
    print(ratings[:20])
    print(len(ratings))


if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    print(f"Runtime: {time.time() - start_time}s")
