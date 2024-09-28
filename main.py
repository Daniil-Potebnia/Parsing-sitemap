import asyncio
import os
import io

import aiohttp
from dotenv import load_dotenv, find_dotenv
from lxml import etree
from bs4 import BeautifulSoup
from openpyxl import Workbook

from aiogram.fsm.context import FSMContext
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message, BufferedInputFile
from aiogram.filters.state import State, StateFilter, StatesGroup

load_dotenv(find_dotenv())

dp = Dispatcher()


class SitemapStates(StatesGroup):
    waiting_for_sitemap_link = State()


async def parse(sp: str) -> dict:
    sites = {'URL': [], 'Title': [], 'Description': [], 'Keywords': [],
             'h1': [], 'h2': [], 'h3': [], 'h4': [], 'h5': [], 'h6': []}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(sp) as response:
                tasks = []
                text = await response.text()
                root = etree.fromstring(text.encode())
                namespaces = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

                urls = root.xpath('//ns:url', namespaces=namespaces)
                for url in urls:
                    loc = url.find('ns:loc', namespaces).text if url.find('ns:loc', namespaces) is not None else 'Нет URL'
                    tasks.append(get_data(session, loc, sites))
                await asyncio.gather(*tasks)
        except:
            pass
    return sites


async def get_data(session, loc, sites):
    try:
        async with session.get(loc) as response:
            sites['URL'].append(loc)

            soup = BeautifulSoup(await response.text(), 'html.parser')

            title = soup.title.string if soup.title else 'Нет title'
            sites['Title'].append(title)
            description = soup.find('meta', attrs={'name': 'description'})
            description = description['content'] if description else 'Нет description'
            sites['Description'].append(description)

            keywords = soup.find('meta', attrs={'name': 'keywords'})
            keywords = keywords['content'] if keywords else 'Нет keywords'
            sites['Keywords'].append(keywords)

            for i in range(1, 7):
                sites[f'h{i}'].append([h.get_text() for h in soup.find_all(f'h{i}')])
    except:
        pass


def write_to_xlsx(data: dict):  # записывает информацию с сайтов в массив байт
    wb = Workbook()
    ws = wb.active
    ws.append(['URL', 'Title', 'Description', 'Keywords', 'H1', 'H2', 'H3', 'H4', 'H5', 'H6'])
    length = len(data[list(data.keys())[0]])
    for i in range(length):
        ws.append([
            data['URL'][i],
            data['Title'][i],
            data['Description'][i],
            data['Keywords'][i],
            ', '.join(data['h1'][i]),
            ', '.join(data['h2'][i]),
            ', '.join(data['h3'][i]),
            ', '.join(data['h4'][i]),
            ', '.join(data['h5'][i]),
            ', '.join(data['h6'][i])
        ])

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output


async def find_all_sitemaps(sp: str) -> list:
    sps = list()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(sp) as response:
                text = await response.text()
                root = etree.fromstring(text.encode())
                namespaces = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                sitemaps = root.xpath('//ns:sitemap', namespaces=namespaces)
                for s in sitemaps:
                    loc = s.find('ns:loc', namespaces).text if s.find('ns:loc', namespaces) is not None else 'Нет Sitemap'
                    sps.append(loc)
                    sps.extend(await find_all_sitemaps(loc))
    except:
        pass
    return sps


@dp.message(Command('sitemap'))
async def sitemap(message: Message, state: FSMContext) -> None:
    await message.answer('Отправьте ссылку на sitemap вашего сайта в формате: https://webjesus.ru/sitemap.xml')
    await state.set_state(SitemapStates.waiting_for_sitemap_link)


@dp.message(StateFilter(SitemapStates.waiting_for_sitemap_link))
async def getting_link(message: Message, state: FSMContext) -> None:
    url = message.text
    sitemaps = await find_all_sitemaps(url)
    sitemaps += [url]
    for s in sitemaps:
        data = await parse(s)
        if len(data[list(data.keys())[0]]) == 0:
            continue
        await state.clear()
        res = write_to_xlsx(data)
        file = BufferedInputFile(res.read(), filename=f'{s[:-4]}.xlsx')
        await message.answer_document(file)


async def main() -> None:
    bot = Bot(token=os.getenv("BOT_TOKEN"))
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())
