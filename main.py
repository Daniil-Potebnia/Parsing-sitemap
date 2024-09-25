import asyncio
import os
import io

import requests
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


def parse(sitemap: str) -> (dict, bool):
    res = {'URL': [], 'Title': [], 'Description': [], 'Keywords': [],
           'h1': [], 'h2': [], 'h3': [], 'h4': [], 'h5': [], 'h6': []}
    if not sitemap.endswith('.xml'):
        return res, False
    try:
        response = requests.get(sitemap)
    except:
        return res, False
    if response.status_code != 200:
        return res, False
    root = etree.fromstring(response.content)
    namespaces = {
        'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9',
        'image': 'http://www.google.com/schemas/sitemap-image/1.1'
    }

    urls = root.xpath('//ns:url', namespaces=namespaces)
    for url in urls:
        loc = url.find('ns:loc', namespaces).text if url.find('ns:loc', namespaces) is not None else 'Нет URL'
        try:
            response = requests.get(loc)
        except:
            continue
        if response.status_code != 200:
            continue

        res['URL'].append(loc)

        soup = BeautifulSoup(response.text, 'html.parser')

        title = soup.title.string if soup.title else 'Нет title'
        res['Title'].append(title)
        description = soup.find('meta', attrs={'name': 'description'})
        description = description['content'] if description else 'Нет description'
        res['Description'].append(description)

        keywords = soup.find('meta', attrs={'name': 'keywords'})
        keywords = keywords['content'] if keywords else 'Нет keywords'
        res['Keywords'].append(keywords)

        for i in range(1, 7):
            res[f'h{i}'].append([h.get_text() for h in soup.find_all(f'h{i}')])
    return res, True


def write_to_xlsx(data: dict):
    wb = Workbook()
    ws = wb.active
    ws.append(['URL', 'Title', 'Description', 'Keywords', 'H1', 'H2', 'H3', 'H4', 'H5', 'H6'])

    for i in range(len(data[list(data.keys())[0]])):
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


@dp.message(Command('sitemap'))
async def sitemap(message: Message, state: FSMContext) -> None:
    await message.answer('Отправьте ссылку на sitemap вашего сайта в формате: https://webjesus.ru/sitemap.xml')
    await state.set_state(SitemapStates.waiting_for_sitemap_link)


@dp.message(StateFilter(SitemapStates.waiting_for_sitemap_link))
async def getting_link(message: Message, state: FSMContext) -> None:
    url = message.text
    data, ok = parse(url)
    if ok:
        res = write_to_xlsx(data)
        file = BufferedInputFile(res.read(), filename='sitemap.xlsx')
        await state.clear()
        await message.answer_document(file)
    else:
        await message.answer('Некорректная ссылка')


async def main() -> None:
    bot = Bot(token=os.getenv("BOT_TOKEN"))
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())
