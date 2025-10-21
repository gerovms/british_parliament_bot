import asyncio
import gc
import itertools
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional

import httpx
from aiogram import Bot
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from ..db.db import get_document, save_document
from .constants import (BASE_NO_PESON_URL, DELAY_TIME, FIRST_MONTH_DAY,
                        ITEMS_PER_PAGE, LAST_MONTH_DAY, MAIN_URL, MONTHS,
                        PERSON)


BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")
TOKEN = os.getenv("TOKEN")


async def get_list_of_mps(surname: str,
                          data: Dict) -> List[List[List[str]]] | str:
    list_of_mps_url = PERSON + surname[0].lower()
    async with httpx.AsyncClient() as client:
        page = await fetch_page(client, list_of_mps_url, data)
    if page is None:
        logging.warning(f'Страница {list_of_mps_url} '
                        'не получена, пропускаем')
        return []
    soup = BeautifulSoup(page, 'lxml')
    list_of_mps = soup.find_all('li', {'class': 'person'})
    list_of_desired_mps: List[List] = [[]]
    for person in list_of_mps:
        full_name = person.find('a').text
        if surname in full_name:
            person_link = person.find('a')['href']
            person_dates = person.find('span').text
            person_string = (
                f'<a href="{PERSON}{person_link}">{full_name}</a> '
                f'({person_dates})'
                )
            if len(list_of_desired_mps[-1]) == ITEMS_PER_PAGE:
                list_of_desired_mps.append([[person_string, person_link]])
            else:
                list_of_desired_mps[-1].append([person_string, person_link])
    return list_of_desired_mps


async def fetch_page(
        client: httpx.AsyncClient, url: str, data: Dict
        ) -> str | None:
    """
    Асинхронная загрузка страницы.
    Возвращает None, если страница недоступна (404) или ошибка сети.
    """
    logging.info(f'Парсим {url} для {data['user_first_name']}')
    retries = 3
    for attempt in range(retries):
        try:
            url = url.split('#')[0]
            row = await get_document(url)
            if not row:
                response = await client.get(
                    url, timeout=30.0, follow_redirects=True
                    )
                response.raise_for_status()
                await save_document(url=url, content=response.text)
                row = response.text
            return row
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logging.warning(f'404 Not Found: {url}, пропускаем')
                return None
            else:
                return None
        except (httpx.HTTPError, httpx.StreamError) as e:
            logging.error(f"Сетевая ошибка {e} при запросе {url}")
            if attempt < retries - 1:
                await asyncio.sleep(DELAY_TIME)
            else:
                bot = Bot(token=TOKEN)
                if data and 'from_date' in data:
                    await bot.send_message(data['chat_id'], text=(
                        'Произошла ошибка при запросе: '
                        f'{data['from_date']}, {data['to_date']}, '
                        f'{data['keyword']}\n\n'
                        'Повторите попытку.'),
                        )
                else:
                    await bot.send_message(data['chat_id'], text=(
                        'Произошла ошибка при запросе '
                        'списка персон.'),
                        )
                raise Exception
    return None
            

async def parsing_fork(data: Dict):
    result = [[f'По ключевому слову "{data["keyword"]}" найдено объектов: ']]
    logging.info(f'Начинаем парсить по {data}')
    async with httpx.AsyncClient() as client:
        if 'person_info' in data.keys():
            result = await person_parsing(data, client, result)
        else:
            result = await no_person_parsing(data, client, result)
    return await setting_file_headers(result, data)


async def setting_file_headers(result: List[List[str]], data: Dict):
    count_result = 0
    for item in range(1, len(result)):
        count_result += len(result[item])
    result[0][0] += str(count_result)
    if 'person_info' in data.keys():
        name = ' '.join(data['person_info'].split('-')).title()
        result[0][0] += f'; по персоне {name}'
    elif 'writings' in data.keys():
        result[0][0] += '; по письмам'
    else:
        result[0][0] += '; по заседаниям'
    if data['from_date'] != '0' and data['to_date'] != '0':
        result[0][0] += (f'; за период с {data['from_date']} '
                         f'по {data['to_date']}')
    else:
        result[0][0] += '; за весь период активности персоны.'
    if 'person_info' in data.keys():
        filename = (f'{data["person_info"]}.{data["keyword"]}.'
                    f'{data["from_date"]}.{data["to_date"]}.txt')
    elif 'writings' in data.keys():
        filename = (f'{data["keyword"]}.writings.'
                    f'{data["from_date"]}.{data["to_date"]}.txt')
    else:
        filename = (f'{data["keyword"]}.sittings.'
                    f'{data["from_date"]}.{data["to_date"]}.txt')
    gc.collect()
    return result, filename


async def person_parsing(data: Dict,
                         client: httpx.AsyncClient,
                         result: List[List[str]]):
    page = await fetch_page(client, f'{PERSON}{data["person_info"]}', data)
    primordial_soup = BeautifulSoup(page, 'lxml')
    years = primordial_soup.find_all('span', {'class': 'speeches-by-year'})
    del primordial_soup
    gc.collect()
    if years:
        if data['from_date'] == '0' and data['to_date'] == '0':
            for year in years:
                link_to_year = f'{MAIN_URL}{year.find('a')['href']}'
                if data['way'] == 'in_headers':
                    result += await parse_headers_with_person(
                        data,
                        link_to_year,
                        client
                        )
                elif data['way'] == 'in_texts':
                    result += await parse_texts_with_person(
                        data,
                        link_to_year,
                        client
                        )
        else:
            for year in range(int(data['from_date']),
                              int(data['to_date']) + 1):
                link_to_year = f'{PERSON}{data['person_info']}/{year}'
                if data['way'] == 'in_headers':
                    result += await parse_headers_with_person(
                        data,
                        link_to_year,
                        client
                        )
                elif data['way'] == 'in_texts':
                    result += await parse_texts_with_person(
                        data,
                        link_to_year,
                        client
                        )

async def no_person_parsing(data: Dict,
                            client: httpx.AsyncClient,
                            result: List[List[str]]):
    for year in range(int(data['from_date']), int(data['to_date']) + 1):
        for month in MONTHS:
            for day in range(FIRST_MONTH_DAY, LAST_MONTH_DAY + 1):
                link_to_day = f'{BASE_NO_PESON_URL}{year}/{month}/{day}'
                page = await fetch_page(client, link_to_day, data)
                if page is None:
                    continue
                soup = BeautifulSoup(page, 'lxml')
                if 'writings' in data.keys():
                    commons_tag = soup.find(
                        'h3',
                        {'id': 'commons_written_answers'}
                        )
                    lords_tag = soup.find(
                        'h3',
                        {'id': 'lords_written_answers'}
                    )
                else:
                    commons_tag = soup.find(
                        'h3',
                        {'id': 'commons'}
                        )
                    lords_tag = soup.find(
                        'h3',
                        {'id': 'lords'}
                    )
                if commons_tag:
                    ol_commons_tag = commons_tag.find_next_sibling()
                    commons_answers = ol_commons_tag.find_all('a')
                else:
                    commons_answers = []
                if lords_tag:
                    ol_lords_tag = lords_tag.find_next_sibling()
                    lords_answers = ol_lords_tag.find_all('a')
                else:
                    lords_answers = []
                del soup
                gc.collect()
                commons_lords = list(itertools.chain(commons_answers,
                                                     lords_answers))
                if data['way'] == 'in_headers':
                    result += await parse_headers_without_person(
                        data,
                        commons_lords,
                        year,
                        month,
                        day
                        )
                elif data['way'] == 'in_texts':
                    result += await parse_texts_without_person(
                        data,
                        commons_lords,
                        year,
                        month,
                        day,
                        client
                        )
    return result



async def parse_headers_with_person(data: Dict,
                                    link_to_year: str,
                                    client: httpx.AsyncClient):
    desired_data = []
    page = await fetch_page(client, link_to_year, data)
    if page is None:
        return desired_data
    soup = BeautifulSoup(page, 'lxml')
    contributions = soup.find_all('p', {'class': 'person-contribution'})
    del soup
    gc.collect()
    for contribution in contributions:
        title = contribution.find('a')
        date = contribution.find('span', {'class': 'date'}).text
        if data['keyword'] in title.text.upper():
            desired_data.append(
                [f'{date} {title.text} – {MAIN_URL}{title['href']}']
                )
    return desired_data


async def parse_texts_with_person(data: Dict,
                                  link_to_year: str,
                                  client: httpx.AsyncClient):
    desired_data = []
    page = await fetch_page(client, link_to_year, data)
    if page is None:
        return desired_data 
    soup = BeautifulSoup(page, 'lxml')
    contributions = soup.find_all('p', {'class': 'person-contribution'})
    del soup
    gc.collect()
    for contribution in contributions:
        title = contribution.find('a')
        date = contribution.find('span', {'class': 'date'}).text
        sub_page = await fetch_page(client, f'{MAIN_URL}{title["href"]}', data)
        if sub_page is None:
            continue
        sub_soup = BeautifulSoup(sub_page, 'lxml')
        sitting_text = await parse_sitting(sub_soup)
        if data['keyword'] in sitting_text:
            desired_data.append(
                [f'{date} {title.text} – {MAIN_URL}{title['href']}']
                )
    return desired_data


async def parse_headers_without_person(
                            data: Dict,
                            commons_lords: list,
                            year: int,
                            month: str,
                            day: int,
                            ):
    desired_data = []
    logging.info(f'Парсим {year}/{month}/{day}')
    for item in commons_lords:
        if data['keyword'] in item.text.upper():
            desired_data.append(
                [f'{year}.{month}.{day} {item.text} – '
                 f'{MAIN_URL}{item['href']}']
                )
    return desired_data


async def parse_texts_without_person(
                            data: Dict,
                            commons_lords: List,
                            year: int,
                            month: str,
                            day: int,
                            client: httpx.AsyncClient
                            ):
    desired_data = []
    logging.info(f'Парсим {year}/{month}/{day}')
    for item in commons_lords:
        page = await fetch_page(client, f'{MAIN_URL}{item["href"]}', data)
        if page is None:
            continue
        soup = BeautifulSoup(page, 'lxml')
        item_text = await parse_sitting(soup)
        del soup
        gc.collect()
        if data['keyword'] in item_text:
            desired_data.append(
                [f'{year}.{month}.{day} {item.text} – '
                 f'{MAIN_URL}{item['href']}']
                )
    return desired_data


async def parse_sitting(sub_soup):
    sitting_text = ''
    sitting_text_tags = sub_soup.find_all(
            'div',
            {'class': 'hentry member_contribution'}
            )
    for tag in sitting_text_tags:
        sitting_text += tag.text.upper()
    return sitting_text
