import logging
from http import HTTPStatus
from typing import Any, Dict, List

import requests_cache
from bs4 import BeautifulSoup

from .constants import (BASE_NO_PESON_URL, FIRST_MONTH_DAY,
                        ITEMS_PER_PAGE, LAST_MONTH_DAY,
                        MAIN_URL, MONTHS, PERSON)


def get_list_of_mps(surname: str) -> List[List[List[str]]] | str:
    list_of_mps_url = PERSON + surname[0].lower()
    session = requests_cache.CachedSession()
    response = session.get(list_of_mps_url)
    soup = BeautifulSoup(response.text, 'lxml')
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


def parsing_fork(data: Dict):
    result = [[f'По ключевому слову "{data["keyword"]}" найдено объектов: ']]
    logging.info(f'Начинаем парсить по {data}')
    if 'person_info' in data.keys():
        session = requests_cache.CachedSession()
        response = session.get(f'{PERSON}{data['person_info']}')
        primordial_soup = BeautifulSoup(response.text, 'lxml')
        years = primordial_soup.find_all('span', {'class': 'speeches-by-year'})
        if years:
            if data['from_date'] == '0' and data['to_date'] == '0':
                for year in years:
                    link_to_year = f'{MAIN_URL}{year.find('a')['href']}'
                    if data['way'] == 'in_headers':
                        result += parse_headers_with_person(data,
                                                                  link_to_year,
                                                                  session)
                    elif data['way'] == 'in_texts':
                        result += parse_texts_with_person(data,
                                                                link_to_year,
                                                                session)
            else:
                for year in range(int(data['from_date']),
                                  int(data['to_date']) + 1):
                    link_to_year = f'{PERSON}{data['person_info']}/{year}'
                    if data['way'] == 'in_headers':
                        result += parse_headers_with_person(data,
                                                                  link_to_year,
                                                                  session)
                    elif data['way'] == 'in_texts':
                        result += parse_texts_with_person(data,
                                                                link_to_year,
                                                                session)
    elif 'person_info' not in data.keys():
        session = requests_cache.CachedSession()
        for year in range(int(data['from_date']), int(data['to_date']) + 1):
            for month in MONTHS:
                for day in range(FIRST_MONTH_DAY, LAST_MONTH_DAY + 1):
                    link_to_day = f'{BASE_NO_PESON_URL}{year}/{month}/{day}'
                    response = session.get(f'{link_to_day}')
                    if response.status_code == HTTPStatus.NOT_FOUND:
                        continue
                    soup = BeautifulSoup(response.text, 'lxml')
                    commons_tag = soup.find('h3', {'id': 'commons'})
                    if commons_tag:
                        ol_tag = commons_tag.find_next_sibling()
                        commons_sittings = ol_tag.find_all('a')
                    else:
                        commons_sittings = []
                    lords_tag = soup.find('h3', {'id': 'lords'})
                    if lords_tag:
                        ol_tag = lords_tag.find_next_sibling()
                        lords_sittings = lords_tag.find_all('a')
                    else:
                        lords_sittings = []
                    if data['way'] == 'in_headers':
                        result += parse_headers_without_person(
                            data,
                            commons_sittings,
                            lords_sittings,
                            year,
                            month,
                            day
                            )
                    elif data['way'] == 'in_texts':
                        result += parse_texts_without_person(
                            data,
                            commons_sittings,
                            lords_sittings,
                            year,
                            month,
                            day,
                            session
                            )

    else:
        raise Exception
    count_result = 0
    for item in range(1, len(result)):
        count_result += len(result[item])
    result[0][0] += str(count_result)
    if 'person_info' in data.keys():
        name = ' '.join(data['person_info'].split('-')).title()
        result[0][0] += f'; по персоне {name}'
    if data['from_date'] != '0' and data['to_date'] != '0':
        result[0][0] += (f'; за период с {data['from_date']} '
                         f'по {data['to_date']}')
    else:
        result[0][0] += '; за весь период активности персоны.'
    if 'person_info' in data.keys():
        filename = (f'{data["person_info"]}.{data["keyword"]}.'
                    f'{data["from_date"]}.{data["to_date"]}.txt')
    else:
        filename = (f'{data["keyword"]}.'
                    f'{data["from_date"]}.{data["to_date"]}.txt')
    return result, filename


def parse_headers_with_person(data: Dict,
                                    link_to_year: str,
                                    session: requests_cache.CachedSession):
    desired_data = []
    response = session.get(f'{link_to_year}')
    soup = BeautifulSoup(response.text, 'lxml')
    contributions = soup.find_all('p', {'class': 'person-contribution'})
    for contribution in contributions:
        title = contribution.find('a')
        date = contribution.find('span', {'class': 'date'}).text
        if data['keyword'] in title.text.upper():
            desired_data.append(
                [f'{date} {title.text} – {MAIN_URL}{title['href']}']
                )
    return desired_data


def parse_texts_with_person(data: Dict,
                                  link_to_year: str,
                                  session: requests_cache.CachedSession):
    desired_data = []
    response = session.get(f'{link_to_year}')
    soup = BeautifulSoup(response.text, 'lxml')
    contributions = soup.find_all('p', {'class': 'person-contribution'})
    for contribution in contributions:
        title = contribution.find('a')
        date = contribution.find('span', {'class': 'date'}).text
        sub_response = session.get(f'{MAIN_URL}{title['href']}')
        sub_soup = BeautifulSoup(sub_response.text, 'lxml')
        sitting_text = parse_sitting(sub_soup)
        if data['keyword'] in sitting_text:
            desired_data.append(
                [f'{date} {title.text} – {MAIN_URL}{title['href']}']
                )
    return desired_data


def parse_headers_without_person(
                            data: Dict,
                            commons_sittings: Any,
                            lords_sittings: Any,
                            year: int,
                            month: str,
                            day: int,
                            ):
    desired_data = []
    logging.info(f'Парсим {year}/{month}/{day}')
    for sitting in commons_sittings:
        if data['keyword'] in sitting.text.upper():
            desired_data.append(
                [f'{year}.{month}.{day} {sitting.text} – '
                 f'{MAIN_URL}{sitting['href']}']
                )
    for sitting in lords_sittings:
        if data['keyword'] in sitting.text.upper():
            desired_data.append(
                [f'{year}.{month}.{day} {sitting.text} – '
                 f'{MAIN_URL}{sitting['href']}']
                )
    return desired_data


def parse_texts_without_person(
                            data: Dict,
                            commons_sittings: Any,
                            lords_sittings: Any,
                            year: int,
                            month: str,
                            day: int,
                            session: requests_cache.CachedSession
                            ):
    desired_data = []
    logging.info(f'Парсим {year}/{month}/{day}')
    for sitting in commons_sittings:
        response = session.get(f'{MAIN_URL}{sitting['href']}')
        soup = BeautifulSoup(response.text, 'lxml')
        sitting_text = parse_sitting(soup)
        if data['keyword'] in sitting_text:
            desired_data.append(
                [f'{year}.{month}.{day} {sitting.text} – '
                 f'{MAIN_URL}{sitting['href']}']
                )
    for sitting in lords_sittings:
        response = session.get(f'{MAIN_URL}{sitting['href']}')
        soup = BeautifulSoup(response.text, 'lxml')
        sitting_text = parse_sitting(soup)
        if data['keyword'] in sitting_text:
            desired_data.append(
                [f'{year}.{month}.{day} {sitting.text} – '
                 f'{MAIN_URL}{sitting['href']}']
                )
    return desired_data


def parse_sitting(sub_soup):
    sitting_text = ''
    sitting_text_tags = sub_soup.find_all(
            'div',
            {'class': 'hentry member_contribution'}
            )
    for tag in sitting_text_tags:
        sitting_text += tag.text.upper()
    return sitting_text
