import logging
import os
from pathlib import Path

import redis.asyncio as aioredis
from aiogram import F, Router
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from dotenv import load_dotenv

import app.utils.parse as p

from ..db.db import get_conn
from ..keyboards import keyboards as kb
from ..messages import messages as m
from ..states import states as s
from ..tasks.tasks import background_parse_task
from ..utils import validators as v

router = Router()
BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

BOT_TOKEN = os.getenv("TOKEN")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_DB = int(os.getenv("REDIS_DB"))


@router.message(CommandStart())
async def cmd_start(message: Message):
    logging.info(f'{message.from_user.first_name} нажал на старт')
    await message.answer(m.START_MESSAGE, reply_markup=kb.main)


@router.callback_query(F.data == 'back_to_menu')
async def main_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer(m.MAIN_MENU_MESSAGE, reply_markup=kb.main)
    await callback.answer()


@router.callback_query(F.data == 'persons')
async def type_surname(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_reply_markup(reply_markup=None)
    await ask_for_surname(callback.message, state)
    await callback.answer()


async def ask_for_surname(message: Message, state: FSMContext):
    """Переход в состояние ввода фамилии"""
    await state.set_state(s.SearchByName.surname)
    await message.answer(
        m.TYPE_SURNAME_MESSAGE,
        reply_markup=kb.to_main
    )


@router.callback_query(F.data == 'among_all')
async def redir_to_ways(callback: CallbackQuery, state: FSMContext):
    await choose_searching_way(callback, state)
    await callback.answer()


@router.message(F.text, s.SearchByName.surname)
async def list_of_mps(message: Message, state: FSMContext):
    await state.clear()
    await state.update_data(surname=message.text)
    await state.update_data(chat_id=message.chat.id)
    await state.update_data(user_first_name=message.from_user.first_name)
    logging.info(f'{message.from_user.first_name} '
                 f'ввёл фамилию {message.text}')
    data = await state.get_data()
    data['surname'] = data['surname'].title()
    bot = message.bot  # берём уже существующий экземпляр
    conn = await get_conn()
    redis_client = aioredis.Redis(host=REDIS_HOST,
                                  port=REDIS_PORT,
                                  db=REDIS_DB)

    try:
        mps = await p.get_list_of_mps(data['surname'],
                                      data,
                                      conn,
                                      redis_client,
                                      bot)
    finally:
        await conn.close()
        await redis_client.close()
        await redis_client.connection_pool.disconnect()

    if not mps or not mps[0]:
        await message.answer(m.SURNAME_ERROR)
        await ask_for_surname(message, state)
        return
    if not mps or not mps[0]:
        await message.answer(
            m.SURNAME_ERROR
            )
        await ask_for_surname(message, state)
    else:
        await state.update_data(mps=mps)
        await message.answer(
            await m.build_persons_message(mps=mps),
            parse_mode='HTML',
            reply_markup=await kb.build_persons_keyboard(mps=mps),
            disable_web_page_preview=True
        )


@router.callback_query(F.data.startswith('page:'))
async def change_page(callback: CallbackQuery, state: FSMContext):
    page = int(callback.data.split(':')[1])
    data = await state.get_data()
    mps = data.get("mps")
    if len(mps[0]) != 0:
        await callback.message.edit_text(
            text=await m.build_persons_message(mps=mps, page=page),
            parse_mode='HTML',
            reply_markup=await kb.build_persons_keyboard(mps, page),
            disable_web_page_preview=True
        )
    await callback.answer()


@router.callback_query(F.data == "back_to_surname")
async def back_to_menu_handler(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await type_surname(callback.message, state)
    await callback.answer()


@router.callback_query(F.data == 'writings')
async def writings_choose_searching_way(callback: CallbackQuery,
                                        state: FSMContext):
    keyboard = await kb.build_searching_ways_keyboard(person=False)
    await state.update_data(writings=True)
    await state.update_data(user_first_name=callback.from_user.first_name)
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer(
        text=m.CHOOSE_WAY_MESSAGE,
        reply_markup=keyboard
    )
    await callback.answer()


@router.callback_query(F.data.startswith('mp_'))
async def choose_searching_way(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    if 'mp_' in callback.data:
        callback_person_info = callback.data[3:]
        await state.update_data(person_info=callback_person_info)
        keyboard = await kb.build_searching_ways_keyboard(person=True)
    else:
        keyboard = await kb.build_searching_ways_keyboard(person=False)
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer(
        text=m.CHOOSE_WAY_MESSAGE,
        reply_markup=keyboard
    )
    await callback.answer()


@router.callback_query(F.data.startswith('in_'))
async def type_key_word(callback: CallbackQuery, state: FSMContext):
    await state.set_state(s.SearchByWord.keyword)
    await state.update_data(way=callback.data)
    logging.info(f'{callback.from_user.first_name} '
                 f'выбрал {callback.data}')
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer(m.TYPE_KEYWORD_MESSAGE,
                                  reply_markup=kb.to_main)
    await callback.answer()


@router.message(F.text, s.SearchByWord.keyword)
async def type_from_date(message: Message, state: FSMContext):
    logging.info(f'{message.from_user.first_name} '
                 f'ввёл ключевое слово {message.text}')
    await state.update_data(keyword=message)
    await state.set_state(s.SearchByWord.from_date)
    await message.answer(m.FROM_DATE_MESSAGE,
                         reply_markup=kb.to_main)


@router.message(F.text, s.SearchByWord.from_date)
async def type_to_date(message: Message, state: FSMContext):
    logging.info(f'{message.from_user.first_name} '
                 f'ввёл нач. дату {message.text}')
    await state.update_data(from_date=message.text)
    await state.set_state(s.SearchByWord.to_date)
    await message.answer(m.TO_DATE_MESSAGE,
                         reply_markup=kb.to_main)


@router.message(F.text, s.SearchByWord.to_date)
async def pre_parsing(message: Message, state: FSMContext):
    await state.update_data(to_date=message.text)
    await state.update_data(chat_id=message.chat.id)
    await state.update_data(user_first_name=message.from_user.first_name)
    logging.info(f'{message.from_user.first_name} '
                 f'ввёл кон. дату {message.text}')
    data = await state.get_data()
    validator_bool = await v.validate_date(data['from_date'],
                                           data['to_date'])
    no_person_date_bool = await v.validate_no_person_date(
        data['from_date'],
        data['to_date'],
        'person_info' in data.keys()
        )
    if validator_bool and no_person_date_bool:
        data['keyword'] = data['keyword'].text.upper()
        await message.answer(
                m.WAITING_MESSAGE,
                reply_markup=kb.to_main
                )
        background_parse_task.delay(data)
        await state.clear()
    else:
        await message.answer(
                m.DATE_ERROR
            )
        await type_from_date(data['keyword'], state)
