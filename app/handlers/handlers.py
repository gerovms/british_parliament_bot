import asyncio
import logging
import gc
import os

from aiogram import F, Router
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, FSInputFile, Message

from ..keyboards import keyboards as kb
from ..messages import messages as m
from ..states import states as s
from ..utils import parse as p
from ..utils import validators as v
from ..utils.making_file import save_parsed_data

router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message):
    logging.info(f'{message.from_user} нажал на старт')
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


@router.message(s.SearchByName.surname)
async def list_of_mps(message: Message, state: FSMContext):
    await state.update_data(surname=message.text)
    logging.info(f'{message.from_user} ввёл фамилию {message.text}')
    data = await state.get_data()
    data['surname'] = data['surname'].title()
    mps = await p.get_list_of_mps(data['surname'])
    if len(mps[0]) == 0:
        await message.answer(
            m.SURNAME_ERROR,
            reply_markup=kb.to_main
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
    if mps:
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
    logging.info(f'{callback.from_user} выбрал {callback.data}')
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer(m.TYPE_KEYWORD_MESSAGE,
                                  reply_markup=kb.to_main)
    await callback.answer()


@router.message(s.SearchByWord.keyword)
async def type_from_date(message: Message, state: FSMContext):
    logging.info(f'{message.from_user} ввёл ключевое слово {message.text}')
    await state.update_data(keyword=message)
    await state.set_state(s.SearchByWord.from_date)
    await message.answer(m.FROM_DATE_MESSAGE,
                         reply_markup=kb.to_main)


@router.message(s.SearchByWord.from_date)
async def type_to_date(message: Message, state: FSMContext):
    logging.info(f'{message.from_user} ввёл нач. дату {message.text}')
    await state.update_data(from_date=message.text)
    await state.set_state(s.SearchByWord.to_date)
    await message.answer(m.TO_DATE_MESSAGE,
                         reply_markup=kb.to_main)


@router.message(s.SearchByWord.to_date)
async def pre_parsing(message: Message, state: FSMContext):
    await state.update_data(to_date=message.text)
    logging.info(f'{message.from_user} ввёл кон. дату {message.text}')
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
        asyncio.create_task(background_parse(message, data))
    else:
        await message.answer(
                m.DATE_ERROR,
                reply_markup=kb.to_main
            )
        await type_from_date(data['keyword'], state)


async def parse_and_send(message: Message, parsed_data, filename):
    file_path = await save_parsed_data(parsed_data, filename)
    if not os.path.exists(file_path):
        await message.answer("Файл не удалось создать ❌",
                             reply_markup=kb.to_main)
        return

    document = FSInputFile(file_path, filename=filename)
    
    await message.answer_document(document,
                                  caption="Вот твой файл с результатами 📄",
                                  reply_markup=kb.to_main)
    logging.info(f'{message.from_user} получил файл')
    gc.collect()


async def background_parse(message: Message, data: dict):
    result, filename = await p.parsing_fork(data)
    await parse_and_send(message, result, filename)