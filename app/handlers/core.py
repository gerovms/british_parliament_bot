import logging
import os

from aiogram.types import FSInputFile, Message

from ..keyboards import keyboards as kb
from ..utils import parse as p
from ..utils.making_file import save_parsed_data


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
    logging.info(f'{message.from_user.first_name} получил файл')


async def background_parse(message: Message, data: dict):
    result, filename = await p.parsing_fork(data)
    await parse_and_send(message, result, filename)
