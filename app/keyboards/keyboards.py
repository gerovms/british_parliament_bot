import re

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

main = InlineKeyboardMarkup(inline_keyboard=[
    [
        InlineKeyboardButton(text='По персонам',
                             callback_data='persons'),
        InlineKeyboardButton(text='По всем заседаниям',
                             callback_data='among_all')
        ],
        [
            InlineKeyboardButton(text='По ответам на письма',
                                 callback_data='writings')
                                 ]
        ]
        )

to_main = InlineKeyboardMarkup(
    inline_keyboard=[[InlineKeyboardButton(text='В главное меню',
                                           callback_data='back_to_menu'),]],
    )


async def build_searching_ways_keyboard(person: bool) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(text='В тексте',
                                 callback_data='in_texts'),
            InlineKeyboardButton(text='В заголовках',
                                 callback_data='in_headers')
        ],
        ]
    if person:
        keyboard.append([InlineKeyboardButton(
                text='Вернуться к вводу фамилии',
                callback_data='persons'
                )])
        keyboard.append([InlineKeyboardButton(
                text='Вернуться в главное меню',
                callback_data='back_to_menu'
                )])
    else:
        keyboard.append(
            [InlineKeyboardButton(
                text='Вернуться в главное меню',
                callback_data='back_to_menu'
                )]
        )
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


async def build_persons_keyboard(mps, page: int = 0) -> InlineKeyboardMarkup:
    page_items = mps[page]
    keyboard = []
    for name, link in page_items:
        keyboard.append(
            [InlineKeyboardButton(
                text=re.sub(r'<.*?>', '', name),
                callback_data=f'mp_{link}'
                )]
            )
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(
            text="⬅️ Назад",
            callback_data=f'page:{page - 1}'
            ))
    if page < len(mps) - 1:
        nav_buttons.append(InlineKeyboardButton(
            text="Вперёд ➡️",
            callback_data=f'page:{page + 1}'
            ))
    if nav_buttons:
        keyboard.append(nav_buttons)
    keyboard.append(
            [InlineKeyboardButton(
                text='Вернуться к вводу фамилии',
                callback_data='persons'
                )]
            )
    keyboard.append(
            [InlineKeyboardButton(
                text='Вернуться в главное меню',
                callback_data='back_to_menu'
                )]
            )
    return InlineKeyboardMarkup(inline_keyboard=keyboard)
