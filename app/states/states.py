from aiogram.fsm.state import State, StatesGroup


class SearchByName(StatesGroup):
    surname = State()


class SearchByWord(StatesGroup):
    keyword = State()
    from_date = State()
    to_date = State()
