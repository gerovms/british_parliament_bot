import re

from .constants import FINISH_DATE, START_DATE


async def validate_date(from_date: str, to_date: str):
    return ((bool(re.fullmatch(r"[+-]?\d+", from_date.strip())) and
             bool(re.fullmatch(r"[+-]?\d+", to_date.strip()))
             ) and
            (int(from_date) >= START_DATE and
            int(to_date) <= FINISH_DATE) or
            (from_date == '0' and to_date == '0'))


async def validate_no_person_date(from_date: str, to_date: str, person: bool):
    return (from_date != '0' and to_date != '0') or person
