import re
from pathlib import Path


class YearMonth:
    def __init__(self, yearmonth: str|int):
        self.yearmonth = int(yearmonth)
        self.year = int(str(yearmonth)[:4])
        self.month = int(str(yearmonth)[4:])

    @classmethod
    def from_filepath(cls, filepath: str|Path):
        ymth = re.findall(r"^\d{6}", Path(filepath).name)[0]
        return YearMonth(ymth)

    def __repr__(self):
        return "<YearMonth: {self.yearmonth}>"

    def __str__(self):
        return str(self.yearmonth)
