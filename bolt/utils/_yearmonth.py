import re
import datetime as dt
from pathlib import Path


class YearMonth[T]:
    def __init__(self, yearmonth: str | int):
        self.yearmonth = int(yearmonth)
        self.year = int(str(yearmonth)[:4])
        self.month = int(str(yearmonth)[4:])

    @classmethod
    def from_date(cls, date: dt.date) -> T:
        """Converts a date (string) to a Year-Month."""
        ymth = int(date.strftime("%Y%m"))
        return YearMonth(ymth)

    @classmethod
    def from_date_string(cls, date_str: str, format: str) -> T:
        """Converts a date (string) to a Year-Month."""
        ymth = int(dt.datetime.strptime(date_str, format).strftime("%Y%m"))
        return YearMonth(ymth)

    @classmethod
    def from_filepath(cls, filepath: str | Path) -> T:
        """Parses a Year-Month from a path."""
        ymth = re.findall(r"^\d{6}", Path(filepath).name)[0]
        return YearMonth(ymth)

    @classmethod
    def from_ints(cls, year: int, month: int):
        return f"{year}{str(month).zfill(2)}"

    def to_year_and_month(self) -> tuple[int, int]:
        """Returns a tuple of year and month."""
        return (self.year, self.month)

    def __repr__(self):
        return f"<YearMonth: {self.yearmonth}>"

    def __str__(self):
        return str(self.yearmonth)

    def __int__(self):
        return int(self.yearmonth)
