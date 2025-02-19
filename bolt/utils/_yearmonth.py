import datetime as dt
import re
from pathlib import Path

import polars as pl


class YearMonth[T]:
    dtype = pl.Int64

    def __init__(self, yearmonth: str | int):
        yearmonth_str = str(yearmonth)
        if not re.match(r'^\d{6}$', yearmonth_str):
            raise ValueError("YearMonth must be a 6-digit number (YYYYMM)")
        
        self._year = int(yearmonth_str[:4])
        self._month = int(yearmonth_str[4:])
        if not (1 <= self.month <= 12):
            raise ValueError("Month must be between 1 and 12")

        self._yearmonth = int(yearmonth_str)

    @property
    def year(self) -> int:
        """Get the year component."""
        return self._year

    @property
    def month(self) -> int:
        """Get the month component."""
        return self._month

    @property
    def yearmonth(self) -> int:
        """Get the combined yearmonth value."""
        return self._yearmonth

    @classmethod
    def from_date(cls, date: dt.date) -> T:
        """Converts a date (string) to a Year-Month."""
        ymth = int(date.strftime("%Y%m"))
        return cls(ymth)

    @classmethod
    def from_date_series(cls, date_col: pl.Expr) -> pl.Expr:
        return date_col.map_elements(
            lambda x: int(cls.from_date(x)), return_dtype=cls.dtype
        ).cast(pl.Int64).alias("YMTH")

    @classmethod
    def from_date_string(cls, date_str: str, format: str) -> T:
        """Converts a date (string) to a Year-Month."""
        ymth = int(dt.datetime.strptime(date_str, format).strftime("%Y%m"))
        return cls(ymth)

    @classmethod
    def from_filepath(cls, filepath: str | Path) -> T:
        """Parses a Year-Month from a path."""
        ymth = re.findall(r"^\d{6}", Path(filepath).name)[0]
        return cls(ymth)

    @classmethod
    def from_ints(cls, year: int, month: int):
        return cls(f"{year}{str(month).zfill(2)}")

    def to_year_and_month(self) -> tuple[int, int]:
        """Returns a tuple of year and month."""
        return (self.year, self.month)

    def as_series(self, col_name: str = "YMTH") -> pl.Series:
        """Return an expression defining a YMTH column literal."""
        return pl.lit(self.yearmonth, dtype=self.dtype).cast(pl.Int64).alias(col_name)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, YearMonth):
            raise TypeError("Type must be YearMonth")
        return self._yearmonth == other._yearmonth

    def __hash__(self) -> int:
        return hash(self._yearmonth)

    def __lt__(self, other: 'YearMonth') -> bool:
        if not isinstance(other, YearMonth):
            raise TypeError("Type must be YearMonth")
        return self._yearmonth < other._yearmonth

    def __le__(self, other: 'YearMonth') -> bool:
        if not isinstance(other, YearMonth):
            raise TypeError("Type must be YearMonth")
        return self._yearmonth <= other._yearmonth

    def __gt__(self, other: 'YearMonth') -> bool:
        if not isinstance(other, YearMonth):
            raise TypeError("Type must be YearMonth")
        return self._yearmonth > other._yearmonth

    def __ge__(self, other: 'YearMonth') -> bool:
        if not isinstance(other, YearMonth):
            raise TypeError("Type must be YearMonth")
        return self._yearmonth >= other._yearmonth

    def __repr__(self):
        return f"<YearMonth: {self.yearmonth}>"

    def __str__(self):
        return str(self.yearmonth)

    def __int__(self):
        return int(self.yearmonth)
