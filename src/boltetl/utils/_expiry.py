"""
Expiry enum allows users to easily specify update cycles.
WIP...
"""
import calendar
import datetime as dt


_supported = {
    "%a", "%A",
    "%w", "%W",
    "%",
    "%",
    "%",
    "%",
}


# This works if run every day
# But what about if scheduled for Monday, not run, runs on Tuesday?
class Schedule:
    def __init__(self):
        self._schedule: list | None = None

    def __len__(self):
        if self._schedule is None:
            return 0
        return len(self._schedule)

    def add(self, code: str, value: str):
        if self._schedule is None:
            self._schedule = []
        self._schedule.append(
            (code, value)
        )
        return

    def add_many(self, *args: list[tuple[str, str]]):
        if self._schedule is None:
            self._schedule = []
        for code, value in args:
            self._schedule.append(
                (code, value)
            )
        return

    def check(self):
        now: dt.datetime = dt.datetime.now()
        cal = calendar.monthcalendar(now.date().year, now.date().month)
        # x-th Tuesday of the month
        #[week[2-1] for week in cal if week[2-1] != 0][x]
        return any([now.strftime(i[0]) == i[1] for i in schedule])


schedule: list[tuple[str, str]] = [
    ("%a", "Wed"),
    ("%a", "Mon"),
    ("%d", "1"),
    ("%d", "15")
]


# Datasource expiry
("%w", "2[>]")  # Tuesday or later
("%w", "2[0]")  # First Tuesday of the month





