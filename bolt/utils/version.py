import datetime as dt
import os

from packaging.version import Version  # parse as parse_version


def from_file_mdate(fpath: str) -> Version:
    """Make a Version object from the Modification Date of a file."""
    ts: float = os.path.getmtime(fpath)
    mod_date: dt.datetime = dt.datetime.fromtimestamp(ts)
    minutes_since_midnight = mod_date.hour * 60 + mod_date.minute
    return Version(mod_date.strftime(f"%Y.%m.%d.{minutes_since_midnight}"))
