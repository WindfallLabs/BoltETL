"""RiderAccount report from Via"""

import pandas as pd

from . import Datasource


class RiderAccounts(Datasource):
    def __init__(self):
        super().__init__()

    def transform(self):
        """."""
        acc = self.raw[0][1].copy()
        acc["Birth Date"] = pd.to_datetime(acc["Birth Date"], errors="coerce")
        acc["Account Creation Date"] = pd.to_datetime(
            acc["Account Creation Date"], errors="coerce"
        )
        # New Fields
        acc["Since"] = acc["Account Creation Date"].apply(
            lambda x: int(f"{x.year}{str(x.month).zfill(2)}")
        )
        self.data = acc
        return
