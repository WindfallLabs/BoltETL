"""."""
from abc import ABC

from datasources import CR0174, ViaS10, DriverShifts


class Report(ABC):
    def update_data(self):
        for src in self.data_sources:
            src.update()
        return


class ElectrificationReport(Report):
    pass


class MonthlyNTDRidership(Report):
    data_sources = [
        CR0174(),
        ViaS10(),
        DriverShifts(),
        ...
    ]
