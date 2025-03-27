import polars as pl

from bolt._config import Config
from bolt.core._datasource import Datasource, RawDataOrigin


def test_data_wrapper():
    Config.log_dir = None
    DATA = pl.DataFrame({"name": ["Sugar", "Spice"], "species": ["cat", "cat"]})

    test_datasource = Datasource(name="TEST", source_dir=None, source_filename=None)

    assert test_datasource.raw_data_origin == RawDataOrigin.INIT

    @test_datasource.data_wrapper
    def set_data(obj, *args, **kwargs) -> pl.DataFrame:
        data = DATA.clone()
        return data

    assert test_datasource.raw_data_origin == RawDataOrigin.DIRECTLY_SET
    assert test_datasource.data.equals(DATA)
