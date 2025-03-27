import polars as pl

from bolt._config import Config
from bolt.core._datasource import Datasource, ETLState, RawDataOrigin


def test_extract_wrapper():
    Config.log_dir = None
    DATA = pl.DataFrame({"name": ["Sugar", "Spice"], "species": ["cat", "cat"]})

    test_datasource = Datasource(
        name="EXTRACT_TEST", source_dir=None, source_filename=None
    )
    assert test_datasource.state == ETLState.INIT
    assert test_datasource.raw_data is None
    assert test_datasource.has_raw_data is False
    assert test_datasource.data is None
    assert test_datasource.has_data is False
    assert test_datasource.raw_data_origin == RawDataOrigin.INIT

    @test_datasource.extract_wrapper
    def extract(obj, *args, **kwargs) -> pl.DataFrame:
        data = DATA.clone()
        return data

    assert test_datasource.raw_data_origin == RawDataOrigin.EXTRACTED
    assert test_datasource.state == ETLState.INIT
    assert test_datasource.is_extracted is False
    test_datasource.extract()
    assert test_datasource.is_extracted is True
    assert test_datasource.state == ETLState.EXTRACTED
    assert test_datasource.has_raw_data is True
    assert test_datasource.raw_data.equals(DATA)
    assert test_datasource.has_data is False
    assert test_datasource.state == ETLState.EXTRACTED
