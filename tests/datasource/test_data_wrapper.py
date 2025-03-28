import polars as pl

from bolt._config import Config
from bolt.core._datasource import Datasource, ETLState, RawDataOrigin


def test_data_wrapper():
    Config.log_dir = None
    DATA = pl.DataFrame({"name": ["Sugar", "Spice"], "species": ["cat", "cat"]})

    test_datasource = Datasource(name="TEST")
    assert test_datasource.name == "TEST"
    assert repr(test_datasource) == "<Datasource(name='TEST')>"
    assert test_datasource.raw_data is None
    assert test_datasource.has_raw_data is False
    assert test_datasource.data is None
    assert test_datasource.has_data is False
    assert test_datasource.raw_data_origin == RawDataOrigin.INIT
    assert test_datasource.metadata.datasource.name == "TEST"
    assert repr(test_datasource.metadata) == "<Metadata(datasource='TEST')>"

    @test_datasource.data_wrapper
    def set_data(obj, *args, **kwargs) -> pl.DataFrame:
        data = DATA.clone()
        return data

    assert test_datasource.raw_data_origin == RawDataOrigin.DIRECTLY_SET
    assert test_datasource.is_directly_set is True
    assert test_datasource.state >= ETLState.EXTRACTED
    assert test_datasource.is_extracted is True
    assert test_datasource.is_transformed is True
    assert test_datasource.has_data is True
    assert test_datasource.data.equals(DATA)
