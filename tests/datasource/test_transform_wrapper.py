import polars as pl

from boltetl._config import Config
from boltetl.core._datasource import Datasource, ETLState, RawDataOrigin


def test_extract_wrapper():
    Config.log_dir = None
    DATA = pl.DataFrame({"name": ["Sugar", "Spice"], "species": ["cat", "cat"]})
    TRANSFORMED = pl.DataFrame({"name": ["Sugar", "Spice"], "species": ["CAT", "CAT"]})

    test_datasource = Datasource(name="TRANSFORM_TEST", source_dir=None, source_filename=None)
    assert test_datasource.raw_data is None
    assert test_datasource.has_raw_data is False
    assert test_datasource.data is None
    assert test_datasource.has_data is False
    assert test_datasource.raw_data_origin == RawDataOrigin.INIT

    @test_datasource.extract_wrapper
    def extract(obj, *args, **kwargs) -> pl.DataFrame:
        data = DATA.clone()
        return data

    @test_datasource.transform_wrapper
    def transform(obj, *args, **kwargs) -> pl.DataFrame:
        transformed_data = obj.raw_data.with_columns(pl.col("species").str.to_uppercase())
        return transformed_data

    assert test_datasource.raw_data_origin == RawDataOrigin.EXTRACTED
    assert test_datasource.state == ETLState.INIT
    assert test_datasource.is_extracted is False
    test_datasource.extract()
    assert test_datasource.is_extracted is True
    assert test_datasource.state == ETLState.EXTRACTED
    assert test_datasource.has_raw_data is True
    assert test_datasource.raw_data.equals(DATA)
    assert test_datasource.has_data is False
    test_datasource.transform()
    assert test_datasource.has_data is True
    assert test_datasource.data.equals(TRANSFORMED)
    assert test_datasource.state == ETLState.TRANSFORMED
    assert test_datasource.is_transformed is True
