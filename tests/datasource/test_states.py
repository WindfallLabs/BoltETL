from boltetl.core._datasource import ETLState


def test_etlstate():
    state = ETLState.INIT
    assert state == ETLState.INIT
    assert repr(state) == "<ETLState.INIT>"

    state = ETLState.EXTRACTED
    assert state > ETLState.INIT

    state = ETLState.TRANSFORMED
    assert state >= ETLState.EXTRACTED
    assert state < ETLState.LOADED
