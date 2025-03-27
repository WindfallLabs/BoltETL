from pathlib import Path

import pytest

from bolt import Config, Datasource
from bolt.utils._logger import make_logger

artifacts_path = Path(__file__).parent / "artifacts"
log_path = artifacts_path / "logs"
if not log_path.exists():
    log_path.mkdir()


@pytest.fixture
def log_file():
    p = log_path / "log_test.log"
    if p.exists():
        p.unlink()
    yield p
    try:
        p.unlink()
    except Exception:
        pass


def test_datasource_logger(log_file):
    assert not log_file.exists()
    Config.log_dir = log_path
    ds = Datasource(
        name=log_file.stem,
        source_dir=artifacts_path,
        source_filename="not-real.csv",
    )
    msg = "I exist!"
    ds.logger.info(msg)
    assert msg in log_file.open().read()


def test_io_logger():
    iologger = make_logger("memory")
    # DEBUG
    debug_msg = "Debug message written to memory!"
    iologger.debug(debug_msg)
    # INFO
    info_msg = "Info message written to memory!"
    iologger.info(info_msg)
    # WARN
    warn_msg = "Warning message written to memory!"
    iologger.warning(warn_msg)
    # ERROR
    err_msg = "Error message written to memory!"
    iologger.error(err_msg)
    # CRITICAL
    crit_msg = "Critical message written to memory!"
    iologger.critical(crit_msg)
    # Tests
    iologger.log.seek(0)
    log = iologger.log.read()
    assert debug_msg in log
    assert info_msg in log
    assert warn_msg in log
    assert err_msg in log
    assert crit_msg in log
