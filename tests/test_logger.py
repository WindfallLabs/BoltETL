from pathlib import Path

import pytest

from bolt import Datasource, Config
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
    msg = "Written to memory!"
    iologger.info(msg)
    iologger.log.seek(0)
    assert msg in iologger.log.read()
