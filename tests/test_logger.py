from pathlib import Path

import pytest

from bolt import Datasource, Options

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
    options = Options(log_dir=log_path)
    ds = Datasource(
        name=log_file.stem,
        source_dir=artifacts_path,
        source_filename="not-real.csv",
        options=options,
    )
    ds.logger.info("I exist!")
    assert "I exist!" in log_file.open().read()
