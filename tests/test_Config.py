import re
from pathlib import Path

import pytest


@pytest.fixture
def cfg():
    from bolt._config import Config

    Config.config_dir = Path(__file__).parent / "artifacts" / "conf"
    Config.env_file = Config.config_dir / ".env"
    yield Config
    Config.env_file.unlink()
    del Config


def test_init(cfg):
    assert not cfg.env_file.exists()
    cfg.init()
    assert cfg.config_dir.exists()
    assert cfg.env_file.exists()
    content = cfg.env_file.open().read()
    assert f"BOLT-ACTIVE={str(cfg.config_dir)}" in content


def test_add_env(cfg):
    cfg.init()
    cfg.add_env("TEST1", Path(__file__).parent / "artifacts", activate=True)
    cfg.add_env("TEST2", str(Path(__file__).parent / "artifacts"))  # string
    content = cfg.env_file.open().read()
    assert re.findall(r"TEST1=(.*?)\\artifacts", content)
    assert re.findall(r"TEST2=(.*?)\\artifacts", content)
    with pytest.raises(KeyError):
        cfg.add_env("TEST2", str(Path(__file__).parent / "artifacts"))  # string


def test_use_env(cfg):
    cfg.init()
    cfg.add_env("TEST", Path(__file__).parent / "artifacts", activate=True)
    cfg.use_env("TEST")
    assert "artifacts" in str(cfg.env_dir)


def test_reserved_keyword(cfg):
    cfg.init()
    with pytest.raises(KeyError):
        cfg.add_env("BOLT-ACTIVE", "...")


def test_get_default(cfg):
    cfg.init()
    assert cfg.get_default_env() == Path("~").expanduser() / ".bolt"


def test_list_envs(cfg):
    cfg.init()
    assert len(cfg.list_envs()) == 2
