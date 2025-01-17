import logging
from pathlib import Path


def make_logger(name: str, log_dir: Path, stream=False, level=logging.DEBUG):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    formatter = logging.Formatter('[{asctime}] [{name}/{levelname}]: {message}', style='{')

    # Add file handler
    log_file = log_dir.joinpath(f"{name}.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)
    logger.addHandler(file_handler)
    return logger