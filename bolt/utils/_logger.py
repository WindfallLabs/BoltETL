import logging
from io import StringIO
from pathlib import Path

FORMATTER = logging.Formatter(
    "[{asctime}] [{name}/{levelname}]: {message}", style="{"
)


class IOLogRecord(logging.LogRecord):
    """Custom LogRecord class for IOLogger."""
    def __init__(self, name, level, pathname, lineno, msg, args, exc_info, func=None, sinfo=None, **kwargs):
        super().__init__(name, level, pathname, lineno, msg, args, exc_info, func, sinfo)
        self.asctime = self.created


class IOLogger(logging.Logger):
    """Logging-like object that writes to a StringIO."""
    def __init__(self, name: str, formatter: logging.Formatter, level=logging.DEBUG):
        super().__init__(name, level)
        self.log = StringIO()
        self.formatter = formatter or logging.Formatter(
            "[{asctime}] [{name}/{levelname}]: {message}", style="{"
        )

    def makeRecord(self, name, level, fn, lno, msg, args, exc_info,
                   func=None, extra=None, sinfo=None):
        """Override makeRecord to use IOLogRecord."""
        return IOLogRecord(name, level, fn, lno, msg, args, exc_info, func, sinfo)

    def handle(self, record):
        """Override handle to prevent writing to the console."""
        self.log.write(self.formatter.format(record) + "\n")

    def debug(self, msg, *args, **kwargs):
        super().debug(msg, *args, **kwargs)
        record = self.makeRecord(
            self.name, logging.DEBUG, "", 0, msg, args, None
        )
        self.log.write(self.formatter.format(record) + "\n")
        return

    def info(self, msg, *args, **kwargs):
        super().info(msg, *args, **kwargs)
        record = self.makeRecord(
            self.name, logging.INFO, "", 0, msg, args, None
        )
        self.log.write(self.formatter.format(record) + "\n")
        return

    def warning(self, msg, *args, **kwargs):
        super().warning(msg, *args, **kwargs)
        record = self.makeRecord(
            self.name, logging.WARNING, "", 0, msg, args, None
        )
        self.log.write(self.formatter.format(record) + "\n")
        return

    def error(self, msg, *args, **kwargs):
        super().error(msg, *args, **kwargs)
        record = self.makeRecord(
            self.name, logging.ERROR, "", 0, msg, args, None
        )
        self.log.write(self.formatter.format(record) + "\n")
        return

    def critical(self, msg, *args, **kwargs):
        super().critical(msg, *args, **kwargs)
        record = self.makeRecord(
            self.name, logging.CRITICAL, "", 0, msg, args, None
        )
        self.log.write(self.formatter.format(record) + "\n")
        return

'''
# Usage example
iologger = IOLogger("my_logger", FORMATTER)
iologger.debug("This is a debug message.")
iologger.info("This is an info message.")
iologger.warning("This is a warning message.")
iologger.error("This is an error message.")
iologger.critical("This is a critical message.")
print(iologger.log.getvalue())
'''


def make_logger(name: str, log_dir: Path|None, level=logging.DEBUG):
    if log_dir is None:
        return IOLogger(name, FORMATTER)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Add file handler
    log_file = log_dir.joinpath(f"{name}.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(FORMATTER)
    file_handler.setLevel(level)
    logger.addHandler(file_handler)
    return logger


__all__ = [
    "make_logger",
    "IOLogger",
]
