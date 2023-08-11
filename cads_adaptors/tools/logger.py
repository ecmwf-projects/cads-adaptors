# Taken from gecko/logger.py
import logging
import os

logging_level = os.getenv("ADAPTORS_LOG_LEVEL", "DEBUG")


class CustomFormatter(logging.Formatter):
    grey = "\x1b[38;20m"
    green = "\x1b[32m"
    yellow = "\x1b[33;20m"
    yellow_box = "\x1b[43;1;38m"
    red = "\x1b[31;20m"
    red_box = "\x1b[41;1;38m"
    bold_red = "\x1b[31;1m"
    white = "\x1b[37m"
    reset = "\x1b[0m"
    _format = "%(levelname)s: %(message)s"

    FORMATS = {
        logging.DEBUG: green + _format + reset,
        logging.INFO: grey + _format + reset,
        logging.WARNING: yellow_box + _format + reset,
        logging.ERROR: red_box + _format + reset,
        logging.CRITICAL: bold_red + _format + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


logger = logging.getLogger(logging_level)
ch = logging.StreamHandler()

ch.setFormatter(CustomFormatter())

logger.addHandler(ch)
