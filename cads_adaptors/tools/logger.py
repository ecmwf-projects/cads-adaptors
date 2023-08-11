# TODO: use a better logger, where better means standardised to cads logs in terms of formatting
import logging
import os

logger = logging.getLogger("adaptors")
logger.setLevel(os.getenv("ADAPTORS_LOG_LEVEL", "DEBUG"))
ch = logging.StreamHandler()

logger.addHandler(ch)
