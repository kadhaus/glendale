from typing import Optional

import logging
import logging.config

from .config.conf import LOGGING_CONF


def setup_logger(log_level: Optional[str] = None):
    if log_level:
        LOGGING_CONF['root']['level'] = log_level.upper()
    logging.config.dictConfig(LOGGING_CONF)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
