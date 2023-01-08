from typing import Dict, Any

from pathlib import Path

_SRC_DIR = Path(__file__).parent.parent

INIT_DB_SQL_PATH = _SRC_DIR / 'static/init_db.sql'
DB_FILE_PATH = _SRC_DIR / 'data_source/main.sqlite3'

WEB: Dict[str, Any] = dict(
    api_keys_dir=_SRC_DIR / 'api_keys',
    oauth2_scopes=['https://www.googleapis.com/auth/indexing'],
    endpoint='https://indexing.googleapis.com/v3/urlNotifications:publish',
)

LOGGING_CONF: Dict[str, Any] = {
    'version': 1,
    'formatters': {
        'simple': {
            'format': '[%(asctime)s][%(levelname)1.1s][%(name)s] %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'simple',
            'stream': 'ext://sys.stdout'
        }
    },
    'loggers': {
        'simpleExample': {
            'propagate': 'no'
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': [
            'console'
        ]
    }
}

LOGGING_INTERVAL = 100
