from typing import TextIO, Optional, Dict

import argparse
import enum
import logging

from collections import Counter

from glendale.config.conf import (
    DB_FILE_PATH,
    INIT_DB_SQL_PATH,
    LOGGING_INTERVAL,
    WEB as WEB_CONFIG,
)
from glendale.db import DatabaseConnection, URLStatus
from glendale.exceptions import AuthenticationDataIsOutError
from glendale.log import get_logger, setup_logger
from glendale.web import GoogleAPIClient


class ClientAction(enum.Enum):
    INIT = 'init'
    ADD_URLS = 'add_urls'
    INDEXING = 'indexing'
    STATS = 'stats'


def create_parser():
    parser = argparse.ArgumentParser(
        prog='Google Indexing API Client',
        description='Lightweight client for managing indexing URLS with Google Indexing API',
        epilog='Created by a.kreskin. E-mail: leshakreskin@gmail.com. Russia, Moscow, 2023',
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Add more log information',
    )

    subparsers = parser.add_subparsers(
        title='Action',
        description='Action of client to do',
        dest='action',
        required=True,
    )

    subparsers.add_parser(
        'init',
        help='Initialize database for client. Required action for the first start. '
             'SQLite database file path specified in config/conf.py:DB_FILE_PATH. '
             'SQL for initialization stored in file specified in config/conf.py:INIT_DB_SQL_PATH. '
             'WARNING! Running of this script will drop your current database. Use it carefully',
    )

    parser_add_urls = subparsers.add_parser(
        'add_urls',
        help='Add urls for indexing from file'
    )
    parser_add_urls.add_argument(
        '-f', '--filepath',
        required=True,
        type=argparse.FileType(),
        help='Path to file with urls to indexing',
    )

    subparsers.add_parser(
        'indexing',
        help='Run indexing process for every url from database, '
             'which is still not sent to indexing. API keys will be taken from directory '
             'specified in config/conf.py:API_KEYS_DIR',
    )

    parser_stats = subparsers.add_parser(
        'stats',
        help='Show statistic for current state of system',
    )
    parser_stats.add_argument(
        '-d', '--detailed',
        action='store_true',
        help='Show detailed statistic for every line in database. '
             'It is recommended to save to a file',
    )
    parser_stats.add_argument(
        '-o', '--output',
        default=None,
        type=argparse.FileType('w'),
        help='Output file for statistic',
    )

    return parser


class MainApplication:
    def __init__(self):
        self._logger: Optional[logging.Logger] = None

    def setup_logger(self, namespace: argparse.Namespace) -> None:
        log_level = None
        if getattr(namespace, 'verbose') is True:
            log_level = 'DEBUG'
        setup_logger(log_level)
        self._logger = get_logger(__name__)

    @property
    def logger(self) -> logging.Logger:
        assert self._logger, 'Logger is not set'
        return self._logger

    def run(self):
        parser = create_parser()
        namespace = parser.parse_args()

        self.setup_logger(namespace)

        action = ClientAction(namespace.action)

        if action is ClientAction.INIT:
            self.init_db()
        elif action is ClientAction.ADD_URLS:
            self.add_urls(namespace.filepath)
        elif action is ClientAction.INDEXING:
            self.index_urls()
        elif action is ClientAction.STATS:
            self.process_statistic(
                detailed=namespace.detailed,
                output=namespace.output,
            )

    def init_db(self) -> None:
        self.logger.info('Start to setting up database')
        self.logger.debug('Database file: %s', DB_FILE_PATH)
        self.logger.debug('Database SQL file: %s', INIT_DB_SQL_PATH)
        db = DatabaseConnection(DB_FILE_PATH)
        db.init_db_from_file(INIT_DB_SQL_PATH)
        db.close_connection()
        self.logger.info('Database initialization finished')

    def add_urls(self, fp: TextIO) -> None:
        self.logger.info('Processing started')
        db = DatabaseConnection(DB_FILE_PATH)
        db.upload_urls_from_io(fp)
        db.close_connection()
        self.logger.info('Processing finished')

    def index_urls(self) -> None:
        log_info: Dict = Counter()

        db = DatabaseConnection(DB_FILE_PATH)
        client = GoogleAPIClient(WEB_CONFIG)

        repeat_current_url: bool = False
        url: Optional[str] = None
        while True:
            if not repeat_current_url:
                url = db.next_url_for_indexing
            else:
                repeat_current_url = False

            if url is None:
                self.logger.info('URLs list is out')
                break
            self.logger.debug('Indexing url: %s', url)

            resp = client.index_request(url)
            self.logger.debug('Response info: %r', resp.json())
            if not log_info['api_keys_used']:
                log_info['api_keys_used'] += 1

            if resp.status_code != 200:
                if resp.status_code == 429:
                    self.logger.warning('Current API key reach rate limit. Try to switch')
                else:
                    self.logger.error(
                        'Error in request: status %s, reason: %s, json: %r',
                        resp.status_code,
                        resp.reason,
                        resp.json(),
                    )
                    self.logger.info('Try to switch probably corrupted session')
                try:
                    client.create_new_session()
                    log_info['api_keys_used'] += 1
                except AuthenticationDataIsOutError:
                    self.logger.info('Out of AuthenticationData. Terminating')
                    break
                repeat_current_url = True
                continue
            db.update_url_status(url, URLStatus.SENT_TO_INDEX)
            log_info['processed_urls'] += 1

            if log_info['processed_urls'] % LOGGING_INTERVAL == 0:
                self.logger.info(
                    'Processed %(processed_urls)s, keys_used %(api_keys_used)s', log_info,
                )
        self.logger.info('Process finished. Stats: %r', log_info)

        client.close()
        db.close_connection()

    def process_statistic(self, detailed: bool, output: Optional[TextIO] = None) -> None:
        pass
