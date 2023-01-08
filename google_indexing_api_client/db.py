from typing import TextIO, Generator, Optional, Dict

import datetime
import enum
import sqlite3

from collections import Counter
from pathlib import Path

from google_indexing_api_client.log import get_logger
from google_indexing_api_client.config.conf import LOGGING_INTERVAL


class URLStatus(enum.Enum):
    NEW = 'new'
    SENT_TO_INDEX = 'sent_to_index'


class DatabaseConnection:
    def __init__(self, db_filepath: Path):
        self._logger = get_logger(__name__)

        self.connection = sqlite3.connect(db_filepath.absolute().as_posix())
        self.write_cursor = self.connection.cursor()
        self.read_cursor = self.connection.cursor()

        self._urls_for_indexing: Optional[Generator[str, None, None]] = None

    def close_connection(self):
        assert self.write_cursor
        assert self.read_cursor
        assert self.connection
        self.write_cursor.close()
        self.read_cursor.close()
        self.connection.close()

    def init_db_from_file(self, filepath: Path):
        with filepath.open() as fp:
            self.write_cursor.executescript(fp.read())
            self.connection.commit()

    def upload_urls_from_io(self, fp: TextIO):
        log_info: Dict = Counter()
        now = datetime.datetime.now()
        for line in fp:
            log_info['num'] += 1
            line = line.strip()
            self._logger.debug('Processing url: %s', line)
            try:
                self.write_cursor.execute(
                    'INSERT INTO indexing_urls (url, status, created, updated)'
                    'VALUES(?, ?, ?, ?)',
                    (line, URLStatus.NEW.value, now, now)
                )
                self.connection.commit()
                log_info['added'] += 1
            except sqlite3.IntegrityError:
                self._logger.debug('URL %s is already in db', line)
                log_info['exists'] += 1

            if log_info['num'] % LOGGING_INTERVAL == 0:
                self._logger.info(
                    'Processed %(num)d urls. Added: %(added)d, skipped: %(exists)s',
                    log_info,
                )
        self._logger.info(
            'Total processed %(num)d urls. Added: %(added)d, skipped: %(exists)s',
            log_info,
        )

    def upload_urls_from_file(self, filepath: Path):
        with filepath.open() as fp:
            self.upload_urls_from_io(fp)

    def _not_indexed_urls_generator(self) -> Generator[str, None, None]:
        db_data = self.read_cursor.execute(
            'SELECT url FROM indexing_urls WHERE status == ?',
            (URLStatus.NEW.value,),
        )
        while True:
            data = db_data.fetchone()
            if data is None:
                break
            yield data[0]

    def update_url_status(self, url: str, new_status: URLStatus):
        now = datetime.datetime.now()
        self.write_cursor.execute(
            'UPDATE indexing_urls SET status = ?, updated = ? WHERE url = ?',
            (new_status.value, now, url),
        )
        self.connection.commit()

    @property
    def next_url_for_indexing(self) -> Optional[str]:
        if self._urls_for_indexing is None:
            self._urls_for_indexing = self._not_indexed_urls_generator()
        try:
            return self._urls_for_indexing.__next__()
        except StopIteration:
            return None


if __name__ == '__main__':
    from config.conf import DB_FILE_PATH
    db = DatabaseConnection(db_filepath=DB_FILE_PATH)

    print(id(db.read_cursor))
    print(id(db.write_cursor))

    # url = db.next_url_for_indexing

    # db.update_url_status(url, URLStatus.SENT_TO_INDEX)

    # db.upload_urls_from_file(_SRC_DIR / 'data_source/urls_batch.csv')
