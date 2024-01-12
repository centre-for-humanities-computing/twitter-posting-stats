import gzip
import glob
from datetime import datetime
from sqlite3 import Connection
from typing import Iterator


class Inserter:

    def __init__(self, connection: Connection, table: str, buffer_size=200_000,
                 skip=None):
        if skip is None:
            skip = set()
        self._connection = connection
        self._table = table
        column_info = connection.execute(f"PRAGMA table_info({table})").fetchall()
        self.columns = [c[1] for c in column_info if not c[1] in skip]
        column_names = ', '.join(self.columns)
        placeholders = ':' + ', :'.join(self.columns)
        self._query = (f'INSERT OR REPLACE INTO {self._table}'
                       f'({column_names}) VALUES ({placeholders})')
        self._items = []
        self._buffer_size = buffer_size

    def insert(self, item: dict):
        self._items.append(item)
        if len(self._items) >= self._buffer_size:
            self._insert_many()

    def _insert_many(self):
        self._connection.executemany(self._query, self._items)
        self._connection.commit()
        self._items.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._insert_many()


def iter_lines(fp: str) -> Iterator[str]:
    files = glob.glob(fp)
    for file in files:
        with gzip.open(file) if file.endswith('gz') else open(file) as f:
            for line in f:
                yield line


def iso_to_posix(iso_time: str) -> int:
    return int(datetime.fromisoformat(iso_time).timestamp())
