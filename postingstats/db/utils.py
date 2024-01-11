import gzip
import sqlite3
import glob
from datetime import datetime
from queue import Queue
from sqlite3 import Connection
from threading import Thread
from typing import List, Iterator


class Task:
    def __init__(self, query: str, items: List[dict], conn: Connection):
        self.query = query
        self.items = items
        self.conn = conn

    def execute(self):
        self.conn.executemany(self.query, self.items)
        self.conn.commit()


class InsertionContext:
    STOP_SIGNAL = object()

    def __init__(self, db_name: str):
        self.db_name = db_name
        self.task_queue = Queue(maxsize=20)
        self.inserter_thread = Thread(target=self._db_worker, daemon=True)

    def add_task(self, task: Task):
        self.task_queue.put(task)

    def _db_worker(self):
        with sqlite3.connect(self.db_name) as conn:
            while True:
                task = self.task_queue.get()
                if task is InsertionContext.STOP_SIGNAL:
                    break
                try:
                    task.conn = conn
                    task.execute()
                except Exception as e:
                    print("Error:", e)
                finally:
                    self.task_queue.task_done()

    def __enter__(self):
        self.inserter_thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.task_queue.put(InsertionContext.STOP_SIGNAL)
        self.task_queue.join()
        self.inserter_thread.join()


class Inserter:

    def __init__(self, connection: Connection, table: str, buffer_size=250_000,
                 skip=None, insertion_context: InsertionContext = None):
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
        self._insertion_context = insertion_context

    def _column_values(self, item: dict):
        return {}

    def insert(self, item: dict):
        self._items.append(item)
        if len(self._items) >= self._buffer_size:
            self._insert_many()

    def _insert_many(self):
        if self._insertion_context:
            self._insertion_context.add_task(
                Task(self._query, self._items, self._connection)
            )
            self._items = []
        else:
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
