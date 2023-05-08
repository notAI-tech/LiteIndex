import sqlite3
import tempfile
import os
from typing import Optional, Dict, Iterator
from contextlib import contextmanager
from threading import Lock


class FileIndex:
    def __init__(self, name: str, db_path: str = ":memory:"):
        self.name = name
        self.db_path = db_path
        self._connection = sqlite3.connect(
            self.db_path, uri=True, check_same_thread=False
        )
        self._lock = Lock()
        self._create_table()

    @contextmanager
    def _locked_cursor(self):
        with self._lock:
            cursor = self._connection.cursor()
            try:
                yield cursor
                self._connection.commit()
            finally:
                cursor.close()

    def _create_table(self):
        with self._locked_cursor() as cursor:
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {self.name} (id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT, file_name TEXT, value BLOB)"
            )
            cursor.execute(
                f"CREATE INDEX IF NOT EXISTS idx_file_name ON {self.name} (file_name)"
            )

    def __setitem__(self, key: str, value: Dict[str, bytes]):
        with self._locked_cursor() as cursor:
            data = [
                (key, file_name, file_content)
                for file_name, file_content in value.items()
            ]
            cursor.executemany(
                f"INSERT OR REPLACE INTO {self.name} (key, file_name, value) VALUES (?, ?, ?)",
                data,
            )

    def __getitem__(self, key: str) -> Dict[str, bytes]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"SELECT file_name, value FROM {self.name} WHERE key=?", (key,)
            )
            results = cursor.fetchall()

        if not results:
            raise KeyError(f"Key '{key}' not found in {self.name}")

        return {file_name: file_data for file_name, file_data in results}

    def get_file_paths(self, key: str) -> Iterator[str]:
        with self._connection.cursor() as cursor:
            temp_dir = tempfile.mkdtemp()
            cursor.execute(
                f"SELECT file_name, value FROM {self.name} WHERE key=?", (key,)
            )
            for file_name, file_data in cursor:
                temp_path = os.path.join(temp_dir, file_name)
                with open(temp_path, "wb") as temp_file:
                    temp_file.write(file_data)
                yield temp_path

    def __delitem__(self, key: str):
        with self._locked_cursor() as cursor:
            cursor.execute(f"DELETE FROM {self.name} WHERE key=?", (key,))


if __name__ == "__main__":
    # Initialize the FileIndex
    file_index = FileIndex("my_files")

    # Add multiple files to the FileIndex
    file_key = "key_1"
    files = {
        "example1.txt": b"File 1 content.",
        "example2.txt": b"File 2 content.",
    }
    file_index[file_key] = files

    # Retrieve the files from the FileIndex
    retrieved_files = file_index[file_key]
    print(f"Retrieved files: {retrieved_files}")

    # Get the file paths of the files stored in memory
    file_paths_iterator = file_index.get_file_paths(file_key)
    for file_path in file_paths_iterator:
        print(f"File path in memory: {file_path}")
        with open(file_path, "rb") as f:
            file_data = f.read()
            print(f"File content from file path: {file_data}")
