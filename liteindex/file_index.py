import sqlite3
import tempfile
import os
from typing import Optional, Dict, Iterator

class FileIndex:
    def __init__(self, name: str, db_path: str = ":memory:"):
        self.name = name
        self.db_path = db_path
        self._connection = sqlite3.connect(self.db_path, uri=True)
        self._create_table()

    def _create_table(self):
        cursor = self._connection.cursor()
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {self.name} (id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT, file_name TEXT, value BLOB)"
        )
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_file_name ON {self.name} (file_name)")
        self._connection.commit()

    def __setitem__(self, key: str, value: Dict[str, bytes]):
        cursor = self._connection.cursor()

        for file_name, file_content in value.items():
            cursor.execute(
                f"INSERT OR REPLACE INTO {self.name} (key, file_name, value) VALUES (?, ?, ?)",
                (key, file_name, file_content),
            )

        self._connection.commit()

    def __getitem__(self, key: str) -> Dict[str, bytes]:
        cursor = self._connection.cursor()
        cursor.execute(f"SELECT file_name, value FROM {self.name} WHERE key=?", (key,))
        results = cursor.fetchall()

        if not results:
            raise KeyError(f"Key '{key}' not found in {self.name}")

        return {file_name: file_data for file_name, file_data in results}

    def get_file_paths(self, key: str) -> Iterator[str]:
        cursor = self._connection.cursor()
        cursor.execute(f"SELECT file_name, value FROM {self.name} WHERE key=?", (key,))
        results = cursor.fetchall()

        if not results:
            raise KeyError(f"Key '{key}' not found in {self.name}")

        temp_dir = tempfile.mkdtemp()

        for file_name, file_data in results:
            temp_path = os.path.join(temp_dir, file_name)
            with open(temp_path, "wb") as temp_file:
                temp_file.write(file_data)
            yield temp_path

    def __delitem__(self, key: str):
        cursor = self._connection.cursor()
        cursor.execute(f"DELETE FROM {self.name} WHERE key=?", (key,))
        self._connection.commit()

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

