import sqlite3
from typing import Union, Tuple, Optional, Any, Callable, Dict, Iterable


class NumberIndex:
    def __init__(self, name: str, db_path: str):
        self.name = name
        self.db_path = db_path
        self._connection = sqlite3.connect(self.db_path, uri=True)
        self._initialize_db()

    def _initialize_db(self):
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=NORMAL")
        self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.name} (
                key TEXT PRIMARY KEY,
                value REAL
            );
        """
        )

        self._connection.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {self.name}_value_index ON {self.name}(value);
        """
        )

        self._connection.commit()

    def __getitem__(self, key: str) -> Union[float, int, None]:
        cursor = self._connection.cursor()
        cursor.execute(f"SELECT value FROM {self.name} WHERE key=?", (key,))
        result = cursor.fetchone()
        return result[0] if result else None

    def __setitem__(self, key: str, value: Union[float, int]):
        if not isinstance(value, (float, int)):
            raise ValueError("Value must be either float or int")

        cursor = self._connection.cursor()
        cursor.execute(
            f"""
            INSERT OR REPLACE INTO {self.name} (key, value)
            VALUES (?, ?)
        """,
            (key, value),
        )
        self._connection.commit()

    def __delitem__(self, key: str):
        cursor = self._connection.cursor()
        cursor.execute(f"DELETE FROM {self.name} WHERE key=?", (key,))
        self._connection.commit()

    def __contains__(self, key: str) -> bool:
        return self[key] is not None

    def __len__(self) -> int:
        cursor = self._connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {self.name}")
        return cursor.fetchone()[0]

    def __iter__(self) -> Iterable[str]:
        return self.keys()

    def keys(self) -> Iterable[str]:
        cursor = self._connection.cursor()
        cursor.execute(f"SELECT key FROM {self.name}")
        for row in cursor:
            yield row[0]

    def values(self) -> Iterable[Union[float, int]]:
        cursor = self._connection.cursor()
        cursor.execute(f"SELECT value FROM {self.name}")
        for row in cursor:
            yield row[0]

    def items(self) -> Iterable[Tuple[str, Union[float, int]]]:
        cursor = self._connection.cursor()
        cursor.execute(f"SELECT key, value FROM {self.name}")
        for row in cursor:
            yield row

    def pop(
        self, key: str, default: Optional[Union[float, int]] = None
    ) -> Union[float, int, None]:
        value = self[key]
        if value is None:
            return default
        else:
            del self[key]
            return value

    def popitem(self) -> Tuple[str, Union[float, int]]:
        cursor = self._connection.cursor()
        cursor.execute(f"SELECT key, value FROM {self.name} LIMIT 1")
        item = cursor.fetchone()
        if item is None:
            raise KeyError("popitem(): dictionary is empty")
        else:
            key, value = item
            del self[key]
            return key, value

    def clear(self):
        cursor = self._connection.cursor()
        cursor.execute(f"DELETE FROM {self.name}")
        self._connection.commit()

    def get(
        self, key: str, default: Optional[Union[float, int]] = None
    ) -> Union[float, int, None]:
        return self[key] if key in self else default

    def setdefault(
        self, key: str, default: Optional[Union[float, int]] = None
    ) -> Union[float, int, None]:
        if key in self:
            return self[key]
        else:
            self[key] = default
            return default

    def update(self, items: dict):
        if not all(isinstance(value, (float, int)) for value in items.values()):
            raise ValueError("All values must be either float or int")

        cursor = self._connection.cursor()
        cursor.executemany(
            f"""
            INSERT OR REPLACE INTO {self.name} (key, value)
            VALUES (?, ?)
        """,
            items.items(),
        )
        self._connection.commit()

    def get_keys_for_value(self, value: Union[float, int]) -> Iterable[str]:
        cursor = self._connection.cursor()
        cursor.execute(f"SELECT key FROM {self.name} WHERE value=?", (value,))
        return [row[0] for row in cursor.fetchall()]

    def top_n_items(self, n: int) -> Iterable[Tuple[str, Union[float, int]]]:
        cursor = self._connection.cursor()
        cursor.execute(
            f"SELECT key, value FROM {self.name} ORDER BY value DESC LIMIT ?", (n,)
        )
        return cursor.fetchall()

    def least_n_items(self, n: int) -> Iterable[Tuple[str, Union[float, int]]]:
        cursor = self._connection.cursor()
        cursor.execute(
            f"SELECT key, value FROM {self.name} ORDER BY value ASC LIMIT ?", (n,)
        )
        return cursor.fetchall()

    def sorted_items(
        self, reverse: bool = False
    ) -> Iterable[Tuple[str, Union[float, int]]]:
        order = "DESC" if reverse else "ASC"
        cursor = self._connection.cursor()
        cursor.execute(f"SELECT key, value FROM {self.name} ORDER BY value {order}")
        for row in cursor:
            yield row


if __name__ == "__main__":
    index = NumberIndex("my_index", "my_database.sqlite")
    # Setting values
    index["one"] = 1
    index["two"] = 2.0

    # Getting values
    print(index["one"])  # Output: 1
    print(index["two"])  # Output: 2.0

    # Deleting values
    del index["one"]

    index.update({"three": 3, "four": 4.0})

    # Checking if a key is in the index
    print("one" in index)  # Output: False
    print("two" in index)  # Output: True
    print("three" in index)  # Output: True

    for key in index:
        print(key)
