import json
import sqlite3
from typing import Union, Tuple, Optional, Any, Callable, Dict, Iterable, List

import sqlite3
import json
from collections.abc import MutableMapping, MutableSequence
from typing import Any, Optional

try:
    from .dict_index_helpers import AnyDict, AnyList
except ImportError:
    from dict_index_helpers import AnyDict, AnyList


class AnyIndex(MutableMapping):
    def __init__(self, name: str, db_path: str = ":memory:"):
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
                value JSON
            );
        """
        )
        self._connection.commit()

    def list_indexes(self):
        sql = f"SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='{self.name}';"
        result = self._connection.execute(sql).fetchall()
        return [row[0] for row in result]

    def search_by_value(self, keys_order, value):
        json_path = "$" + "".join([f".{key}" for key in keys_order])
        query = f"SELECT key, value FROM {self.name} WHERE json_extract(value, ?) = ?;"

        cursor = self._connection.cursor()
        cursor.execute(query, (json_path, value))

        # Return iterator over cursor that yields key and JSON-loaded value pairs
        return (
            (key, json.loads(json_value) if isinstance(json_value, str) else json_value)
            for key, json_value in cursor
        )

    def search_by_key(self, keys_order):
        json_path = "$" + "".join([f".{key}" for key in keys_order])
        query = f"""
            SELECT key, value
            FROM {self.name}
            WHERE json_extract(value, ?) IS NOT NULL;
        """

        cursor = self._connection.cursor()
        cursor.execute(query, (json_path,))

        # Return iterator over cursor that yields key and JSON-loaded value pairs
        return (
            (key, json.loads(json_value) if isinstance(json_value, str) else json_value)
            for key, json_value in cursor
        )

    def search_value_in_list(self, keys_order, value):
        json_path = "$" + "".join([f".{key}" for key in keys_order])

        query = f"""
            SELECT main.key, main.value
            FROM {self.name} AS main
            JOIN (
                SELECT key, json_extract(value, ?) as array_value
                FROM {self.name}
                WHERE json_type(json_extract(value, ?)) = 'array'
            ) AS derived
            ON main.key = derived.key
            WHERE EXISTS (
                SELECT 1
                FROM json_each(derived.array_value)
                WHERE json_each.value = ?
            );
        """

        cursor = self._connection.cursor()
        cursor.execute(query, (json_path, json_path, value))

        # Return iterator over cursor that yields key and JSON-loaded value pairs
        return ((key, json.loads(json_value)) for key, json_value in cursor)

    def create_index(self, keys_order):
        index_name = "_".join(keys_order) + "_index"
        json_path = "$" + "".join([f".{key}" for key in keys_order])
        query = f"CREATE INDEX IF NOT EXISTS {index_name} ON {self.name} (json_extract(value, ?));"
        self._connection.execute(query, (json_path,))
        self._connection.commit()

    def get_sorted_by_key(self, keys_order, reverse=False):
        json_path = "$" + "".join([f".{key}" for key in keys_order])
        order = "DESC" if reverse else "ASC"
        query = f"SELECT key, value FROM {self.name} ORDER BY json_extract(value, ?) {order};"
        cursor = self._connection.cursor()
        cursor.execute(query, (json_path,))
        return (
            (key, json.loads(json_value) if isinstance(json_value, str) else json_value)
            for key, json_value in cursor
        )

    def get_max_value(self, keys_order):
        json_path = "$" + "".join([f".{key}" for key in keys_order])
        query = f"SELECT MAX(json_extract(value, ?)) FROM {self.name};"
        cursor = self._connection.cursor()
        cursor.execute(query, (json_path,))
        return cursor.fetchone()[0]

    def get_min_value(self, keys_order):
        json_path = "$" + "".join([f".{key}" for key in keys_order])
        query = f"SELECT MIN(json_extract(value, ?)) FROM {self.name};"
        cursor = self._connection.cursor()
        cursor.execute(query, (json_path,))
        return cursor.fetchone()[0]

    def get_unique_values_and_counts(self, keys_order):
        json_path = "$" + "".join([f".{key}" for key in keys_order])
        query = f"SELECT json_extract(value, ?) as extracted_value, COUNT(*) as count FROM {self.name} GROUP BY extracted_value;"
        cursor = self._connection.cursor()
        cursor.execute(query, (json_path,))

        return {row[0]: row[1] for row in cursor}

    def close(self):
        if self._connection:
            self._connection.close()
            self._connection = None

    def __getitem__(self, key: str) -> Optional[Any]:
        value = self._get_nested_item(key, "$")

        if value is None:
            raise KeyError(key)

        if isinstance(value, dict):
            return AnyDict(self, key)
        elif isinstance(value, list):
            return AnyList(self, key)
        return value

    def _get_nested_item(self, key: str, path: str) -> Any:
        cursor = self._connection.cursor()
        cursor.execute(
            f"SELECT json_extract(value, ?) FROM {self.name} WHERE key=?", (path, key)
        )
        result = cursor.fetchone()
        if result:
            try:
                return json.loads(result[0])
            except:
                return result[0]
        return None

    def _set_nested_item(self, outer_key, path, value):
        cursor = self._connection.cursor()
        cursor.execute(
            f"UPDATE {self.name} SET value=json_set(value, ?, json(?)) WHERE key=?",
            (path, json.dumps(value), outer_key),
        )
        self._connection.commit()

    def _remove_nested_item(self, outer_key, path):
        cursor = self._connection.cursor()
        cursor.execute(
            f"UPDATE {self.name} SET value=json_remove(value, ?) WHERE key=?",
            (path, outer_key),
        )
        self._connection.commit()

    def _insert_nested_item(self, outer_key, path, value):
        cursor = self._connection.cursor()
        cursor.execute(
            f"UPDATE {self.name} SET value=json_insert(value, ?, json(?)) WHERE key=?",
            (path, json.dumps(value), outer_key),
        )
        self._connection.commit()

    def __setitem__(self, key: str, value: Any):
        cursor = self._connection.cursor()
        cursor.execute(
            f"INSERT OR REPLACE INTO {self.name}(key, value) VALUES (?, json(?))",
            (key, json.dumps(value)),
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
        cursor = self._connection.cursor()

        # Convert the items dictionary into a list of tuples
        # containing key-value pairs for use with executemany
        item_list = [(key, json.dumps(value)) for key, value in items.items()]

        # Execute the update statement with executemany
        cursor.executemany(
            f"""
            INSERT OR REPLACE INTO {self.name} (key, value)
            VALUES (?, json(?))
        """,
            item_list,
        )

        self._connection.commit()

    def find_keys_by_value(self, value: Union[float, int]) -> Iterable[str]:
        cursor = self._connection.cursor()
        cursor.execute(f"SELECT key FROM {self.name} WHERE value=?", (value,))
        return [row[0] for row in cursor.fetchall()]

    def get_top_n_items(self, n: int) -> Iterable[Tuple[str, Union[float, int]]]:
        cursor = self._connection.cursor()
        cursor.execute(
            f"SELECT key, value FROM {self.name} ORDER BY value DESC LIMIT ?", (n,)
        )
        return cursor.fetchall()

    def get_least_n_items(self, n: int) -> Iterable[Tuple[str, Union[float, int]]]:
        cursor = self._connection.cursor()
        cursor.execute(
            f"SELECT key, value FROM {self.name} ORDER BY value ASC LIMIT ?", (n,)
        )
        return cursor.fetchall()

    def get_sorted_items(
        self, reverse: bool = False
    ) -> Iterable[Tuple[str, Union[float, int]]]:
        order = "DESC" if reverse else "ASC"
        cursor = self._connection.cursor()
        cursor.execute(f"SELECT key, value FROM {self.name} ORDER BY value {order}")
        for row in cursor:
            yield row


if __name__ == "__main__":
    d = AnyIndex("test_db_anyddd", "test.db")

    # Test simple key-value pairs
    d["key1"] = "value1"
    assert d["key1"] == "value1"

    d["key2"] = 42
    assert d["key2"] == 42

    # Test a nested dictionary
    d["key3"] = {
        "nested_key1": "nested_value1",
        "nested_key2": {"nested_key3": "nested_value2"},
    }
    assert d["key3"]["nested_key1"] == "nested_value1"
    assert d["key3"]["nested_key2"]["nested_key3"] == "nested_value2"

    # Test updating a nested dictionary
    d["key3"]["nested_key2"]["nested_key3"] = "updated_nested_value2"
    assert d["key3"]["nested_key2"]["nested_key3"] == "updated_nested_value2"

    # Test a nested list
    d["key4"] = [1, 2, 3, ["nested_list_item1", "nested_list_item2"]]
    assert d["key4"][0] == 1
    assert d["key4"][3][1] == "nested_list_item2"

    # Test updating a nested list
    d["key4"][3][1] = "updated_nested_list_item2"
    assert d["key4"][3][1] == "updated_nested_list_item2"

    # Test deleting a key
    del d["key1"]
    try:
        _ = d["key1"]
    except KeyError:
        print("key1 successfully deleted")

    # Test iteration
    print("Iterating over keys:")
    for key in d:
        print(key)

    # Test length
    print(f"Length: {len(d)}")

    test_dict = {
        "wqDFOyquKhtMjfRtgYYcQJmXHkQpwcbAlJqLQqqUp": "g",
        "AGRzfpfhK": {
            "QnyPcxXWY": ["BqcMJuFH", 48],
            "ksfD": 39,
            "cenIanGoAZNBwEfzObaaSOagJaGoMSbAWCoZbJJHHbzbnOUOHS": [71, 49, "LA", 1],
        },
        "XqOPDKbnFZ": {"aJYdC": "Initial value"},
    }
    d["testAAA"] = test_dict

    assert d["testAAA"].get_object() == test_dict

    d["search_number_test_1"] = {"number": 1}
    d["search_number_test_2"] = {"number": 2}
    d["search_number_test_3_list"] = {"number": [3]}

    print("search results:", d.search_by_value(["number"], 2))

    # d.create_number_index(["number"])

    # print(d.list_indexes())

    print("All tests passed!")
