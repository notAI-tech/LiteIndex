import sqlite3
import pickle
import json
import datetime


class AnyIndex:
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
            CREATE TABLE IF NOT EXISTS {self.name}_json (
                key TEXT NOT NULL PRIMARY KEY,
                json_data JSON
            )
        """
        )
        self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.name}_blob (
                key TEXT NOT NULL,
                blob_key_path TEXT NOT NULL,
                blob_data BLOB,
                PRIMARY KEY (key, blob_key_path)
            )
        """
        )
        self._connection.commit()

    def _store(self, key, json_data, blob_data):
        self._connection.execute(
            f"""
            INSERT OR REPLACE INTO {self.name}_json (key, json_data)
            VALUES (?, json(?))
        """,
            (key, json_data),
        )
        self._connection.commit()

        self._connection.execute(f"DELETE FROM {self.name}_blob WHERE key = ?", (key,))
        for blob_key_path, blob_value in blob_data:
            self._connection.execute(
                f"""
                INSERT INTO {self.name}_blob (key, blob_key_path, blob_data)
                VALUES (?, ?, ?)
            """,
                (key, blob_key_path, sqlite3.Binary(blob_value)),
            )
        self._connection.commit()

    def _traverse(self, key, obj, key_path=()):
        json_data = {}
        blob_data = []

        if isinstance(obj, (list, dict)):
            # Store the object type information
            obj_type = "list" if isinstance(obj, list) else "dict"
            json_data[f"_type_{key_path}"] = obj_type

            if isinstance(obj, list):
                it = enumerate(obj)
            else:
                it = obj.items()

            for sub_key, sub_value in it:
                new_key_path = key_path + (sub_key,)
                sub_json_data, sub_blob_data = self._traverse(
                    key, sub_value, new_key_path
                )
                json_data.update(sub_json_data)
                blob_data.extend(sub_blob_data)
        else:
            if isinstance(obj, (int, float, bool, str, type(None))):
                json_data[key_path] = obj
            elif isinstance(obj, datetime.datetime):
                json_data[key_path] = obj.isoformat()
            else:
                pickled_value = pickle.dumps(obj)
                blob_data.append((key_path, pickled_value))

        return json_data, blob_data

    def _unflatten(self, flat_data):
        result = {}

        for key_path_tuple, value in flat_data.items():
            # Skip type information keys
            if key_path_tuple and key_path_tuple[0] == "_type_":
                continue

            key_path = ".".join(str(k) for k in key_path_tuple)
            keys = key_path.split(".")
            current = result

            for key in keys[:-1]:
                key = int(key) if key.isdigit() else key
                if key not in current:
                    # If type information is available, use it to determine whether to create a list or a dictionary
                    obj_type_key = ("_type_",) + tuple(str(k) for k in keys[:-1])
                    if obj_type_key in flat_data and flat_data[obj_type_key] == "list":
                        current[key] = []
                    else:
                        current[key] = {}
                current = current[key]

            last_key = keys[-1]
            last_key = int(last_key) if last_key.isdigit() else last_key
            current[last_key] = value

        return result

    def _get_value(self, key):
        cursor = self._connection.execute(
            f"SELECT json_data FROM {self.name}_json WHERE key = ?", (key,)
        )
        row = cursor.fetchone()
        if row is not None:
            json_data = row[0]
            flat_json_value = json.loads(json_data)
            flat_json_value = {tuple(eval(k)): v for k, v in flat_json_value.items()}

            cursor = self._connection.execute(
                f"SELECT blob_key_path, blob_data FROM {self.name}_blob WHERE key = ?",
                (key,),
            )
            for row in cursor:
                blob_key_path, blob_data = row
                blob_value = pickle.loads(blob_data)
                flat_json_value[eval(blob_key_path)] = blob_value

            return self._unflatten(flat_json_value)
        return None

    def __getitem__(self, key):
        return self._get_value(key)

    def __setitem__(self, key, value):
        json_data, blob_data = self._traverse(key, value)
        json_data = {str(k): v for k, v in json_data.items()}
        blob_data = [(str(k), v) for k, v in blob_data]
        json_str = json.dumps(json_data)
        self._store(key, json_str, blob_data)

    def __delitem__(self, key):
        self._delete(key)

    def _delete(self, key):
        self._connection.execute(f"DELETE FROM {self.name}_json WHERE key = ?", (key,))
        self._connection.execute(f"DELETE FROM {self.name}_blob WHERE key = ?", (key,))
        self._connection.commit()

    def __iter__(self):
        cursor = self._connection.execute(f"SELECT key FROM {self.name}_json")
        return (row[0] for row in cursor)

    def __len__(self):
        cursor = self._connection.execute(f"SELECT COUNT(*) FROM {self.name}_json")
        return cursor.fetchone()[0]

    def keys(self):
        return iter(self)

    def values(self):
        for key in self.keys():
            yield self[key]

    def items(self):
        for key in self.keys():
            yield (key, self[key])

    def update(self, other):
        json_data = {}
        blob_data = {}

        if isinstance(other, (dict, MutableMapping)):
            for key, value in other.items():
                sub_json_data, sub_blob_data = self._traverse(repr(key), value)
                json_data.update(sub_json_data)
                blob_data.update({key: value for key, value in sub_blob_data})
        else:
            for key, value in other:
                sub_json_data, sub_blob_data = self._traverse(repr(key), value)
                json_data.update(sub_json_data)
                blob_data.update({key: value for key, value in sub_blob_data})

        # Combine JSON data and blob data into a single list of tuples
        combined_data = [
            (key, json.dumps(json_data.get(key)), blob_data.get(key))
            for key in set(json_data) | set(blob_data)
        ]

        # Store combined data in a single call
        self._connection.executemany(
            f"""
            INSERT OR REPLACE INTO {self.name} (key_path, json_data, blob)
            VALUES (?, json(?), ?)
        """,
            combined_data,
        )
        self._connection.commit()

    def clear(self):
        self._connection.execute(f"DELETE FROM {self.name}")
        self._connection.commit()

    def pop(self, key, default=None):
        try:
            value = self[key]
            del self[key]
            return value
        except KeyError:
            if default is not None:
                return default
            else:
                raise KeyError(key)

    def popitem(self):
        try:
            key = next(iter(self))
            value = self[key]
            del self[key]
            return (key, value)
        except StopIteration:
            raise KeyError("popitem(): dictionary is empty")

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def setdefault(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return default

    def copy(self):
        return self


if __name__ == "__main__":
    import numpy as np

    any_index = AnyIndex(name="test")
    any_index["a"] = 1
    any_index["b"] = 2.0
    any_index["c"] = True
    any_index["d"] = datetime.datetime.now()
    any_index["e"] = "Hello"
    any_index["f"] = b"World"
    any_index["g"] = {"a": 1, "b": 2}
    any_index["h"] = [1, 2, 3]
    any_index["i"] = {"a": [1, 2, 3], "b": {"c": 4, "d": 5}}
    any_index["j"] = [1, None, 2, {"a": 3, "b": 4}]
    any_index["k"] = {"a": 1, "b": [2, 3, {"c": 4, "d": 5}]}
    any_index["l"] = {"a": 1, "b": [2, 3, {"c": 4, "d": 5, "e": {"f": 6, "g": 7}}]}
    any_index["m"] = {
        "a": 1,
        "b": [2, 3, {"c": 4, "d": 5, "e": {"f": 6, "g": 7, "h": [8, 9, 10]}}],
    }
    any_index["n"] = np.array([1, 2, 3])

    print(any_index["a"])
    print(any_index["b"])
    print(any_index["c"])
    print(any_index["d"])
    print(any_index["e"])
    print(any_index["f"])
    print(any_index["g"])
    print(any_index["h"])
    print(any_index["i"])
    print("--", any_index["j"])
    print(any_index["k"])
    print(any_index["l"])
    print(any_index["m"])
    print(any_index["n"])
