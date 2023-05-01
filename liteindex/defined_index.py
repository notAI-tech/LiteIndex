import json
import sqlite3
from typing import Any, Union, List, Iterator, Optional, Dict
from collections.abc import MutableMapping


class CustomList(list):
    def __init__(
        self, parent: MutableMapping, parent_key: str, key: str, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.parent = parent
        self.parent_key = parent_key
        self.key = key

    def pop(self, index=-1):
        value = super().pop(index)
        self.parent._set_list(self.parent_key, self.key, self)
        return value

    def __repr__(self):
        return repr(list(self))

    def __getitem__(self, index):
        value = self.parent._get_list_item(self.parent_key, self.key, index)
        return super().__getitem__(index) if value is None else value

    def __setitem__(self, index, value):
        self.parent._set_list_item(self.parent_key, self.key, index, value)

    def append(self, value):
        self.parent._append_list_item(self.parent_key, self.key, value)

    def remove(self, value):
        self.parent._remove_list_item(self.parent_key, self.key, value)

    def extend(self, iterable):
        values = list(iterable)
        new_list = list(self) + values
        serialized_values = json.dumps(new_list)
        self.parent._connection.execute(
            f"UPDATE {self.parent.name} SET {self.key} = ? WHERE id = ?",
            (serialized_values, self.parent_key),
        )

    def insert(self, index, value):
        self.parent._connection.execute(
            f"UPDATE {self.parent.name} SET {self.key} = json_insert({self.key}, '$[{index}]', ?) WHERE id = ?",
            (value, self.parent_key),
        )
        super().insert(index, value)

    def clear(self):
        self.parent._connection.execute(
            f"UPDATE {self.parent.name} SET {self.key} = '[]' WHERE id = ?",
            (self.parent_key,),
        )
        super().clear()

    def sort(self, key=None, reverse=False):
        super().sort(key=key, reverse=reverse)
        self.parent._connection.execute(
            f"UPDATE {self.parent.name} SET {self.key} = ? WHERE id = ?",
            (json.dumps(self), self.parent_key),
        )

    def reverse(self):
        super().reverse()
        self.parent._connection.execute(
            f"UPDATE {self.parent.name} SET {self.key} = ? WHERE id = ?",
            (json.dumps(self), self.parent_key),
        )


class CustomDict(dict):
    def __init__(self, parent: MutableMapping, key: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parent = parent
        self.key = key

    def __getitem__(self, k):
        value = super().__getitem__(k)
        if isinstance(value, list):
            return CustomList(self.parent, self.key, k, value)
        return value

    def __setitem__(self, k, v):
        if isinstance(v, list):
            v = CustomList(self.parent, self.key, k, v)
        super().__setitem__(k, v)
        self.parent._update_item(self.key, k, v)

    def __custom_repr(self, value):
        if isinstance(value, CustomList):
            return repr(list(value))
        return repr(value)

    def pop(self, k, *args):
        value = super().pop(k, *args)
        if k in self.parent.schema:
            self.parent.__delitem__(self.key)
        return value

    def popitem(self):
        key, value = super().popitem()
        if key in self.parent.schema:
            self.parent.__delitem__(self.key)
        return key, value

    def keys(self):
        return iter(super().keys())

    def values(self):
        return (self[key] for key in self)

    def items(self):
        return ((key, self[key]) for key in self)

    def __repr__(self):
        return (
            "{"
            + ", ".join(f"{repr(k)}: {self.__custom_repr(self[k])}" for k in self)
            + "}"
        )


class DefinedIndex(MutableMapping):
    def __init__(
        self, name: str, schema: Optional[dict] = None, db_path: str = ":memory:"
    ):
        self.name = name
        self.db_path = db_path
        self._connection = sqlite3.connect(self.db_path, uri=True)
        self._connection.row_factory = sqlite3.Row
        if schema is None:
            self.schema = self._load_schema_from_table()
        else:
            self.schema = schema
            self._validate_schema()
        self._initialize_db()

    def _load_schema_from_table(self) -> dict:
        cursor = self._connection.cursor()
        cursor.execute(f"PRAGMA table_info({self.name})")
        columns = cursor.fetchall()

        if not columns:
            raise ValueError(
                f"Table {self.name} does not exist, and no schema was provided."
            )

        schema = {}
        for col in columns:
            if col["name"] == "id":
                continue
            if col["type"] == "NUMBER":
                schema[col["name"]] = 0
            elif col["type"] == "JSON":
                schema[col["name"]] = []
            else:
                schema[col["name"]] = ""
        return schema

    def _initialize_db(self):
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=NORMAL")
        self._create_table()

    def _deserialize_value(self, value: Any, example: Any) -> Any:
        if isinstance(example, (list, tuple)):
            return CustomList("", "", json.loads(value))
        else:
            return value

    def _validate_schema(self):
        allowed_types = (str, int, float, list)
        for key, value in self.schema.items():
            if not isinstance(key, allowed_types):
                raise ValueError(
                    f"Invalid schema key: {key}. Keys must be strings or numbers."
                )
            if not isinstance(value, allowed_types):
                raise ValueError(
                    f"Invalid schema value for key {key}: {value}. Values must be strings, numbers, or lists."
                )
            if isinstance(value, list):
                if not all(isinstance(item, allowed_types) for item in value):
                    raise ValueError(
                        f"Invalid schema value for key {key}: {value}. List items must be strings or numbers."
                    )
            
            # if isinstance(value, dict):
            #     if not all(isinstance(item, allowed_types) for item in value.keys()) and not all(isinstance(item, allowed_types) for item in value.values()):
            #         raise ValueError(
            #             f"Invalid schema value for key {key}: {value}. List items must be strings or numbers."
            #         )

    def _create_table(self):
        columns = []
        for key, value in self.schema.items():
            column_type = self._get_column_type(value)
            columns.append(f"{key} {column_type}")
        columns_str = ", ".join(columns)
        self._connection.execute(
            f"CREATE TABLE IF NOT EXISTS {self.name} (id TEXT PRIMARY KEY, {columns_str})"
        )
        self._connection.commit()

    def _get_column_type(self, value: Any) -> str:
        if isinstance(value, (int, float)):
            return "NUMBER"
        elif isinstance(value, (list, tuple)): #, dict)):
            return "JSON"
        else:
            return "TEXT"

    def _serialize_value(self, value: Any) -> Union[str, float, int]:
        if isinstance(value, (list, tuple)): #, dict)):
            return json.dumps(value)
        else:
            return value

    def _deserialize_value(self, value: Any, example: Any) -> Any:
        if isinstance(example, (list, tuple)): #, dict)):
            return json.loads(value)
        else:
            return value

    def __getitem__(self, key: str) -> dict:
        result = self._connection.execute(
            f"SELECT * FROM {self.name} WHERE id = ?", (key,)
        ).fetchone()
        if result is None:
            raise KeyError(f"Key {key} not found")
        _, *values = result
        data = {
            k: self._deserialize_value(value, self.schema[k])
            for k, value in zip(self.schema.keys(), values)
            if value is not None
        }
        return CustomDict(self, key, data)

    def __setitem__(self, key: str, data: dict):
        # Insert the row with the specified key if it doesn't exist
        with self._connection:
            self._connection.execute(
                f"INSERT OR IGNORE INTO {self.name} (id) VALUES (?)", (key,)
            )

        # Update only the supplied keys
        for k, v in data.items():
            self._update_item(key, k, v)

    def _update_item(self, main_key: str, sub_key: str, value: Any):
        serialized_value = self._serialize_value(value)
        with self._connection:
            self._connection.execute(
                f"UPDATE {self.name} SET {sub_key} = ? WHERE id = ?",
                (serialized_value, main_key),
            )

    def __delitem__(self, key: str):
        with self._connection:
            self._connection.execute(f"DELETE FROM {self.name} WHERE id = ?", (key,))

    def __contains__(self, key: str) -> bool:
        return (
            self._connection.execute(
                f"SELECT 1 FROM {self.name} WHERE id = ?", (key,)
            ).fetchone()
            is not None
        )

    def __len__(self) -> int:
        return self._connection.execute(f"SELECT COUNT(*) FROM {self.name}").fetchone()[
            0
        ]

    def __iter__(self) -> Iterator[str]:
        cursor = self._connection.execute(f"SELECT id FROM {self.name}")
        return (row[0] for row in cursor)

    def _get_list_item(self, row_key: str, column_key: str, index: int):
        result = self._connection.execute(
            f"SELECT json_extract({column_key}, '$[{index}]') FROM {self.name} WHERE id = ?",
            (row_key,),
        ).fetchone()
        return result[0] if result and result[0] is not None else None

    def _set_list_item(self, row_key: str, column_key: str, index: int, value: Any):
        serialized_value = value
        self._connection.execute(
            f"UPDATE {self.name} SET {column_key} = json_set({column_key}, '$[{index}]', ?) WHERE id = ?",
            (serialized_value, row_key),
        )

    def _append_list_item(self, row_key: str, column_key: str, value: Any):
        serialized_value = value
        self._connection.execute(
            f"UPDATE {self.name} SET {column_key} = json_insert({column_key}, '$[{len(self[row_key][column_key])}]', ?) WHERE id = ?",
            (serialized_value, row_key),
        )

    def _remove_list_item(self, row_key: str, column_key: str, value: Any):
        with self._connection:
            self._connection.execute(
                f"UPDATE {self.name} SET {column_key} = json_remove({column_key}, json_array_length({column_key}) - json_array_length(json_remove({column_key}, json_array_length({column_key}) - json_array_length(json_remove({column_key}, json_array_length({column_key}), ?))))) WHERE id = ?",
                (serialized_value, row_key),
            )

    def _prepare_update_data(self, key: str, data: dict) -> tuple:
        columns = list(self.schema.keys())
        values = [self._serialize_value(data.get(key, None)) for key in columns]
        return (key, *values)

    def update(self, items: dict):
        if not items:
            return

        data = [self._prepare_update_data(key, value) for key, value in items.items()]
        columns = list(self.schema.keys())
        placeholders = ", ".join(["?"] * len(columns))

        update_columns = ", ".join([f"{column} = ?" for column in columns])

        with self._connection:
            self._connection.executemany(
                f"INSERT OR REPLACE INTO {self.name} (id, {', '.join(columns)}) VALUES (?, {placeholders})",
                data,
            )
            self._connection.commit()

    def keys(self):
        return iter(self)

    def values(self):
        cursor = self._connection.execute(f"SELECT id, * FROM {self.name}")
        for row in cursor:
            key = row[0]
            _, *values = row
            data = {
                k: self._deserialize_value(value, self.schema[k])
                for k, value in zip(self.schema.keys(), values)
            }
            yield CustomDict(data, self, key)

    def items(self):
        cursor = self._connection.execute(f"SELECT id, * FROM {self.name}")
        for row in cursor:
            key = row[0]
            _, *values = row
            data = {
                k: self._deserialize_value(value, self.schema[k])
                for k, value in zip(self.schema.keys(), values)
            }
            yield key, CustomDict(data, self, key)

    def __str__(self):
        return f"DefinedIndex with {len(self)} items"

    def _row_to_dict(self, row: sqlite3.Row) -> Dict[str, CustomDict[str, Any]]:
        result = (
            row["id"],
            {
                key: self._deserialize_value(row[key], self.schema[key])
                for key in self.schema
            },
        )
        return result

    def filter_by_values_at_keys(
        self, filters: Dict[str, Any]
    ) -> Iterator[Dict[str, Any]]:
        # Initialize a cursor for the database connection
        cursor = self._connection.cursor()

        # Generate the SQL query string based on the filters dictionary
        conditions = " AND ".join([f"{key} = ?" for key in filters.keys()])
        query = f"SELECT id, * FROM {self.name} WHERE {conditions}"

        # Execute the query with the provided filter values
        cursor.execute(query, tuple(filters.values()))

        # Return an iterator that converts each row to a dictionary
        return (self._row_to_dict(row) for row in cursor)

    def filter_by_values_in_list_at_keys(
        self, filters: Dict[str, List[Any]]
    ) -> Iterator[Dict[str, Any]]:
        # Initialize a cursor for the database connection
        cursor = self._connection.cursor()

        # Generate the SQL query string based on the filters dictionary
        conditions = " AND ".join(
            [f"json_array_contains(json(?), {key})" for key in filters.keys()]
        )
        query = f"SELECT id, * FROM {self.name} WHERE {conditions}"

        # Prepare the filter values by converting the lists to JSON strings
        filter_values = [json.dumps(value) for value in filters.values()]

        # Execute the query with the provided filter values
        cursor.execute(query, tuple(filter_values))

        # Return an iterator that converts each row to a dictionary
        return (self._row_to_dict(row) for row in cursor)

    def sort_by_key(self, key: str, ascending: bool = True) -> Iterator[Dict[str, Any]]:
        order = "ASC" if ascending else "DESC"
        cursor = self._connection.cursor()
        cursor.execute(f"SELECT id, * FROM {self.name} ORDER BY {key} {order}")
        return (self._row_to_dict(row) for row in cursor)

    def get_distinct_values_at_key(self, key: str) -> Iterator[Any]:
        cursor = self._connection.cursor()
        cursor.execute(f"SELECT DISTINCT {key} FROM {self.name}")
        return (row[0] for row in cursor)

    def count_by_value_at_key(self, key: str, value: Any) -> int:
        cursor = self._connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {self.name} WHERE {key} = ?", (value,))
        return cursor.fetchone()[0]

    def count_distinct_values_at_key(self, key: str) -> int:
        cursor = self._connection.cursor()
        cursor.execute(f"SELECT COUNT(DISTINCT {key}) FROM {self.name}")
        return cursor.fetchone()[0]

    def group_by_key(
        self, key: str, aggregate_function: str, target_key: str
    ) -> Iterator[Dict[str, Any]]:
        cursor = self._connection.cursor()
        cursor.execute(
            f"SELECT {key}, {aggregate_function}({target_key}) as aggregate FROM {self.name} GROUP BY {key}"
        )
        return (dict(zip(row.keys(), row)) for row in cursor)

    def filter_by_list_length_at_key(
        self, key: str, length: int, condition: str = "="
    ) -> Iterator[Dict[str, Any]]:
        if condition not in ["=", "<", ">", "<=", ">="]:
            raise ValueError(
                "Invalid condition. Allowed conditions: '=', '<', '>', '<=', '>='."
            )
        cursor = self._connection.cursor()
        cursor.execute(
            f"SELECT id, * FROM {self.name} WHERE json_array_length({key}) {condition} ?",
            (length,),
        )
        return (self._row_to_dict(row) for row in cursor)

    def sort_by_list_length_at_key(
        self, key: str, ascending: bool = True
    ) -> Iterator[Dict[str, Any]]:
        order = "ASC" if ascending else "DESC"
        cursor = self._connection.cursor()
        cursor.execute(
            f"SELECT id, * FROM {self.name} ORDER BY json_array_length({key}) {order}"
        )
        return (self._row_to_dict(row) for row in cursor)

    def count_value_in_list_at_key(self, key: str, value: Any) -> int:
        cursor = self._connection.cursor()
        cursor.execute(
            f"SELECT COUNT(*) FROM {self.name} WHERE json_array_contains(json(?), {key})",
            (json.dumps(value),),
        )
        return cursor.fetchone()[0]

    def get_distinct_values_in_list_at_key(self, key: str) -> Iterator[Any]:
        cursor = self._connection.cursor()
        cursor.execute(
            f"SELECT DISTINCT json_each.value FROM {self.name}, json_each({key})"
        )
        return (row[0] for row in cursor)

    def aggregate_list_values_at_key(self, key: str, aggregate_function: str) -> Any:
        if aggregate_function not in ["sum", "avg", "min", "max"]:
            raise ValueError(
                "Invalid aggregate function. Allowed functions: 'sum', 'avg', 'min', 'max'."
            )
        cursor = self._connection.cursor()
        cursor.execute(
            f"SELECT {aggregate_function}(value) FROM {self.name}, json_each({key})"
        )
        return cursor.fetchone()[0]


if __name__ == "__main__":
    # Assuming we have the DefinedIndex class implementation
    index = DefinedIndex(
        "people",
        schema={
            "name": "",
            "age": 0,
            "interests": ["reading", "traveling"]
        },
        db_path=":memory:",
    )

    # Add an item to the index
    index["k1"] = {
        "name": "Alice",
        "age": 30,
        "interests": ["reading", "traveling"]
    }
    # index now contains: {"k1": {"name": "Alice", "age": 30, "address": {"city": "New York", "country": "USA"}, "interests": ["reading", "traveling"]}}


    # Update the age directly
    index["k1"]["age"] = 31
    # index now contains: {"k1": {"name": "Alice", "age": 31, "address": {"city": "New York", "country": "USA"}, "interests": ["reading", "traveling"]}}


    # Append a new interest to the list directly
    index["k1"]["interests"].append("cooking")
    # index now contains: {"k1": {"name": "Alice", "age": 31, "address": {"city": "New York", "country": "USA"}, "interests": ["reading", "traveling", "cooking"]}}

    print(index["k1"]["interests"])


