import re
import json
import sqlite3
import random
from .query_parser import search_query, distinct_query, count_query, delete_query


class DefinedIndex:
    def __init__(self, name, schema=None, db_path=":memory:", auto_key=False):
        self.name = name
        self.db_path = db_path
        self.auto_key = auto_key
        self._connection = sqlite3.connect(self.db_path, uri=True)
        self._connection.row_factory = sqlite3.Row
        if schema is None:
            self.schema = self._load_schema_from_table()
        else:
            self.schema = schema
            self._validate_schema()
        self.column_type_map = {
            key: self._get_column_type(value) for key, value in self.schema.items()
        }

        self._initialize_db()

    def _initialize_db(self):
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=NORMAL")
        self._create_table()

    def _load_schema_from_table(self):
        cursor = self._connection.cursor()
        cursor.execute(
            f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{self.name}'"
        )
        result = cursor.fetchone()
        if result is None:
            raise ValueError(f"Table {self.name} does not exist in the database")
        sql = result["sql"]
        column_defs = re.findall(r"\((.*)\)", sql)[0].split(",")
        schema = {}

        for column_def in column_defs:
            column_name, column_type, *rest = column_def.split()
            if column_name == "id":
                self.auto_key = "INTEGER" in column_type
            schema[column_name] = self._get_value_from_column_type(column_type)
        return schema

    def _get_value_from_column_type(self, column_type):
        if column_type.startswith("TEXT"):
            return ""
        elif column_type == "NUMBER":
            return 0
        elif column_type == "JSON":
            return {}
        elif column_type == "INTEGER":
            return False
        else:
            raise ValueError(f"Unsupported column type: {column_type}")

    def _validate_schema(self):
        for key, value in self.schema.items():
            if key == "id":
                raise ValueError("The schema should not include an 'id' key.")

            if not isinstance(key, str):
                raise ValueError(f"Invalid schema key: {key}. Keys must be strings.")
            if not isinstance(value, (str, int, float, list, dict, bool)):
                raise ValueError(
                    f"Invalid schema value for key {key}: {value}. Values must be strings, numbers, booleans, or plain lists or dicts."
                )

    def _create_table(self):
        columns = []
        for key, value in self.schema.items():
            column_type = self._get_column_type(value)
            columns.append(f"{key} {column_type}")
        columns_str = ", ".join(columns)
        id_column = (
            "id INTEGER PRIMARY KEY AUTOINCREMENT"
            if self.auto_key
            else "id TEXT PRIMARY KEY"
        )
        self._connection.execute(
            f"CREATE TABLE IF NOT EXISTS {self.name} ({id_column}, {columns_str})"
        )
        self._connection.commit()

    def _get_column_type(self, value):
        if isinstance(value, bool):
            return "INTEGER"  # SQLite does not have a native BOOLEAN type, but INTEGER can store 0 (False) or 1 (True)
        elif isinstance(value, (int, float)):
            return "NUMBER"
        elif isinstance(value, (list, dict)):
            return "JSON"
        else:
            return "TEXT"

    def optimize(self, key_name):
        if key_name not in self.schema:
            raise ValueError(f"Invalid key_name: {key_name}. Key not in schema.")

        index_name = f"{self.name}_{key_name}_idx"
        self._connection.execute(
            f"CREATE INDEX IF NOT EXISTS {index_name} ON {self.name} ({key_name})"
        )
        self._connection.commit()

    def list_optimized_keys(self):
        cursor = self._connection.cursor()
        cursor.execute(
            f"SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='{self.name}'"
        )
        results = cursor.fetchall()
        optimized_keys = []
        for result in results:
            index_name = result["name"]
            index_sql = result["sql"]
            if index_sql is not None:
                column_name = re.search(r"\((.*?)\)", index_sql).group(1)
                optimized_keys.append(column_name)
        return optimized_keys

    def drop(self):
        """
        Drops the table from the database.
        """
        query = f"DROP TABLE IF EXISTS {self.name}"
        self._connection.execute(query)
        self._connection.commit()

    def add(self, value):
        if not self.auto_key:
            raise ValueError("The add function can only be used when auto_key is True")

        if isinstance(value, dict):
            value_list = [value]
        elif isinstance(value, list):
            value_list = value
        else:
            raise ValueError("The add function accepts a dict or a list of dicts")

        # Collect all unique keys in a set
        key_set = set()
        for item in value_list:
            key_set.update(item.keys())

        # Cast the set to a list maintaining order
        keys = list(key_set)

        all_values = []
        for item in value_list:
            values = []
            for key in keys:
                if key == "id":
                    raise ValueError(
                        "The add function should not include an 'id' key in the input dict"
                    )

                # Handle missing keys by setting the value to None
                val = item.get(key, None)

                if isinstance(val, (list, dict)):
                    val = json.dumps(val)

                values.append(val)

            all_values.append(values)

        keys_str = ", ".join(keys)
        placeholders_str = ", ".join(["?"] * len(keys))

        query = f"""
        INSERT INTO {self.name} ({keys_str})
        VALUES ({placeholders_str})
        """

        self._connection.executemany(query, all_values)
        self._connection.commit()

    def set(self, key, value):
        if self.auto_key and isinstance(key, str):
            raise KeyError(
                "The index has auto_key. use add Function to add new items to the index"
            )

        if isinstance(key, tuple):
            id, column, *keys = key

            if not keys:
                if isinstance(value, (list, dict)):
                    value = json.dumps(value)

                query = f"""
                INSERT INTO {self.name} (id, {column})
                VALUES (?, ?)
                ON CONFLICT(id) DO UPDATE SET {column} = ?
                """
                self._connection.execute(query, (id, value, value))
                self._connection.commit()
            else:
                json_set_path = f'$.{".".join([str(k) for k in keys])}'
                query = f"""
                INSERT INTO {self.name} (id, {column})
                VALUES (?, json(?))
                ON CONFLICT(id) DO UPDATE SET {column} = json_set({column}, ?, ?)
                """
                self._connection.execute(
                    query,
                    (
                        id,
                        json.dumps({keys[-1]: value}),
                        json_set_path,
                        json.dumps(value),
                    ),
                )
                self._connection.commit()
        else:
            id = key
            item = value
            item["id"] = id
            keys = []
            values = []
            placeholders = []
            for key, value in item.items():
                keys.append(key)

                if isinstance(value, (list, dict)):
                    value = json.dumps(value)

                values.append(value)
                placeholders.append("?")
            keys_str = ", ".join(keys)
            placeholders_str = ", ".join(placeholders)
            update_str = ", ".join([f"{key} = ?" for key in keys])

            query = f"""
            INSERT INTO {self.name} ({keys_str})
            VALUES ({placeholders_str})
            ON CONFLICT(id) DO UPDATE SET {update_str}
            """

            self._connection.execute(query, values * 2)
            self._connection.commit()

    def delete(self, x, return_deleted=False):
        to_del_keys = []
        to_del_by_query = None

        if isinstance(x, str):
            to_del_keys = [x]
        elif isinstance(x, (list, tuple, set)):
            to_del_keys = list(x)
        elif isinstance(x, dict):
            to_del_by_query = x

        to_return = None

        if to_del_keys:
            if return_deleted:
                select_query_str = f"SELECT * FROM {self.name} WHERE id IN ({', '.join(['?' for _ in to_del_keys])})"
                to_return = [
                    self.__row_to_id_and_item(row)
                    for row in self._connection.execute(select_query_str, to_del_keys)
                ]

            delete_query_str = f"DELETE FROM {self.name} WHERE id IN ({', '.join(['?' for _ in to_del_keys])})"
            self._connection.execute(delete_query_str, to_del_keys)
            self._connection.commit()

        elif to_del_by_query is not None:
            if return_deleted:
                to_return = list(self.search(query=to_del_by_query))
                to_del_keys = [_ for _, __ in to_return]
                delete_query_str = f"DELETE FROM {self.name} WHERE id IN ({', '.join(['?' for _ in to_del_keys])})"
                self._connection.execute(delete_query_str, to_del_keys)
                self._connection.commit()
            else:
                delete_query_str, params = delete_query(
                    self.name, to_del_by_query, self.column_type_map
                )
                self._connection.execute(delete_query_str, params)
                self._connection.commit()

        return to_return

    def get(self, id, *keys):
        """Retrieve an item or a specific key path value from the item in the index by its id."""
        value = None
        if len(keys) == 1:
            query = f"SELECT {keys[0]} FROM {self.name} WHERE id = ?"
            cursor = self._connection.execute(query, (id,))
            row = cursor.fetchone()
            if row:
                value = row[0] if row[0] is not None else None
                # Check if the column is a JSON column and parse the JSON string
                if len(keys) >= 1 and self.column_type_map[keys[0]] == "JSON":
                    value = json.loads(value) if value is not None else None

        elif len(keys) > 1:
            column = keys[0]
            key_path_parts = []
            for key in keys[1:]:
                if isinstance(key, int):
                    key_path_parts.append(f"[{key}]")
                else:
                    key_path_parts.append(f".{key}")
            key_path = "$" + "".join(key_path_parts)
            query = f"SELECT json_extract({column}, ?) FROM {self.name} WHERE id = ?"
            cursor = self._connection.execute(query, (key_path, id))
            row = cursor.fetchone()
            if row:
                value = row[0] if row[0] is not None else None
        else:
            query = f"SELECT * FROM {self.name} WHERE id = ?"
            cursor = self._connection.execute(query, (id,))
            row = cursor.fetchone()
            if row:
                value = self.__row_to_id_and_item(row)[1]

        if not value:
            raise KeyError(f"Key {id} {keys} not found in index {self.name}")

        return value

    def __getitem__(self, key):
        if isinstance(key, tuple):
            id, *keys = key
            return self.get(id, *keys)
        else:
            id = key
            return self.get(id)

    def __row_to_id_and_item(self, row):
        item = dict(row)
        for k, v in item.items():
            try:
                item[k] = json.loads(v)
            except:
                pass

        return item.pop("id"), item

    def search(
        self,
        query={},
        sort_by=None,
        reversed_sort=False,
        n=None,
        page=None,
        page_size=50,
        select_columns=None,
    ):
        query, params = search_query(
            table_name=self.name,
            query=query,
            column_type_map=self.column_type_map,
            sort_by=sort_by,
            reversed_sort=reversed_sort,
            n=n,
            page=page,
            page_size=page_size,
            select_columns=select_columns,
        )
        cursor = self._connection.execute(query, params)

        def gen():
            for row in cursor:
                yield self.__row_to_id_and_item(row)

        if n is None and page is None:
            return gen()
        else:
            return [self.__row_to_id_and_item(row) for row in cursor.fetchall()]

    def count(self, query={}):
        query, params = count_query(
            table_name=self.name, query=query, column_type_map=self.column_type_map
        )
        cursor = self._connection.execute(query, params)
        row = cursor.fetchone()
        return row[0]

    def distinct(self, column, query={}):
        query, params = distinct_query(
            table_name=self.name,
            column=column,
            query=query,
            column_type_map=self.column_type_map,
        )
        cursor = self._connection.execute(query, params)
        rows = cursor.fetchall()
        if rows:
            return [row[0] for row in rows if row is not None]
        else:
            return []



if __name__ == "__main__":
    # Define the schema for the index
    schema = {
        "name": "",
        "age": 0,
        "address": {"street": "", "city": "", "state": "", "country": ""},
        "years": [1, 2, 3, 4],
    }

    # Create a new index with the specified schema
    index = DefinedIndex("test_index", schema=schema)
    # Set an item in the index
    item = {
        "name": "John Doe",
        "age": 30,
        "address": {
            "street": "123 Main St",
            "city": "New York",
            "state": "NY",
            "country": "USA",
        },
    }
    index.set("1", item)

    index.set(("1", "name"), "updated John Doe")
    print(index.get("1"))

    index.set(("1", "years"), [1, 2, 3])
    print(index.get("1"))

    index.set(("1", "years", 1), 4)
    print(index.get("1"))

    index.set(("1", "address", "state"), "updated NY 2")
    print(index.get("1"))

    # Search for items in the index
    query = {"age": {"$lte": 35}}
    results = index.search(query)
    for result in results:
        print("--", result)

    # Count the number of items matching a query
    count = index.count(query)
    print("Count:", count)

    # distinct the number of items matching a query
    distinct = index.distinct("age", query)
