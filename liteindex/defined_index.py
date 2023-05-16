import json
import sqlite3
import random
from query_parser import search_query


class DefinedIndex:
    def __init__(self, name, schema=None, db_path=":memory:"):
        self.name = name
        self.db_path = db_path
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
            column_name = column_def.split()[0]
            column_type = column_def.split()[1]
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
        self._connection.execute(
            f"CREATE TABLE IF NOT EXISTS {self.name} (id TEXT PRIMARY KEY, {columns_str})"
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

    def set(self, key, value):
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

    def get(self, id, *keys):
        """Retrieve an item or a specific key path value from the item in the index by its id."""
        value = None
        if len(keys) == 1:
            query = f"SELECT {keys[0]} FROM {self.name} WHERE id = ?"
            cursor = self._connection.execute(query, (id,))
            row = cursor.fetchone()
            if row:
                value = row[0] if row[0] is not None else None
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

        return item["id"], item

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

    def count(self, query):
        if query:
            where_conditions, params = parse_query(query, self.column_type_map)
            where_clause = " AND ".join(where_conditions)
            query = f"SELECT COUNT(*) FROM {self.name} WHERE {where_clause}"
        else:
            query = f"SELECT COUNT(*) FROM {self.name}"
            params = []

        cursor = self._connection.execute(query, params)
        return cursor.fetchone()[0]

    def sum(self, column, query):
        if query:
            where_conditions, params = parse_query(query, self.column_type_map)
            where_clause = " AND ".join(where_conditions)
            query = f"SELECT SUM({column}) FROM {self.name} WHERE {where_clause}"
        else:
            query = f"SELECT SUM({column}) FROM {self.name}"
            params = []
        cursor = self._connection.execute(query, params)
        return cursor.fetchone()[0]

    def average(self, column, query):
        if query:
            where_conditions, params = parse_query(query, self.column_type_map)
            where_clause = " AND ".join(where_conditions)
            query = f"SELECT AVG({column}) FROM {self.name} WHERE {where_clause}"
        else:
            query = f"SELECT AVG({column}) FROM {self.name}"
            params = []
        cursor = self._connection.execute(query, params)
        return cursor.fetchone()[0]


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
    query = {"age": (None, 35)}
    results = index.search(query)
    for result in results:
        print("--", result)

    # Count the number of items matching a query
    count = index.count(query)
    print("Count:", count)

    # Calculate the sum and average of a numeric column for items matching a query
    total_age = index.sum("age", query)
    average_age = index.average("age", query)
    print("Total age:", total_age)
    print("Average age:", average_age)
