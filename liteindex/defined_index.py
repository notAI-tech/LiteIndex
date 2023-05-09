import json
import sqlite3
from typing import Any, Union, List, Iterator, Optional, Dict, Tuple


class DefinedIndex:
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

    def _get_value_from_column_type(self, column_type: str):
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

    def _get_column_type(self, value: Any) -> str:
        if isinstance(value, bool):
            return "INTEGER"  # SQLite does not have a native BOOLEAN type, but INTEGER can store 0 (False) or 1 (True)
        elif isinstance(value, (int, float)):
            return "NUMBER"
        elif isinstance(value, (list, dict)):
            return "JSON"
        else:
            return "TEXT"

    def _process_query_conditions(
        self, query: Dict, prefix: Optional[List[str]] = None
    ) -> Tuple[List[str], List]:
        where_conditions = []
        params = []

        if prefix is None:
            prefix = []

        def process_nested_query(
            value: Union[Dict, Tuple, List, int, str, float], prefix: List[str]
        ) -> None:
            nonlocal where_conditions, params
            column = (
                "json_extract(" + prefix[0] + ", '$." + ".".join(prefix[1:]) + "')"
                if len(prefix) > 1
                else prefix[0]
            )

            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    process_nested_query(sub_value, prefix + [sub_key])
            elif isinstance(value, tuple):
                if value[0] is not None:
                    where_conditions.append(f"{column} >= ?")
                    params.append(value[0])
                if value[1] is not None:
                    where_conditions.append(f"{column} <= ?")
                    params.append(value[1])
            elif isinstance(value, list):
                where_conditions.append(
                    f"{column} IN ({', '.join(['?' for _ in value])})"
                )
                params.extend(value)
            else:
                where_conditions.append(f"{column} = ?")
                params.append(value)

        for key, value in query.items():
            process_nested_query(value, [key])

        return where_conditions, params

    def set(self, key: Union[str, Tuple[str, Union[str, int]]], value: Any) -> None:
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

    def get(self, id: str, *keys: Union[str, int]) -> Optional[Any]:
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

    def __getitem__(
        self, key: Union[str, Tuple[str, Union[str, int]]]
    ) -> Optional[Any]:
        if isinstance(key, tuple):
            id, *keys = key
            return self.get(id, *keys)
        else:
            id = key
            return self.get(id)

    def __row_to_id_and_item(self, row: sqlite3.Row) -> Tuple[str, Dict[str, Any]]:
        item = dict(row)
        for k, v in item.items():
            try:
                item[k] = json.loads(v)
            except:
                pass

        return item["id"], item

    def search(
        self,
        query: Optional[Dict],
        sort_by: Optional[str] = None,
        reversed_sort: Optional[bool] = False,
    ) -> List[Dict]:

        if query:
            where_conditions, params = self._process_query_conditions(query)
            where_clause = " AND ".join(where_conditions)
            query = f"SELECT * FROM {self.name} WHERE {where_clause}"
        else:
            query = f"SELECT * FROM {self.name}"
            params = []

        if sort_by:
            query += f" ORDER BY {sort_by}"
            if reversed_sort:
                query += " DESC"

        cursor = self._connection.execute(query, params)
        for row in cursor:
            yield self.__row_to_id_and_item(row)

    def count(self, query: Optional[Dict] = None) -> int:
        if query:
            where_conditions, params = self._process_query_conditions(query)
            where_clause = " AND ".join(where_conditions)
            query = f"SELECT COUNT(*) FROM {self.name} WHERE {where_clause}"
        else:
            query = f"SELECT COUNT(*) FROM {self.name}"
            params = []

        cursor = self._connection.execute(query, params)
        return cursor.fetchone()[0]

    def sum(self, column: str, query: Optional[Dict] = None) -> float:
        if query:
            where_conditions, params = self._process_query_conditions(query)
            where_clause = " AND ".join(where_conditions)
            query = f"SELECT SUM({column}) FROM {self.name} WHERE {where_clause}"
        else:
            query = f"SELECT SUM({column}) FROM {self.name}"
            params = []
        cursor = self._connection.execute(query, params)
        return cursor.fetchone()[0]

    def average(self, column: str, query: Optional[Dict] = None) -> float:
        if query:
            where_conditions, params = self._process_query_conditions(query)
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
