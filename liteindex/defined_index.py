import json
import sqlite3
from typing import Any, Union, List, Iterator, Optional, Dict, Tuple

class DefinedIndex():
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

    def _validate_schema(self):
        for key, value in self.schema.items():
            if not isinstance(key, str):
                raise ValueError(
                    f"Invalid schema key: {key}. Keys must be strings."
                )
            if not isinstance(value, (str, int, float, list, dict)):
                raise ValueError(
                    f"Invalid schema value for key {key}: {value}. Values must be strings, numbers, or plain lists or dicts."
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
        if isinstance(value, (int, float)):
            return "NUMBER"
        elif isinstance(value, (list, dict)):
            return "JSON"
        else:
            return "TEXT"

    def _process_query_conditions(self, query: Dict, prefix: Optional[List[str]] = None) -> Tuple[List[str], List]:
        where_conditions = []
        params = []

        if prefix is None:
            prefix = []

        for key, value in query.items():
            new_prefix = prefix + [key]

            if isinstance(value, dict):
                nested_conditions, nested_params = self._process_query_conditions(value, prefix=new_prefix)
                where_conditions.extend(nested_conditions)
                params.extend(nested_params)
            elif isinstance(value, (int, float, str)):
                if not prefix:
                    where_conditions.append(f"{key} = ?")
                else:
                    where_conditions.append(f"json_extract({new_prefix[0]}, '$.{'.'.join(new_prefix[1:])}') = ?")
                params.append(value)
            elif isinstance(value, (tuple, list)):
                if isinstance(value[0], (int, float, str)):
                    where_conditions.append(f"json_extract({new_prefix[0]}, '$.{'.'.join(new_prefix[1:])}') IN ({','.join(['?' for _ in value])})")
                    params.extend(value)
                elif isinstance(value, tuple):
                    operator, condition_value = value
                    if operator in ("<", ">", "<=", ">=", "=", "<>"):
                        where_conditions.append(f"json_extract({new_prefix[0]}, '$.{'.'.join(new_prefix[1:])}') {operator} ?")
                        params.append(condition_value)
                    else:
                        raise ValueError(f"Invalid operator: {operator}")
            else:
                raise ValueError(f"Unsupported value type: {value}")

        return where_conditions, params

        
    def set(self, id: str, item: Dict[str, Any]) -> None:
        """Insert or update an item in the index."""
        item['id'] = id
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

    def __row_to_id_and_item(self, row: sqlite3.Row) -> Tuple[str, Dict[str, Any]]:
        item = dict(row)
        for k, v in item.items():
            try:
                item[k] = json.loads(v)
            except:
                pass

        return item["id"], item

    def get(self, id: str, *keys: str) -> Optional[Dict[str, Any]]:
        """Retrieve an item or a specific key path value from the item in the index by its id."""
        if not keys:
            query = f"SELECT * FROM {self.name} WHERE id = ?"
            cursor = self._connection.execute(query, (id,))
            row = cursor.fetchone()
            if row:
                return self.__row_to_id_and_item(row)[1]
            else:
                return None
        else:
            key_path = '.'.join(keys)
            query = f"SELECT json_extract({keys[0]}, ?) FROM {self.name} WHERE id = ?"
            cursor = self._connection.execute(query, (key_path, id))
            row = cursor.fetchone()
            if row:
                return row[0] if row[0] is not None else None
            else:
                return None
                
    def search(
        self,
        query: Optional[Dict],
        sort_by: Optional[str] = None,
        reversed_sort: Optional[bool] = False
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
        "address": {
            "street": "",
            "city": "",
            "state": "",
            "country": ""
        }
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
            "country": "USA"
        }
    }
    index.set("1", item)

    # Get an item from the index
    retrieved_item = index.get("1")
    print(retrieved_item)

    # Search for items in the index
    query = {"address": {"state": "NY"}}
    results = index.search(query)
    for result in results:
        print('--', result)

    # Count the number of items matching a query
    count = index.count(query)
    print("Count:", count)

    # Calculate the sum and average of a numeric column for items matching a query
    total_age = index.sum("age", query)
    average_age = index.average("age", query)
    print("Total age:", total_age)
    print("Average age:", average_age)

