import re
import os
import json
import pickle
import datetime
import sqlite3
from query_parser import search_query, distinct_query, count_query, delete_query

import hashlib
import pickle

def stable_hash(obj):
    return hashlib.sha256(pickle.dumps(obj)).hexdigest()

class DefinedIndex:
    def __init__(self, name, schema=None, db_path=":memory:"):
        if name.startswith("__"):
            raise ValueError("Index name cannot start with '__'")

        self.name = name
        self.schema = schema
        self.meta_table_name = f"__{name}_meta"
        self.db_path = db_path
        self.key_hash_to_original_key = {}
        self.original_key_to_key_hash = {}
        self.original_key_to_value_type = {}

        if not db_path == ":memory:":
            db_dir = os.path.dirname(self.db_path).strip()
            if not db_dir:
                db_dir = "./"

            if not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)

        self._connection = sqlite3.connect(self.db_path, uri=True)

        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=NORMAL")
        self._create_table_and_meta_table()


    def _get_column_types(self, value):
        # item_type can be common, bool, jsoned, pickled, datetime
        if isinstance(value, bool):
            return "INTEGER", "bool"
        elif isinstance(value, (int, float)):
            return "NUMBER", "common"
        elif isinstance(value, (list, dict)):
            try:
                json.dumps(value)
                return "JSON", "jsoned"
            except:
                return "BLOB", "pickled"

        elif isinstance(value, str):
            return "TEXT", "common"
        elif isinstance(value, datetime.datetime):
            return "NUMBER", "datetime"
        else:
            return "BLOB", "pickled"

    def _create_table_and_meta_table(self):
        # self._connection.execute(f"SELECT * FROM {self.meta_table_name}")
        # existing_schema = self._connection.fetchall()
        # if existing_schema != meta_columns:
        #     raise ValueError("Existing schema does not match supplied schema")

        key_hash_to_pickled_key = {}
        columns = []
        meta_columns = []
        for key, value in self.schema.items():
            key_hash = stable_hash(key)
            self.original_key_to_key_hash[key] = key_hash
            self.key_hash_to_original_key[key_hash] = key
            key_hash_to_pickled_key[key_hash] = pickle.dumps(key)
            sql_column_type, value_type = self._get_column_types(value)
            self.original_key_to_value_type[key] = value_type
            columns.append(f'"{key_hash}" {sql_column_type}')
            meta_columns.append((key_hash, pickle.dumps(key), value_type))

        if len(key_hash_to_pickled_key) != len(self.schema):
            raise ValueError("Keys in schema should be unique")

        columns_str = ", ".join(columns)

        self._connection.execute(
            f"CREATE TABLE IF NOT EXISTS {self.name} (id TEXT PRIMARY KEY, {columns_str})"
        )

        # Create the metadata table
        self._connection.execute(
            f"CREATE TABLE IF NOT EXISTS {self.meta_table_name} "
            "(hash INT PRIMARY KEY, pickled BLOB, value_type TEXT)"
        )

        # Populate metadata table
        for hash_val, pickled, value_type in meta_columns:
            self._connection.execute(
                f"INSERT INTO {self.meta_table_name} (hash, pickled, value_type) "
                f"VALUES (?, ?, ?)",
                (hash_val, sqlite3.Binary(pickled), value_type)
            )

        self._connection.commit()
    
    def update(self, data):
        transactions = []

        # Prepare the SQL command
        all_columns = ["id"]
        all_columns.extend(self.original_key_to_key_hash.values())
        columns = ', '.join([f'"{h}"' for h in all_columns])
        values = ', '.join(['?' for _ in range(len(all_columns))])
        sql = f"REPLACE INTO {self.name} ({columns}) VALUES ({values})"

        # Iterate through each item in the data
        for k, _data in data.items():
            # Create a new dictionary to store processed (hashed key) data
            processed_data = { h: None for h in all_columns }
            processed_data["id"] = k  # Update this to include `k`
            for key, value in _data.items():
                # Get the hashed equivalent of the key
                key_hash = self.original_key_to_key_hash[key]
                
                # Process the value based on its type
                if self.original_key_to_value_type[key] == "pickle":
                    value = sqlite3.Binary(pickle.dumps(value))
                elif self.original_key_to_value_type[key] == "datetime":
                    value = value.timestamp()
                elif self.original_key_to_value_type[key] == "jsoned":
                    value = json.dumps(value)
                
                # Store the processed data
                processed_data[key_hash] = value
            
            transactions.append(tuple(processed_data.values()))
        
        # Execute the SQL command using `executemany`
        self._connection.executemany(sql, transactions)
        self._connection.commit()
    
    def get(self, *args):
        # Prepare the SQL command
        columns = ', '.join([f'"{h}"' for h in self.original_key_to_key_hash.values()])
        column_str = "id, "+columns  # Update this to include `id`
        
        # Format the ids for the where clause
        id_placeholders = ', '.join(['?' for _ in args])
        sql = f"SELECT {column_str} FROM {self.name} WHERE id IN ({id_placeholders})"
        
        cursor = self._connection.execute(sql, args)

        result = {}
        for row in cursor.fetchall():
            record = {self.key_hash_to_original_key[h]: val for h, val in zip(self.original_key_to_key_hash.values(), row[1:]) if val is not None}
            for k, v in record.items():
                if v is None:
                    continue
                if self.original_key_to_value_type[k] == "pickle":
                    record[k] = pickle.loads(v)
                elif self.original_key_to_value_type[k] == "datetime":
                    record[k] = datetime.datetime.fromtimestamp(v)
                elif self.original_key_to_value_type[k] == "jsoned":
                    record[k] = json.loads(v)
            result[row[0]] = record
        return result
    
    def clear(self):
        # CLEAR function: deletes the content of the table but keeps the table itself and the metadata table
        self._connection.execute(f"DROP TABLE IF EXISTS {self.name}")
        self._create_table_and_meta_table()

    def drop(self):
        # DROP function: deletes both the table itself and the metadata table
        self._connection.execute(f"DROP TABLE IF EXISTS {self.name}")
        self._connection.execute(f"DROP TABLE IF EXISTS {self.meta_table_name}")
        self._connection.commit()
    
    

if __name__ == "__main__":
    schema = {
        "name": "",
        "age": 0,
        "password": "",
        "verified": False,
        "nicknames": [],
        "address_details": {},
        "profile_picture": b""
    }

    # db path defaults to memory
    # auto_key defaults to False
    index = DefinedIndex(
                name="user_details",
                schema=schema
            )
    
    index.update({
        "user1": {
            "name": "John Doe",
            "age": 25,
            "password": "password123",
            "verified": True,
            "nicknames": ["John", "Johnny"],
            "address_details": {
                "city": "New York",
                "state": "New York",
                "country": "USA"
            },
            "profile_picture": b"some binary data here"
        },
        "user2": {
            "name": "Jane Doe",
            "age": 22,
        }
    })

    print(index.get("user1", "user2", "user3"))
