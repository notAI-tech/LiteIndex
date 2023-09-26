import common_utils

common_utils.set_ulimit()

import re
import os
import time
import json
import pickle
import sqlite3
import datetime

from query_parser import (
    search_query,
    distinct_query,
    count_query,
    delete_query,
    group_by_query,
    max_query,
    avg_query,
    min_query,
    sum_query,
)

import threading


class DefinedIndex:
    def __init__(self, name, schema=None, example=None, db_path=":memory:"):
        if name.startswith("__"):
            raise ValueError("Index name cannot start with '__'")

        self.name = name
        if not schema and example:
            schema = {}
            for k, v in example.items():
                if isinstance(v, bool):
                    schema[k] = "boolean"
                elif isinstance(v, int) or isinstance(v, float):
                    schema[k] = "number"
                elif isinstance(v, str):
                    schema[k] = "string"
                elif isinstance(v, dict) or isinstance(v, list):
                    schema[k] = "json"
                elif isinstance(v, bytes):
                    schema[k] = "blob"
                elif isinstance(v, datetime.datetime):
                    schema[k] = "datetime"
                else:
                    schema[k] = "other"

        self.schema = schema
        self.hashed_key_schema = {}
        self.meta_table_name = f"__{name}_meta"
        self.db_path = db_path
        self.key_hash_to_original_key = {}
        self.original_key_to_key_hash = {}
        self.schema_property_to_column_type = {
            "boolean": "INTEGER",
            "number": "NUMBER",
            "string": "TEXT",
            "json": "JSON",
            "blob": "BLOB",
            "datetime": "NUMBER",
            "other": "BLOB",
        }
        self.not_allowed_character_in_id = chr(31)

        if not db_path == ":memory:":
            db_dir = os.path.dirname(self.db_path).strip()
            if not db_dir:
                db_dir = "./"

            if not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)

        self.local_storage = threading.local()

        self._validate_set_schema_if_exists()
        self._parse_schema()

        self._create_table_and_meta_table()

    def __del__(self):
        if self._connection:
            self._connection.close()
            self._connection = None

    @property
    def _connection(self):
        if (
            not hasattr(self.local_storage, "db_conn")
            or self.local_storage.db_conn is None
        ):
            self.local_storage.db_conn = sqlite3.connect(self.db_path, uri=True)
            self.local_storage.db_conn.execute("PRAGMA journal_mode=WAL")
            self.local_storage.db_conn.execute("PRAGMA synchronous=NORMAL")
        return self.local_storage.db_conn

    def _validate_set_schema_if_exists(self):
        try:
            rows = self._connection.execute(
                f"SELECT * FROM {self.meta_table_name}"
            ).fetchall()
        except:
            return

        if self.schema is None:
            self.schema = {}

            for _hash, pickled, value_type in rows:
                self.key_hash_to_original_key[_hash] = pickle.loads(pickled)
                self.original_key_to_key_hash[pickle.loads(pickled)] = _hash
                self.schema[pickle.loads(pickled)] = value_type
        else:
            for _hash, pickled, value_type in rows:
                if pickle.loads(pickled) not in self.schema:
                    raise ValueError(
                        f"Schema mismatch: {pickle.loads(pickled)} not found in schema"
                    )
                if self.schema[pickle.loads(pickled)] != value_type:
                    raise ValueError(
                        f"Schema mismatch: {pickle.loads(pickled)} has type {value_type} but {self.schema[pickle.loads(pickled)]} was expected"
                    )

            if len(self.schema) != len(rows):
                raise ValueError("existing schema does not match the provided schema")

    def _parse_schema(self):
        for key, value_type in self.schema.items():
            if value_type not in self.schema_property_to_column_type:
                raise ValueError(f"Invalid schema property: {value_type}")
            key_hash = common_utils.stable_hash(key)
            self.key_hash_to_original_key[key_hash] = key
            self.original_key_to_key_hash[key] = key_hash
            self.hashed_key_schema[key_hash] = value_type

    def _create_table_and_meta_table(self):
        columns = []
        meta_columns = []
        for key, value_type in self.schema.items():
            key_hash = self.original_key_to_key_hash[key]
            sql_column_type = self.schema_property_to_column_type[value_type]
            columns.append(f'"{key_hash}" {sql_column_type}')
            meta_columns.append((key_hash, pickle.dumps(key), value_type))

        columns_str = ", ".join(columns)

        self._connection.execute(
            f"CREATE TABLE IF NOT EXISTS {self.name} (id TEXT PRIMARY KEY, updated_at NUMBER, {columns_str})"
        )

        self._connection.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self.name}_updated_at ON {self.name} (updated_at)"
        )

        # Create the metadata table
        self._connection.execute(
            f"CREATE TABLE IF NOT EXISTS {self.meta_table_name} "
            "(hash TEXT PRIMARY KEY, pickled BLOB, value_type TEXT)"
        )

        self._connection.executemany(
            f"INSERT OR IGNORE INTO {self.meta_table_name} (hash, pickled, value_type) "
            f"VALUES (?, ?, ?)",
            [
                (hash_val, sqlite3.Binary(pickled), value_type)
                for hash_val, pickled, value_type in meta_columns
            ],
        )

        self._connection.commit()

    def update(self, data):
        transactions = []

        # Prepare the SQL command
        all_columns = ["id", "updated_at"]
        all_columns.extend(self.original_key_to_key_hash.values())
        columns = ", ".join([f'"{h}"' for h in all_columns])
        values = ", ".join(["?" for _ in range(len(all_columns))])
        sql = f"REPLACE INTO {self.name} ({columns}) VALUES ({values})"

        # Iterate through each item in the data
        for k, _data in data.items():
            if self.not_allowed_character_in_id in k:
                raise ValueError(
                    f"Invalid character '{self.not_allowed_character_in_id}' in id: {k}"
                )
            # Create a new dictionary to store processed (hashed key) data
            processed_data = {h: None for h in all_columns}
            processed_data["id"] = k
            processed_data["updated_at"] = time.time()
            for key, value in _data.items():
                # Get the hashed equivalent of the key
                key_hash = self.original_key_to_key_hash[key]

                # Process the value based on its type
                if self.schema[key] == "other":
                    value = sqlite3.Binary(pickle.dumps(value))
                elif self.schema[key] == "datetime":
                    value = value.timestamp()
                elif self.schema[key] == "json":
                    value = json.dumps(value)

                processed_data[key_hash] = value

            transactions.append(tuple(processed_data.values()))

        # Execute the SQL command using `executemany`
        self._connection.executemany(sql, transactions)
        self._connection.commit()

    def get(self, ids):
        if isinstance(ids, str):
            ids = [ids]

        # Prepare the SQL command
        columns = ", ".join([f'"{h}"' for h in self.original_key_to_key_hash.values()])
        column_str = "id, " + columns  # Update this to include `id`

        # Format the ids for the where clause
        id_placeholders = ", ".join(["?" for _ in ids])
        sql = f"SELECT {column_str} FROM {self.name} WHERE id IN ({id_placeholders})"

        cursor = self._connection.execute(sql, ids)

        result = {}
        for row in cursor.fetchall():
            record = {
                self.key_hash_to_original_key[h]: val
                for h, val in zip(self.original_key_to_key_hash.values(), row[1:])
                if val is not None
            }
            for k, v in record.items():
                if v is None:
                    continue
                if self.schema[k] == "other":
                    record[k] = pickle.loads(v)
                elif self.schema[k] == "datetime":
                    record[k] = datetime.datetime.fromtimestamp(v)
                elif self.schema[k] == "json":
                    record[k] = json.loads(v)
                elif self.schema[k] == "boolean":
                    record[k] = bool(v)

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

    def search(
        self,
        query={},
        sort_by="updated_at",
        reversed_sort=False,
        n=None,
        page_no=None,
        select_keys=[],
    ):
        if {
            k
            for k in query
            if k not in self.schema or self.schema[k] in {"blob", "other"}
        }:
            raise ValueError("Invalid query")

        sql_query, sql_params = search_query(
            table_name=self.name,
            query={self.original_key_to_key_hash[k]: v for k, v in query.items()},
            schema=self.hashed_key_schema,
            sort_by=sort_by,
            reversed_sort=reversed_sort,
            n=n,
            page=page_no,
            page_size=n if page_no else None,
            select_columns=(
                ["id", "updated_at"]
                + [self.original_key_to_key_hash[k] for k in select_keys]
            )
            if select_keys
            else None,
        )

        results = {}
        for result in self._connection.execute(sql_query, sql_params).fetchall():
            _id, updated_at = result[:2]
            record = {
                self.key_hash_to_original_key[h]: val
                for h, val in zip(self.original_key_to_key_hash.values(), result[2:])
                if val is not None
            }
            for k, v in record.items():
                if self.schema[k] == "other":
                    record[k] = pickle.loads(v)
                elif self.schema[k] == "datetime":
                    record[k] = datetime.datetime.fromtimestamp(v)
                elif self.schema[k] == "json":
                    record[k] = json.loads(v)
                elif self.schema[k] == "boolean":
                    record[k] = bool(v)

            results[_id] = record

        return results

    def distinct(self, key, query):
        sql_query, sql_params = distinct_query(
            table_name=self.name,
            column=self.original_key_to_key_hash[key],
            query={self.original_key_to_key_hash[k]: v for k, v in query.items()},
            schema=self.hashed_key_schema,
        )

        return {
            _[0] for _ in self._connection.execute(sql_query, sql_params).fetchall()
        }

    def group(self, keys, query):
        if isinstance(keys, str):
            keys = [keys]

        sql_query, sql_params = group_by_query(
            table_name=self.name,
            columns=[self.original_key_to_key_hash[key] for key in keys],
            query={self.original_key_to_key_hash[k]: v for k, v in query.items()},
            schema=self.hashed_key_schema,
        )

        return {
            _[0]: _[1].split(self.not_allowed_character_in_id)
            for _ in self._connection.execute(sql_query, sql_params).fetchall()
        }

    def delete(self, ids=None, query=None):
        if query:
            sql_query, sql_params = delete_query(
                table_name=self.name,
                query={self.original_key_to_key_hash[k]: v for k, v in query.items()},
                schema=self.hashed_key_schema,
            )

            self._connection.execute(sql_query, sql_params)
            self._connection.commit()
        elif ids:
            if isinstance(ids, str):
                ids = [ids]

            placeholders = ", ".join(["?" for _ in ids])
            sql_query = f"DELETE FROM {self.name} WHERE id IN ({placeholders})"
            self._connection.execute(sql_query, ids)
            self._connection.commit()
        else:
            raise ValueError("Either ids or query must be provided")

    def count(self, query={}):
        sql_query, sql_params = count_query(
            table_name=self.name,
            query={self.original_key_to_key_hash[k]: v for k, v in query.items()},
            schema=self.hashed_key_schema,
        )

        return self._connection.execute(sql_query, sql_params).fetchone()[0]

    def optimize_key_for_querying(self, key, is_unique=False):
        if self.schema[key] in {"string", "number", "boolean", "datetime"}:
            key_hash = self.original_key_to_key_hash[key]
            if not is_unique:
                self._connection.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{self.name}_{key_hash} ON {self.name} ({key_hash})"
                )
            else:
                self._connection.execute(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{self.name}_{key_hash} ON {self.name} ({key_hash})"
                )

            self._connection.commit()
        else:
            raise ValueError(
                f"Cannot optimize for querying on {key}. Only string, number, boolean and datetime types are supported"
            )

    def querying_optimized_keys(self):
        return {
            k: v
            for k, v in {
                self.key_hash_to_original_key.get(
                    _[1].replace(f"idx_{self.name}_", "")
                ): {"is_unique": bool(_[2])}
                for _ in self._connection.execute(
                    f"PRAGMA index_list({self.name})"
                ).fetchall()
                if _[1].startswith(f"idx_{self.name}_")
            }.items()
            if k and v
        }

    def math(self, key, op, query={}):
        if op not in {"sum", "avg", "min", "max"}:
            raise ValueError("Invalid operation")

        op_func = {
            "sum": sum_query,
            "avg": avg_query,
            "min": min_query,
            "max": max_query,
        }[op]

        sql_query, sql_params = op_func(
            table_name=self.name,
            column=self.original_key_to_key_hash[key],
            query={self.original_key_to_key_hash[k]: v for k, v in query.items()},
            schema=self.hashed_key_schema,
        )

        return self._connection.execute(sql_query, sql_params).fetchone()[0]


if __name__ == "__main__":
    schema = {
        "name": "string",
        "age": "number",
        "password": "string",
        "verified": "boolean",
        "nicknames": "json",
        "address_details": "json",
        "profile_picture": "blob",
        "user_vector": "other",
    }

    index = DefinedIndex(name="user_details", schema=schema)

    index.update(
        {
            "user1": {
                "name": "John Doe",
                "age": 25,
                "password": "password123",
                "verified": True,
                "nicknames": ["John", "Johnny"],
                "address_details": {
                    "city": "New York",
                    "state": "New York",
                    "country": "USA",
                },
                "profile_picture": b"some binary data here",
            },
            "user2": {
                "name": "Jane Doe",
                "age": 22,
            },
        }
    )

    index.update(
        {
            "user3": {
                "name": "John Doe",
                "age": 25,
                "password": "password123",
                "verified": True,
                "nicknames": ["John", "Johnny"],
                "address_details": {
                    "city": "New York",
                    "state": "New York",
                    "country": "USA",
                },
                "profile_picture": b"some binary data here",
            },
            "user4": {
                "name": "Jane Doe",
                "age": 22,
            },
        }
    )

    print("-->", index.querying_optimized_keys())
    index.optimize_key_for_querying("name")
    print("-->", index.querying_optimized_keys())

    print(
        "---->",
        index.math("age", "sum", query={"age": {"$gt": 20}}),
        index.math("age", "sum", query={"age": {"$gt": 60}}),
    )

    # print(index.get("user1", "user2", "user3"))

    print(index.search(query={"age": {"$gt": 20}}))

    print("Get:", index.get(["user1", "user2", "user3", "user4"]))

    print(index.distinct(key="name", query={"age": {"$gt": 20}}))

    print(index.group(keys="name", query={"age": {"$gt": 20}}))

    print(index.count(query={"age": {"$gt": 20}}), index.count())

    index.delete(ids=["user1", "user2"])

    print(index.group(keys="name", query={"age": {"$gt": 20}}))

    print(index.count(query={"age": {"$gt": 20}}))

    index.clear()

    print(index.count(query={"age": {"$gt": 20}}), index.count())

    index.drop()

    print(index.count(query={"age": {"$gt": 20}}))
