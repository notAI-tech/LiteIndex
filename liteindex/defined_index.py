from . import common_utils

common_utils.set_ulimit()

import re
import os
import time
import json
import pickle
import sqlite3
import datetime

try:
    import zstandard
except:
    zstandard = None

from .query_parser import (
    search_query,
    distinct_query,
    count_query,
    delete_query,
    group_by_query,
    max_query,
    avg_query,
    min_query,
    sum_query,
    plus_equals_query,
    minus_equals_query,
    multiply_equals_query,
    divide_equals_query,
    floor_divide_equals_query,
    modulo_equals_query,
    pop_query,
)

import threading


class DefinedIndex:
    def __init__(
        self,
        name,
        schema=None,
        example=None,
        import_from_file=None,
        file_type=None,
        db_path=None,
        ram_cache_mb=64,
    ):
        if name.startswith("__"):
            raise ValueError("Index name cannot start with '__'")

        self.name = name
        self.ram_cache_mb = ram_cache_mb
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
        self.db_path = ":memory:" if db_path is None else db_path
        self.key_hash_to_original_key = {}
        self.original_key_to_key_hash = {}
        self.column_names = []

        self.schema_property_to_column_type = {
            "boolean": "INTEGER",
            "number": "NUMBER",
            "string": "TEXT",
            "json": "JSON",
            "blob": "BLOB",
            "datetime": "NUMBER",
            "other": "BLOB",
        }

        if not db_path == ":memory:":
            db_dir = os.path.dirname(self.db_path).strip()
            if not db_dir:
                db_dir = "./"

            if not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)

        self.local_storage = threading.local()

        self._validate_set_schema_if_exists()

        if not self.schema:
            raise ValueError("Schema must be provided")

        self._parse_schema()
        self._create_table_and_meta_table()

    def __del__(self):
        if self._connection:
            self._connection.close()

    def serialize_record(self, record):
        _record = {}
        for k, v in record.items():
            if v is None:
                _record[self.original_key_to_key_hash[k]] = None

            elif self.schema[k] == "other":
                _record[self.original_key_to_key_hash[k]] = sqlite3.Binary(
                    self._compressor.compress(pickle.dumps(v, protocol=5))
                    if self._compressor is not False
                    else pickle.dumps(v, protocol=5)
                )
            elif self.schema[k] == "datetime":
                _record[self.original_key_to_key_hash[k]] = v.timestamp()
            elif self.schema[k] == "json":
                _record[self.original_key_to_key_hash[k]] = json.dumps(v)
            elif self.schema[k] == "boolean":
                _record[self.original_key_to_key_hash[k]] = int(v)
            elif self.schema[k] == "blob":
                _record[f"__size_{self.original_key_to_key_hash[k]}"] = len(v)
                _record[
                    f"__hash_{self.original_key_to_key_hash[k]}"
                ] = common_utils.hash_bytes(v)
                _record[self.original_key_to_key_hash[k]] = sqlite3.Binary(
                    self._compressor.compress(v) if self._compressor is not False else v
                )
            else:
                _record[self.original_key_to_key_hash[k]] = v

        return _record

    def deserialize_record(self, record, return_compressed=False):
        _record = {}
        for k, v in record.items():
            if v is None:
                _record[self.key_hash_to_original_key[k]] = None

            elif self.schema[self.key_hash_to_original_key[k]] == "other":
                _record[self.key_hash_to_original_key[k]] = pickle.loads(
                    self._decompressor.decompress(v)
                    if (self._decompressor is not False or return_compressed)
                    else v
                ) if not return_compressed else v
            elif self.schema[self.key_hash_to_original_key[k]] == "datetime":
                _record[
                    self.key_hash_to_original_key[k]
                ] = datetime.datetime.fromtimestamp(v)
            elif self.schema[self.key_hash_to_original_key[k]] == "json":
                _record[self.key_hash_to_original_key[k]] = json.loads(v)
            elif self.schema[self.key_hash_to_original_key[k]] == "boolean":
                _record[self.key_hash_to_original_key[k]] = bool(v)
            elif self.schema[self.key_hash_to_original_key[k]] == "blob":
                _record[self.key_hash_to_original_key[k]] = bytes(
                    self._decompressor.decompress(v)
                    if self._decompressor is not False
                    else v
                ) if not return_compressed else v
            else:
                _record[self.key_hash_to_original_key[k]] = v

        return _record

    @property
    def _connection(self):
        if (
            not hasattr(self.local_storage, "db_conn")
            or self.local_storage.db_conn is None
        ):
            self.local_storage.db_conn = sqlite3.connect(self.db_path, uri=True)
            self.local_storage.db_conn.execute("PRAGMA journal_mode=WAL")
            self.local_storage.db_conn.execute("PRAGMA synchronous=NORMAL")

            self.local_storage.db_conn.execute("PRAGMA auto_vacuum=FULL")
            self.local_storage.db_conn.execute("PRAGMA auto_vacuum_increment=1000")

            self.local_storage.db_conn.execute(
                f"PRAGMA cache_size=-{self.ram_cache_mb * 1024}"
            )

        return self.local_storage.db_conn

    @property
    def _compressor(self):
        if (
            not hasattr(self.local_storage, "compressor")
            or self.local_storage.compressor is None
        ):
            self.local_storage.compressor = (
                zstandard.ZstdCompressor(level=3) if zstandard is not None else False
            )
        return self.local_storage.compressor

    @property
    def _decompressor(self):
        if (
            not hasattr(self.local_storage, "decompressor")
            or self.local_storage.decompressor is None
        ):
            self.local_storage.decompressor = (
                zstandard.ZstdDecompressor() if zstandard is not None else False
            )
        return self.local_storage.decompressor

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
        for _i, (key, value_type) in enumerate(self.schema.items()):
            if value_type not in self.schema_property_to_column_type:
                raise ValueError(f"Invalid schema property: {value_type}")
            key_hash = f"col_{_i}"
            self.key_hash_to_original_key[key_hash] = key
            self.original_key_to_key_hash[key] = key_hash
            self.hashed_key_schema[key_hash] = value_type

    def _create_table_and_meta_table(self):
        columns = []
        meta_columns = []
        for key, value_type in self.schema.items():
            key_hash = self.original_key_to_key_hash[key]
            sql_column_type = self.schema_property_to_column_type[value_type]
            if value_type == "blob":
                columns.append(f'"__size_{key_hash}" NUMBER')
                self.column_names.append(f"__size_{key_hash}")
                columns.append(f'"__hash_{key_hash}" TEXT')
                self.column_names.append(f"__hash_{key_hash}")

            columns.append(f'"{key_hash}" {sql_column_type}')
            meta_columns.append((key_hash, pickle.dumps(key, protocol=5), value_type))
            self.column_names.append(key_hash)

        columns_str = ", ".join(columns)

        self._connection.execute(
            f"CREATE TABLE IF NOT EXISTS {self.name} (id TEXT PRIMARY KEY, updated_at NUMBER, {columns_str})"
        )

        self._connection.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self.name}_updated_at ON {self.name} (updated_at)"
        )

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
        ids_grouped_by_common_keys = {}

        for i, (k, _data) in enumerate(data.items()):
            keys = tuple(
                __ for _, __ in self.original_key_to_key_hash.items() if _ in _data
            )

            if keys not in ids_grouped_by_common_keys:
                ids_grouped_by_common_keys[keys] = []

            ids_grouped_by_common_keys[keys].append(k)

        with self._connection:
            updated_at = time.time()

            for keys, ids_group in ids_grouped_by_common_keys.items():
                transactions = []

                all_columns = ("id", "updated_at") + keys

                columns = ", ".join([f'"{h}"' for h in all_columns])
                values = ", ".join(["?" for _ in range(len(all_columns))])
                updates = ", ".join(
                    [f'"{f}" = excluded."{f}"' for f in all_columns if f != "id"]
                )

                sql = f"INSERT INTO {self.name} ({columns}) VALUES ({values}) ON CONFLICT(id) DO UPDATE SET {updates}"

                for k in ids_group:
                    _data = self.serialize_record(data[k])

                    _data["id"] = k
                    _data["updated_at"] = updated_at
                    transactions.append([_data[key] for key in all_columns])

                self._connection.executemany(sql, transactions)

    def get(self, ids, select_keys=[], return_compressed=False):
        if isinstance(ids, str):
            ids = [ids]

        if not select_keys:
            select_keys = list(self.original_key_to_key_hash.values())
        else:
            if [k for k in select_keys if k not in self.original_key_to_key_hash]:
                raise ValueError(
                    f"Invalid select_keys: {[k for k in select_keys if k not in self.original_key_to_key_hash]}"
                )

            select_keys = [self.original_key_to_key_hash[k] for k in select_keys]

        # Prepare the SQL command
        columns = ", ".join([f'"{h}"' for h in select_keys])
        column_str = "id, " + columns  # Update this to include `id`

        # Format the ids for the where clause
        id_placeholders = ", ".join(["?" for _ in ids])
        sql = f"SELECT {column_str} FROM {self.name} WHERE id IN ({id_placeholders})"

        result = {}
        for row in self._connection.execute(sql, ids).fetchall():
            result[row[0]] = self.deserialize_record(
                {h: val for h, val in zip(select_keys, row[1:])}, return_compressed
            )

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
        sort_by=None,
        reversed_sort=False,
        n=None,
        page_no=None,
        select_keys=[],
        update=None,
        return_compressed=False
    ):
        if {k for k in query if k not in self.schema or self.schema[k] in {"other"}}:
            raise ValueError("Invalid query")

        if sort_by:
            if sort_by not in self.schema or self.schema[sort_by] in {
                "other",
                "string",
                "json",
            }:
                raise ValueError("Invalid sort_by")

            elif self.schema[sort_by] == "blob":
                sort_by = f"__size_{self.original_key_to_key_hash[sort_by]}"
            else:
                sort_by = self.original_key_to_key_hash[sort_by]

        if not select_keys:
            select_keys = list(self.original_key_to_key_hash)

        select_keys_hashes = [self.original_key_to_key_hash[k] for k in select_keys]

        sql_query, sql_params = search_query(
            table_name=self.name,
            query={self.original_key_to_key_hash[k]: v for k, v in query.items()},
            schema=self.hashed_key_schema,
            sort_by=sort_by if sort_by is not None else "updated_at",
            reversed_sort=reversed_sort,
            n=n,
            page=page_no,
            page_size=n if page_no else None,
            select_columns=(["id", "updated_at"] + select_keys_hashes)
            if not update
            else ["id"],
        )

        _results = None

        if update:
            update = self.serialize_record(update)

            update_columns = ", ".join([f'"{h}" = ?' for h in update.keys()])

            sql_query = f"UPDATE {self.name} SET {update_columns} WHERE id IN ({sql_query}) RETURNING {', '.join(['id', 'updated_at'] + select_keys_hashes)}"

            sql_params = [_ for _ in update.values()] + sql_params

            _results = self._connection.execute(sql_query, sql_params).fetchall()

            self._connection.commit()

        else:
            _results = self._connection.execute(sql_query, sql_params).fetchall()

        results = {}

        for result in _results:
            _id, updated_at = result[:2]
            results[_id] = self.deserialize_record(
                {h: val for h, val in zip(select_keys_hashes, result[2:])}, return_compressed
            )

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
            _[0]: _[1].split(chr(31))
            for _ in self._connection.execute(sql_query, sql_params).fetchall()
        }

    def pop(self, ids=None, query={}, n=1, sort_by=None, reversed_sort=False, return_compressed=False):
        if ids is not None:
            return {
                row[0]: self.deserialize_record(
                    {h: val for h, val in zip(self.column_names, row[2:]) if h in self.key_hash_to_original_key}, return_compressed
                )
                for row in self._connection.execute(
                    f"DELETE FROM {self.name} WHERE id IN ({', '.join(['?' for _ in ids])}) RETURNING *",
                    ids,
                ).fetchall()
            }

        elif query is not None:
            sql_query, sql_params = pop_query(
                table_name=self.name,
                query={self.original_key_to_key_hash[k]: v for k, v in query.items()},
                schema=self.hashed_key_schema,
                sort_by=sort_by if sort_by is not None else "updated_at",
                reversed_sort=reversed_sort,
                n=n,
            )

            return {
                row[0]: self.deserialize_record(
                    {h: val for h, val in zip(self.column_names, row[2:]) if h in self.key_hash_to_original_key}, return_compressed
                )
                for row in self._connection.execute(sql_query, sql_params).fetchall()
            }

        else:
            raise ValueError("Either ids or query must be provided")

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

    def list_optimized_keys(self):
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
        if op not in {"sum", "avg", "min", "max", "+=", "-=", "*=", "/=", "//=", "%="}:
            raise ValueError("Invalid operation")

        op_func = {
            "sum": sum_query,
            "avg": avg_query,
            "min": min_query,
            "max": max_query,
            "+=": plus_equals_query,
            "-=": minus_equals_query,
            "*=": multiply_equals_query,
            "/=": divide_equals_query,
            "//=": floor_divide_equals_query,
            "%=": modulo_equals_query,
        }[op]

        sql_query, sql_params = op_func(
            table_name=self.name,
            column=self.original_key_to_key_hash[key],
            query={self.original_key_to_key_hash[k]: v for k, v in query.items()},
            schema=self.hashed_key_schema,
        )

        return self._connection.execute(sql_query, sql_params).fetchone()[0]

    def trigger(
        self, function, operation="UPDATE", timing="AFTER", on_keys=None, each_row=False
    ):
        trigger_name = f"{self.name}_{operation.lower()}_trigger"

        if operation.upper() not in {"INSERT", "UPDATE", "DELETE"}:
            raise ValueError("Invalid operation type. Choose: INSERT, UPDATE, DELETE.")

        if timing.upper() not in {"BEFORE", "AFTER"}:
            raise ValueError("Invalid timing. Choose: BEFORE, AFTER.")

        if operation.upper() == "UPDATE" and on_keys is None:
            raise ValueError(
                "For UPDATE operation, the affected columns should be specified."
            )

        keys_sql = f"OF {','.join(on_keys)}" if on_keys else ""

        each_row_sql = "FOR EACH ROW" if each_row else ""

        trigger_sql = f"""
        CREATE TRIGGER {trigger_name}
        {timing.upper()} {operation.upper()} {keys_sql} ON {self.name} 
        {each_row_sql}
        BEGIN
          {function};
        END;
        """

        self._connection.execute(trigger_sql)

    def list_triggers(self, table_name=None):
        if table_name:
            result = self._connection.execute(
                f"SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = '{table_name}';"
            )
        else:
            result = self._connection.execute(
                f"SELECT name FROM sqlite_master WHERE type = 'trigger';"
            )
        return result.fetchall()

    def delete_trigger(self, trigger_name):
        self._connection.execute(f"DROP TRIGGER {trigger_name};")
