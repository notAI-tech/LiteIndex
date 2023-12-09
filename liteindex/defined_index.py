from . import common_utils
from . import defined_serializers

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
        db_path=None,
        ram_cache_mb=64,
        compression_level=-1,
    ):
        self.name = name
        self.schema = schema
        self.db_path = ":memory:" if db_path is None else db_path
        self.ram_cache_mb = ram_cache_mb
        self.compression_level = compression_level

        if self.name.startswith("__"):
            raise ValueError("Index name cannot start with '__'")

        self.__meta_table_name = f"__{self.name}_meta"

        self.__hashed_key_schema = {}
        self.__key_hash_to_original_key = {}
        self.__original_key_to_key_hash = {}

        self.__column_names = []

        self.__local_storage = threading.local()

        if not self.db_path == ":memory:":
            db_dir = os.path.dirname(self.db_path).strip()
            if not db_dir:
                db_dir = "./"

            if not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)

        self.__validate_set_schema_if_exists()

        if not self.schema:
            raise ValueError("Schema must be provided")

        self.__parse_schema()
        self.__create_table_and_meta_table()

    def __del__(self):
        if self.__connection:
            self.__connection.close()

    @property
    def __connection(self):
        if (
            not hasattr(self.__local_storage, "db_conn")
            or self.__local_storage.db_conn is None
        ):
            self.__local_storage.db_conn = sqlite3.connect(self.db_path, uri=True)
            self.__local_storage.db_conn.execute("PRAGMA journal_mode=WAL")
            self.__local_storage.db_conn.execute("PRAGMA synchronous=NORMAL")

            self.__local_storage.db_conn.execute("PRAGMA auto_vacuum=FULL")
            self.__local_storage.db_conn.execute("PRAGMA auto_vacuum_increment=1000")

            self.__local_storage.db_conn.execute(
                f"PRAGMA cache_size=-{self.ram_cache_mb * 1024}"
            )

        return self.__local_storage.db_conn

    @property
    def __compressor(self):
        if self.compression_level is None:
            return False

        if (
            not hasattr(self.__local_storage, "compressor")
            or self.__local_storage.compressor is None
        ):
            self.__local_storage.compressor = (
                zstandard.ZstdCompressor(level=self.compression_level)
                if zstandard is not None
                else False
            )
        return self.__local_storage.compressor

    @property
    def __decompressor(self):
        if self.compression_level is None:
            return False

        if (
            not hasattr(self.__local_storage, "decompressor")
            or self.__local_storage.decompressor is None
        ):
            self.__local_storage.decompressor = (
                zstandard.ZstdDecompressor() if zstandard is not None else False
            )
        return self.__local_storage.decompressor

    def __validate_set_schema_if_exists(self):
        try:
            rows = self.__connection.execute(
                f"SELECT * FROM {self.__meta_table_name}"
            ).fetchall()
        except:
            return

        if self.schema is None:
            self.schema = {}

            for _hash, pickled, value_type in rows:
                self.__key_hash_to_original_key[_hash] = pickle.loads(pickled)
                self.__original_key_to_key_hash[pickle.loads(pickled)] = _hash
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

    def __parse_schema(self):
        for _i, (key, value_type) in enumerate(self.schema.items()):
            if value_type not in defined_serializers.schema_property_to_column_type:
                raise ValueError(f"Invalid schema property: {value_type}")
            key_hash = f"col_{_i}"
            self.__key_hash_to_original_key[key_hash] = key
            self.__original_key_to_key_hash[key] = key_hash
            self.__hashed_key_schema[key_hash] = value_type

    def __create_table_and_meta_table(self):
        columns = []
        meta_columns = []

        for key, value_type in self.schema.items():
            key_hash = self.__original_key_to_key_hash[key]
            sql_column_type = defined_serializers.schema_property_to_column_type[
                value_type
            ]
            if value_type in {"blob", "other"}:
                columns.append(f'"__size_{key_hash}" NUMBER')
                self.__column_names.append(f"__size_{key_hash}")
                columns.append(f'"__hash_{key_hash}" TEXT')
                self.__column_names.append(f"__hash_{key_hash}")

            columns.append(f'"{key_hash}" {sql_column_type}')
            self.__column_names.append(key_hash)

            meta_columns.append(
                (
                    key_hash,
                    pickle.dumps(key, protocol=pickle.HIGHEST_PROTOCOL),
                    value_type,
                )
            )

        columns_str = ", ".join(columns)

        with self.__connection:
            self.__connection.execute(
                f"CREATE TABLE IF NOT EXISTS {self.name} (id TEXT PRIMARY KEY, updated_at NUMBER, {columns_str})"
            )

            self.__connection.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.name}_updated_at ON {self.name} (updated_at)"
            )

            self.__connection.execute(
                f"CREATE TABLE IF NOT EXISTS {self.__meta_table_name} "
                "(hash TEXT PRIMARY KEY, pickled BLOB, value_type TEXT)"
            )

            self.__connection.executemany(
                f"INSERT OR IGNORE INTO {self.__meta_table_name} (hash, pickled, value_type) "
                f"VALUES (?, ?, ?)",
                [
                    (hash_val, sqlite3.Binary(pickled), value_type)
                    for hash_val, pickled, value_type in meta_columns
                ],
            )

    def update(self, data):
        ids_grouped_by_common_key_hashes = {}

        for i, _id in enumerate(data.keys()):
            ordered_key_hashes = tuple(
                key_hash
                for key, key_hash in self.__original_key_to_key_hash.items()
                if key in data[_id]
            )

            if ordered_key_hashes not in ids_grouped_by_common_key_hashes:
                ids_grouped_by_common_key_hashes[ordered_key_hashes] = []

            ids_grouped_by_common_key_hashes[ordered_key_hashes].append(_id)

        with self.__connection:
            updated_at = time.time()

            for (
                ordered_key_hashes,
                ids_group,
            ) in ids_grouped_by_common_key_hashes.items():
                transactions = []

                all_columns = ("id", "updated_at") + ordered_key_hashes

                columns = ", ".join([f'"{h}"' for h in all_columns])
                values = ", ".join(["?" for _ in range(len(all_columns))])
                updates = ", ".join(
                    [f'"{f}" = excluded."{f}"' for f in all_columns if f != "id"]
                )

                sql = f"INSERT INTO {self.name} ({columns}) VALUES ({values}) ON CONFLICT(id) DO UPDATE SET {updates}"

                for _id in ids_group:
                    data[_id] = defined_serializers.serialize_record(
                        self.__original_key_to_key_hash,
                        self.schema,
                        data[_id],
                        self.__compressor,
                    )

                    data[_id]["id"] = _id
                    data[_id]["updated_at"] = updated_at

                    transactions.append([data[_id][key] for key in all_columns])

                self.__connection.executemany(sql, transactions)

    def get(self, ids, select_keys=None, update=None):
        if isinstance(ids, str):
            ids = [ids]

        if not select_keys:
            select_keys_hashes = list(self.__original_key_to_key_hash.values())
        else:
            if [k for k in select_keys if k not in self.__original_key_to_key_hash]:
                raise ValueError(
                    f"Invalid select_keys: {[k for k in select_keys if k not in self.__original_key_to_key_hash]}"
                )

            select_keys_hashes = [
                self.__original_key_to_key_hash[k] for k in select_keys
            ]

        # Prepare the SQL command
        columns = ", ".join([f'"{h}"' for h in select_keys_hashes])
        column_str = "id, " + columns  # Update this to include `id`

        # Format the ids for the where clause
        id_placeholders = ", ".join(["?" for _ in ids])

        if update:
            update = defined_serializers.serialize_record(
                self.__original_key_to_key_hash, self.schema, update, self.__compressor
            )

            update_columns = ", ".join([f'"{h}" = ?' for h in update.keys()])

            sql_query = f"UPDATE {self.name} SET {update_columns} WHERE id IN ({id_placeholders}) RETURNING {', '.join(['id'] + select_keys_hashes)}"

            sql_params = [_ for _ in update.values()] + ids

            _result = self.__connection.execute(sql_query, sql_params).fetchall()
            self.__connection.commit()

        else:
            sql_query = (
                f"SELECT {column_str} FROM {self.name} WHERE id IN ({id_placeholders})"
            )
            _result = self.__connection.execute(sql_query, ids).fetchall()

        result = {}
        for row in _result:
            result[row[0]] = defined_serializers.deserialize_record(
                self.__key_hash_to_original_key,
                self.__hashed_key_schema,
                {h: val for h, val in zip(select_keys_hashes, row[1:])},
                self.__decompressor,
            )

        return result

    def clear(self):
        # CLEAR function: deletes the content of the table but keeps the table itself and the metadata table
        with self.__connection:
            self.__connection.execute(f"DROP TABLE IF EXISTS {self.name}")
            self.__create_table_and_meta_table()

    def drop(self):
        # DROP function: deletes both the table itself and the metadata table
        with self.__connection:
            self.__connection.execute(f"DROP TABLE IF EXISTS {self.name}")
            self.__connection.execute(f"DROP TABLE IF EXISTS {self.__meta_table_name}")

    def search(
        self,
        query={},
        sort_by=None,
        reversed_sort=False,
        n=None,
        page_no=None,
        select_keys=[],
        update=None,
    ):
        if sort_by:
            if sort_by not in self.schema or self.schema[sort_by] in {
                "json",
            }:
                raise ValueError("Invalid sort_by")

            elif self.schema[sort_by] in {"blob", "other"}:
                sort_by = f"__size_{self.__original_key_to_key_hash[sort_by]}"
            else:
                sort_by = self.__original_key_to_key_hash[sort_by]

        if not select_keys:
            select_keys = list(self.__original_key_to_key_hash)

        select_keys_hashes = [self.__original_key_to_key_hash[k] for k in select_keys]

        _query = {}
        for k, v in query.items():
            if k not in self.schema:
                raise ValueError(f"Invalid query: {k} is not a valid key")
            if self.schema[k] == "other":
                _query[
                    f"__hash_{self.__original_key_to_key_hash[k]}"
                ] = defined_serializers.hash_bytes(
                    pickle.dumps(v, protocol=pickle.HIGHEST_PROTOCOL)
                )
            elif self.schema[k] == "blob":
                _query[
                    f"__hash_{self.__original_key_to_key_hash[k]}"
                ] = defined_serializers.hash_bytes(v)
            elif self.schema[k] == "compressed_string":
                _query[self.__original_key_to_key_hash[k]] = sqlite3.Binary(
                    self.__compressor.compress(v.encode())
                    if self.__compressor is not False
                    else v.encode()
                )
            else:
                _query[self.__original_key_to_key_hash[k]] = v

        sql_query, sql_params = search_query(
            table_name=self.name,
            query=_query,
            schema=self.__hashed_key_schema,
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
            update = defined_serializers.serialize_record(
                self.__original_key_to_key_hash, self.schema, update, self.__compressor
            )

            update_columns = ", ".join([f'"{h}" = ?' for h in update.keys()])

            sql_query = f"UPDATE {self.name} SET {update_columns} WHERE id IN ({sql_query}) RETURNING {', '.join(['id', 'updated_at'] + select_keys_hashes)}"

            sql_params = [_ for _ in update.values()] + sql_params

            _results = self.__connection.execute(sql_query, sql_params).fetchall()

            self.__connection.commit()

        else:
            _results = self.__connection.execute(sql_query, sql_params).fetchall()

        results = {}

        for result in _results:
            _id, updated_at = result[:2]
            results[_id] = defined_serializers.deserialize_record(
                self.__key_hash_to_original_key,
                self.__hashed_key_schema,
                {h: val for h, val in zip(select_keys_hashes, result[2:])},
                self.__decompressor,
            )

        return results

    def distinct(self, key, query={}):
        sql_query, sql_params = distinct_query(
            table_name=self.name,
            column=self.__original_key_to_key_hash[key],
            query={self.__original_key_to_key_hash[k]: v for k, v in query.items()},
            schema=self.__hashed_key_schema,
        )

        return {
            _[0] for _ in self.__connection.execute(sql_query, sql_params).fetchall()
        }

    def group(self, keys, query={}):
        if isinstance(keys, str):
            keys = [keys]

        sql_query, sql_params = group_by_query(
            table_name=self.name,
            columns=[self.__original_key_to_key_hash[key] for key in keys],
            query={self.__original_key_to_key_hash[k]: v for k, v in query.items()},
            schema=self.__hashed_key_schema,
        )

        return {
            _[0]: _[1].split(chr(31))
            for _ in self.__connection.execute(sql_query, sql_params).fetchall()
        }

    def pop(self, ids=None, query={}, n=1, sort_by=None, reversed_sort=False):
        if ids is not None:
            with self.__connection:
                return {
                    row[0]: defined_serializers.deserialize_record(
                        self.__key_hash_to_original_key,
                        self.__hashed_key_schema,
                        {
                            h: val
                            for h, val in zip(self.__column_names, row[2:])
                            if h in self.__key_hash_to_original_key
                        },
                        self.__decompressor,
                    )
                    for row in self.__connection.execute(
                        f"DELETE FROM {self.name} WHERE id IN ({', '.join(['?' for _ in ids])}) RETURNING *",
                        ids,
                    ).fetchall()
                }

        elif query is not None:
            sql_query, sql_params = pop_query(
                table_name=self.name,
                query={self.__original_key_to_key_hash[k]: v for k, v in query.items()},
                schema=self.__hashed_key_schema,
                sort_by=sort_by if sort_by is not None else "updated_at",
                reversed_sort=reversed_sort,
                n=n,
            )

            with self.__connection:
                return {
                    row[0]: self.deserialize_record(
                        self.__key_hash_to_original_key,
                        self.__hashed_key_schema,
                        {
                            h: val
                            for h, val in zip(self.__column_names, row[2:])
                            if h in self.__key_hash_to_original_key
                        },
                        self.__decompressor,
                    )
                    for row in self.__connection.execute(
                        sql_query, sql_params
                    ).fetchall()
                }

        else:
            raise ValueError("Either ids or query must be provided")

    def delete(self, ids=None, query=None):
        if query is not None:
            sql_query, sql_params = delete_query(
                table_name=self.name,
                query={self.__original_key_to_key_hash[k]: v for k, v in query.items()},
                schema=self.__hashed_key_schema,
            )

            self.__connection.execute(sql_query, sql_params)
            self.__connection.commit()
        elif ids:
            if isinstance(ids, str):
                ids = [ids]

            placeholders = ", ".join(["?" for _ in ids])
            sql_query = f"DELETE FROM {self.name} WHERE id IN ({placeholders})"
            self.__connection.execute(sql_query, ids)
            self.__connection.commit()
        else:
            raise ValueError("Either ids or query must be provided")

    def count(self, query={}):
        sql_query, sql_params = count_query(
            table_name=self.name,
            query={self.__original_key_to_key_hash[k]: v for k, v in query.items()},
            schema=self.__hashed_key_schema,
        )

        return self.__connection.execute(sql_query, sql_params).fetchone()[0]

    def optimize_key_for_querying(self, key, is_unique=False):
        if self.schema[key] in {"string", "number", "boolean", "datetime"}:
            key_hash = self.__original_key_to_key_hash[key]
            if not is_unique:
                self.__connection.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{self.name}_{key_hash} ON {self.name} ({key_hash})"
                )
            else:
                self.__connection.execute(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{self.name}_{key_hash} ON {self.name} ({key_hash})"
                )

            self.__connection.commit()
        else:
            raise ValueError(
                f"Cannot optimize for querying on {key}. Only string, number, boolean and datetime types are supported"
            )

    def list_optimized_keys(self):
        return {
            k: v
            for k, v in {
                self.__key_hash_to_original_key.get(
                    _[1].replace(f"idx_{self.name}_", "")
                ): {"is_unique": bool(_[2])}
                for _ in self.__connection.execute(
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
            column=self.__original_key_to_key_hash[key],
            query={self.__original_key_to_key_hash[k]: v for k, v in query.items()},
            schema=self.__hashed_key_schema,
        )

        return self.__connection.execute(sql_query, sql_params).fetchone()[0]

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

        self.__connection.execute(trigger_sql)

    def list_triggers(self, table_name=None):
        if table_name:
            result = self.__connection.execute(
                f"SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = '{table_name}';"
            )
        else:
            result = self.__connection.execute(
                f"SELECT name FROM sqlite_master WHERE type = 'trigger';"
            )
        return result.fetchall()

    def delete_trigger(self, trigger_name):
        self.__connection.execute(f"DROP TRIGGER {trigger_name};")

    def vaccum(self):
        self.__connection.execute("VACUUM")
        self.__connection.commit()

    def export(self, format, ids, query=None, select_keys=None, file_path=None):
        if format not in {"json", "jsonl", "csv", "df"}:
            raise ValueError("Invalid format, can be one of json, jsonl, csv, df.")
