from . import common_utils
from . import defined_serializers

common_utils.set_ulimit()

import re
import os
import time
import json
import uuid
import pickle

try:
    import sqlean as sqlite3
except:
    import sqlite3

import datetime

try:
    import zstandard
except:
    zstandard = None

try:
    import faiss
    import numpy as np
except:
    faiss = None

from .query_parser import (
    search_query,
    distinct_query,
    distinct_count_query,
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
        self, name, schema=None, db_path=None, ram_cache_mb=64, compression_level=-1
    ):
        if sqlite3.sqlite_version < "3.35.0":
            raise ValueError(
                "SQLite version must be at least 3.35.0. `pip install sqlean.py` or update your python to newer version"
            )

        self.name = name
        self.schema = schema
        self.db_path = ":memory:" if db_path is None else db_path
        self.ram_cache_mb = ram_cache_mb
        self.compression_level = compression_level

        if self.name.startswith("__"):
            raise ValueError("Index name cannot start with '__'")

        self.__meta_table_name = f"__{self.name}_meta"
        self.__column_names = ["id", "updated_at"]

        self.__vector_search_indexes = {}
        self.__vector_indexes_last_updated_at = {}

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

        self.__meta_schema = self.schema.copy()
        self.__meta_schema["updated_at"] = "number"
        self.__meta_schema["integer_id"] = "number"

    def __update_vector_search_index(self, for_key, dim=None):
        if for_key not in self.__vector_indexes_last_updated_at:
            try:
                self.__vector_search_indexes[for_key] = faiss.IndexIDMap(
                    faiss.IndexFlatIP(dim)
                )
            except ImportError:
                raise ValueError(
                    "`pip install faiss-cpu` or `pip install liteindex[all]`"
                )

            self.__vector_indexes_last_updated_at[for_key] = 0

        newest_updated_at_time = self.__connection.execute(
            f"SELECT MAX(updated_at) FROM {self.name}"
        ).fetchone()[0]

        if newest_updated_at_time is None:
            return

        if self.__vector_indexes_last_updated_at[for_key] >= newest_updated_at_time:
            return

        embeddings_batch = []
        integer_id_batch = []

        for __row in self.__connection.execute(
            f"SELECT integer_id, {for_key} FROM {self.name} WHERE updated_at > {self.__vector_indexes_last_updated_at[for_key]} AND updated_at <= {newest_updated_at_time} AND {for_key} IS NOT NULL"
        ):
            embeddings_batch.append(__row[1])
            integer_id_batch.append(__row[0])

            if len(integer_id_batch) >= 10000:
                integer_id_batch = np.array(integer_id_batch, dtype=np.int64)
                self.__vector_search_indexes[for_key].remove_ids(integer_id_batch)

                self.__vector_search_indexes[for_key].add_with_ids(
                    np.frombuffer(b"".join(embeddings_batch), dtype=np.float32).reshape(
                        len(integer_id_batch), dim
                    ),
                    integer_id_batch,
                )
                embeddings_batch = []
                integer_id_batch = []

        if len(integer_id_batch) > 0:
            integer_id_batch = np.array(integer_id_batch, dtype=np.int64)
            self.__vector_search_indexes[for_key].remove_ids(integer_id_batch)

            self.__vector_search_indexes[for_key].add_with_ids(
                np.frombuffer(b"".join(embeddings_batch), dtype=np.float32).reshape(
                    len(integer_id_batch), dim
                ),
                integer_id_batch,
            )

            embeddings_batch = []
            integer_id_batch = []

        self.__vector_indexes_last_updated_at[for_key] = newest_updated_at_time

    def __get_scores_and_integer_ids_table_name(
        self,
        sort_by_embedding,
        key_name,
        sort_by_embedding_min_similarity,
    ):
        sort_by_embedding = np.array(sort_by_embedding, dtype=np.float32).reshape(1, -1)

        for try_n in range(1, 11):
            n_vecs_to_search = max(
                (self.__vector_search_indexes[key_name].ntotal * try_n) // 10, 1
            )

            scores, integer_ids = self.__vector_search_indexes[key_name].search(
                sort_by_embedding,
                n_vecs_to_search,
            )

            if (
                scores[0][-1] < sort_by_embedding_min_similarity
            ) or n_vecs_to_search >= self.__vector_search_indexes[key_name].ntotal:
                break

        integer_ids = integer_ids[0]
        scores = scores[0]

        scores = scores[scores >= sort_by_embedding_min_similarity].tolist()
        integer_ids = integer_ids[: len(scores)].tolist()

        with self.__connection as conn:
            _temp_name = f"temp_embeds_{uuid.uuid4().hex}"

            conn.execute(
                f"CREATE TEMP TABLE {_temp_name} (_integer_id INTEGER PRIMARY KEY, score NUMBER)"
            )
            _temp_name = f"temp.{_temp_name}"

            conn.executemany(
                f"INSERT INTO {_temp_name} (_integer_id, score) VALUES (?, ?)",
                zip(integer_ids, scores),
            )

            return _temp_name

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

            self.__local_storage.db_conn.execute(f"PRAGMA BUSY_TIMEOUT=60000")

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

            for key, value_type in rows:
                self.schema[key] = value_type
        else:
            for key, value_type in rows:
                if key not in self.schema:
                    raise ValueError(f"Schema mismatch: {key} not found in schema")
                if self.schema[key] != value_type:
                    raise ValueError(
                        f"Schema mismatch: {key} has type {value_type} but {self.schema[key]} was expected"
                    )

            if len(self.schema) != len(rows):
                raise ValueError("existing schema does not match the provided schema")

    def __parse_schema(self):
        for _i, (key, value_type) in enumerate(self.schema.items()):
            if value_type not in defined_serializers.schema_property_to_column_type:
                raise ValueError(f"Invalid schema property: {value_type}")

    def __create_table_and_meta_table(self):
        columns = []
        meta_columns = []

        for key, value_type in self.schema.items():
            sql_column_type = defined_serializers.schema_property_to_column_type[
                value_type
            ]

            if value_type in {"blob", "other"}:
                columns.append(f'"__size_{key}" NUMBER')
                self.__column_names.append(f"__size_{key}")
                columns.append(f'"__hash_{key}" TEXT')
                self.__column_names.append(f"__hash_{key}")

            columns.append(f'"{key}" {sql_column_type}')
            self.__column_names.append(key)

            meta_columns.append((key, value_type))

        columns_str = ", ".join(columns)

        with self.__connection:
            self.__connection.execute(
                f"CREATE TABLE IF NOT EXISTS {self.name} (integer_id INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE, updated_at NUMBER, {columns_str})"
            )

            self.__connection.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.name}_updated_at ON {self.name} (updated_at)"
            )

            self.__connection.execute(
                f"CREATE TABLE IF NOT EXISTS {self.__meta_table_name} "
                "(key TEXT PRIMARY KEY, value_type TEXT)"
            )

            self.__connection.executemany(
                f"INSERT OR IGNORE INTO {self.__meta_table_name} (key, value_type) "
                f"VALUES (?, ?)",
                [(key, value_type) for key, value_type in meta_columns],
            )

    def update(self, data):
        ids_grouped_by_common_keys = {}

        for _id, _data in data.items():
            data[_id] = defined_serializers.serialize_record(
                self.schema, data[_id], self.__compressor, _id, time.time()
            )

            keys_in_current_data = tuple(data[_id].keys())

            if keys_in_current_data not in ids_grouped_by_common_keys:
                ids_grouped_by_common_keys[keys_in_current_data] = [_id]
            else:
                ids_grouped_by_common_keys[keys_in_current_data].append(_id)

        with self.__connection:
            updated_at = time.time()

            for (
                keys_in_current_data,
                ids_group,
            ) in ids_grouped_by_common_keys.items():
                columns = ", ".join([f'"{h}"' for h in keys_in_current_data])
                values = ", ".join(["?" for _ in range(len(keys_in_current_data))])
                updates = ", ".join(
                    [
                        f'"{f}" = excluded."{f}"'
                        for f in keys_in_current_data
                        if f != "id"
                    ]
                )

                sql = f"INSERT INTO {self.name} ({columns}) VALUES ({values}) ON CONFLICT(id) DO UPDATE SET {updates}"

                def yield_transaction():
                    for _id in ids_group:
                        yield tuple(data[_id].values())

                self.__connection.executemany(sql, yield_transaction())

    def get(
        self,
        ids,
        select_keys=None,
        update=None,
        return_metadata=False,
        metadata_key_name="__meta",
    ):
        if isinstance(ids, str):
            ids = [ids]

        if select_keys is None:
            select_keys = self.schema
        elif not select_keys:
            select_keys = []

        if not return_metadata:
            select_keys = tuple(select_keys)
        else:
            select_keys = ("integer_id", "updated_at") + tuple(select_keys)

        columns = ", ".join([f'"{h}"' for h in select_keys])
        column_str = "id, " + columns

        id_placeholders = ", ".join(["?" for _ in ids])

        if update:
            update = defined_serializers.serialize_record(
                self.schema, update, self.__compressor
            )

            update_columns = ", ".join((f'"{h}" = ?' for h in update.keys()))

            sql_query = f"UPDATE {self.name} SET {update_columns} WHERE id IN ({id_placeholders}) RETURNING {', '.join(('id') + select_keys)}"

            sql_params = tuple(update.values()) + ids

            _result = self.__connection.execute(sql_query, sql_params).fetchall()
            self.__connection.commit()

        else:
            sql_query = (
                f"SELECT {column_str} FROM {self.name} WHERE id IN ({id_placeholders})"
            )
            _result = self.__connection.execute(sql_query, ids).fetchall()

        if return_metadata:
            select_keys = select_keys[2:]

        result = {}
        for row in _result:
            result[row[0]] = defined_serializers.deserialize_record(
                self.schema,
                {
                    h: val
                    for h, val in zip(
                        select_keys, row[1:] if not return_metadata else row[3:]
                    )
                },
                self.__decompressor,
            )

            if return_metadata:
                result[row[0]][metadata_key_name] = {
                    "integer_id": row[1],
                    "updated_at": row[2],
                }

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
        offset=None,
        page_no=None,
        select_keys=None,
        update=None,
        return_metadata=False,
        metadata_key_name="__meta",
        sort_by_embedding=None,
        sort_by_embedding_min_similarity=0,
        meta_query={},
    ):
        if page_no is not None:
            offset = (page_no - 1) * n

        if not sort_by:
            sort_by = "updated_at"

        elif self.schema[sort_by] in {"blob", "other"}:
            sort_by = f"__size_{sort_by}"

        sorting_by_vector = False

        if self.schema.get(sort_by) == "normalized_embedding":
            if sort_by_embedding is None:
                raise ValueError("sort_by_embedding must be provided")

            self.__update_vector_search_index(sort_by, len(sort_by_embedding))

            sorting_by_vector = True

            integer_ids_to_scores_table_name = (
                self.__get_scores_and_integer_ids_table_name(
                    sort_by_embedding, sort_by, sort_by_embedding_min_similarity
                )
            )

        if select_keys is None:
            select_keys = self.schema

        elif not select_keys:
            select_keys = []

        select_keys = tuple(select_keys)

        if meta_query:
            query.update(meta_query)

        sql_query, sql_params = search_query(
            table_name=self.name,
            query=query,
            schema=self.__meta_schema,
            sort_by=sort_by,
            reversed_sort=reversed_sort,
            n=n,
            offset=offset,
            select_columns=(
                ("integer_id", "id", "updated_at")
                + (() if sort_by_embedding is None else ("score",))
                + select_keys
            )
            if not update
            else ["id"],
            for_sorting_integer_ids_to_scores_table_name=integer_ids_to_scores_table_name
            if sorting_by_vector
            else None,
        )

        _results = None

        if update:
            update = defined_serializers.serialize_record(
                self.schema, update, self.__compressor
            )

            update_columns = ", ".join([f'"{h}" = ?' for h in update.keys()])

            sql_query = f"UPDATE {self.name} SET {update_columns} WHERE id IN ({sql_query}) RETURNING {', '.join(('integer_id', 'id', 'updated_at') + select_keys)}"

            sql_params = [_ for _ in update.values()] + sql_params

            with self.__connection as conn:
                _results = conn.execute(sql_query, sql_params).fetchall()

        else:
            _results = self.__connection.execute(sql_query, sql_params).fetchall()

        if sorting_by_vector:
            self.__connection.execute(
                f"DROP TABLE IF EXISTS {integer_ids_to_scores_table_name}"
            )

        results = {}

        for result in _results:
            if sorting_by_vector:
                integer_id, _id, updated_at, score = result[:4]
            else:
                integer_id, _id, updated_at = result[:3]

            results[_id] = defined_serializers.deserialize_record(
                self.schema,
                {
                    h: val
                    for h, val in zip(
                        select_keys, result[3:] if not sorting_by_vector else result[4:]
                    )
                },
                self.__decompressor,
            )

            if return_metadata:
                results[_id][metadata_key_name] = {
                    "integer_id": integer_id,
                    "updated_at": updated_at,
                }

                if sorting_by_vector:
                    results[_id][metadata_key_name]["sort_score"] = score

        return results

    def distinct(self, key, query={}):
        sql_query, sql_params = distinct_query(
            table_name=self.name,
            column=key,
            query={k: v for k, v in query.items()},
            schema=self.schema,
        )

        return {
            _[0] for _ in self.__connection.execute(sql_query, sql_params).fetchall()
        }

    def distinct_count(self, key, query={}):
        sql_query, sql_params = distinct_count_query(
            table_name=self.name,
            column=key,
            query={k: v for k, v in query.items()},
            schema=self.schema,
        )

        return {
            _[0]: _[1]
            for _ in self.__connection.execute(sql_query, sql_params).fetchall()
        }

    def group(self, keys, query={}):
        if isinstance(keys, str):
            keys = [keys]

        sql_query, sql_params = group_by_query(
            table_name=self.name,
            columns=[key for key in keys],
            query={k: v for k, v in query.items()},
            schema=self.schema,
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
                        self.schema,
                        {
                            h: val
                            for h, val in zip(self.__column_names, row[2:])
                            if h in self.schema
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
                query={k: v for k, v in query.items()},
                schema=self.schema,
                sort_by=sort_by if sort_by is not None else "updated_at",
                reversed_sort=reversed_sort,
                n=n,
            )

            with self.__connection:
                return {
                    row[0]: defined_serializers.deserialize_record(
                        self.schema,
                        {
                            h: val
                            for h, val in zip(self.__column_names, row[2:])
                            if h in self.schema
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
                query={k: v for k, v in query.items()},
                schema=self.schema,
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
            query={k: v for k, v in query.items()},
            schema=self.schema,
        )

        return self.__connection.execute(sql_query, sql_params).fetchone()[0]

    def optimize_for_query(self, keys, is_unique=False):
        if isinstance(keys, str):
            keys = [keys]

        key_hashes = []
        size_hashes = []

        for k in keys:
            if self.schema[k] in {"blob", "other"}:
                key_hashes.append(f"__hash_{k}")
                size_hashes.append(f"__size_{k}")
            elif self.schema[k] == "json":
                pass
            else:
                key_hashes.append(k)

        if key_hashes:
            self.__connection.execute(
                f"CREATE {'UNIQUE' if is_unique else ''} INDEX IF NOT EXISTS idx_{self.name}_{'_'.join(key_hashes)} ON {self.name} ({','.join(key_hashes)})"
            )

            for size_hash in size_hashes:
                self.__connection.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{self.name}_{size_hash} ON {self.name} ({size_hash})"
                )

            self.__connection.commit()

    def list_optimized_keys(self):
        return {
            k: v
            for k, v in {
                _[1].replace(f"idx_{self.name}_", ""): {"is_unique": bool(_[2])}
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
            column=key,
            query={k: v for k, v in query.items()},
            schema=self.schema,
        )

        return self.__connection.execute(sql_query, sql_params).fetchone()[0]

    def create_trigger(
        self,
        trigger_name,
        for_key,
        function_to_trigger,
        before_delete=False,
        after_set=False,
        before_set=False,
        for_keys=None,
    ):
        if after_set:
            operation = "INSERT"
            timing = "AFTER"
        elif before_set:
            operation = "INSERT"
            timing = "BEFORE"
        elif before_delete:
            operation = "DELETE"
            timing = "BEFORE"
        else:
            raise ValueError("before_delete or after_set must be True")

        if for_keys is None:
            for_keys = [for_key]

        for_keys = [self.__co]

        function_name = f"{trigger_name}_function"

        with self.__connection as conn:
            conn.create_function(function_name, 1, function_to_trigger)

            trigger_sql = f"""
            CREATE TRIGGER IF NOT EXISTS {trigger_name}
            {timing} {operation} ON {self.name}

            WHEN NEW.

            BEGIN
                {function_name}(NEW.{for_key});
            END;
            """

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
