from .common_utils import set_ulimit, EvictionCfg
from .kv_index_utils import create_tables, create_where_clause

set_ulimit()

import os
import time
import pickle
import hashlib
import sqlite3
import threading
import functools


class KVIndex:
    def __init__(
        self,
        db_path=None,
        store_key=True,
        preserve_order=True,
        ram_cache_mb=32,
        eviction=EvictionCfg(EvictionCfg.EvictNone),
    ):
        self.store_key = store_key
        self.eviction = eviction
        self.db_path = db_path if db_path is not None else ":memory:"

        os.makedirs(os.path.dirname(self.db_path), exist_ok=True) if os.path.dirname(
            self.db_path
        ) else None

        self.ram_cache_mb = ram_cache_mb
        self.preserve_order = (
            preserve_order or self.eviction.policy == EvictionCfg.EvictFIFO
        )

        self.__local_storage = threading.local()

        with self.__connection as conn:
            create_tables(
                store_key=self.store_key,
                preserve_order=self.preserve_order,
                eviction=self.eviction,
                conn=conn,
            )

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

    def __current_time(self):
        return int(time.time() * 100000)

    def __setitem__(self, key, value):
        self.update({key: value})

    def __getitem__(self, key):
        key = self.__encode_and_hash(key, return_encoded_key=False)[0]

        if self.eviction.policy in {EvictionCfg.EvictAny, EvictionCfg.EvictNone}:
            row = self.__connection.execute(
                "SELECT num_value, string_value, pickled_value FROM kv_index WHERE key_hash = ?",
                (key,),
            ).fetchone()
        else:
            with self.__connection as conn:
                if self.eviction.policy == EvictionCfg.EvictLRU:
                    row = conn.execute(
                        "UPDATE kv_index SET last_accessed_time = ? WHERE key_hash = ? RETURNING num_value, string_value, pickled_value",
                        (self.__current_time(), key),
                    ).fetchone()
                elif self.eviction.policy == EvictionCfg.EvictLFU:
                    row = conn.execute(
                        "UPDATE kv_index SET access_frequency = access_frequency + 1 WHERE key_hash = ? RETURNING num_value, string_value, pickled_value",
                        (key,),
                    ).fetchone()
                elif self.eviction.policy == EvictionCfg.EvictAny:
                    row = conn.execute(
                        "UPDATE kv_index SET updated_at = ? WHERE key_hash = ? RETURNING num_value, string_value, pickled_value",
                        (self.__current_time(), key),
                    ).fetchone()

        if row is None:
            raise KeyError

        return self.__decode_value(row)

    def getvalues(self, keys, default=None):
        keys = [self.__encode_and_hash(key)[0] for key in keys]

        if self.eviction.policy in {EvictionCfg.EvictAny, EvictionCfg.EvictNone}:
            rows = self.__connection.execute(
                f"SELECT key_hash, num_value, string_value, pickled_value FROM kv_index WHERE key_hash IN ({', '.join(['?'] * len(keys))})",
                keys,
            ).fetchall()

        else:
            with self.__connection as conn:
                if self.eviction.policy == EvictionCfg.EvictLRU:
                    rows = conn.execute(
                        f"UPDATE kv_index SET last_accessed_time = ? WHERE key_hash IN ({', '.join(['?'] * len(keys))}) RETURNING key_hash, num_value, string_value, pickled_value",
                        (self.__current_time(), *keys),
                    ).fetchall()
                elif self.eviction.policy == EvictionCfg.EvictLFU:
                    rows = conn.execute(
                        f"UPDATE kv_index SET access_frequency = access_frequency + 1 WHERE key_hash IN ({', '.join(['?'] * len(keys))}) RETURNING key_hash, num_value, string_value, pickled_value",
                        keys,
                    ).fetchall()
                elif self.eviction.policy == EvictionCfg.EvictAny:
                    rows = conn.execute(
                        f"UPDATE kv_index SET updated_at = ? WHERE key_hash IN ({', '.join(['?'] * len(keys))}) RETURNING key_hash, num_value, string_value, pickled_value",
                        (self.__current_time(), *keys),
                    ).fetchall()

        if rows is None:
            raise KeyError

        rows = {row[0]: self.__decode_value(row[1:]) for row in rows}

        return [rows.get(key_hash, default) for key_hash in keys]

    def items(self, reverse=False):
        if not self.store_key:
            raise Exception("Cannot iterate over items when store_key is False")

        if not self.preserve_order and reverse:
            raise Exception(
                "Cannot iterate over items in reverse when preserve_order is False"
            )

        sql = f"SELECT key_hash, pickled_key, num_value, string_value, pickled_value FROM kv_index {'ORDER BY updated_at' if self.preserve_order else ''} {'DESC' if reverse else ''}"

        for row in self.__connection.execute(sql):
            if row is None:
                break

            yield self.__decode_key(row[1], row[0]), self.__decode_value(row[2:])

    def keys(self, reverse=False):
        if not self.store_key:
            raise Exception("Cannot iterate over items when store_key is False")

        if not self.preserve_order and reverse:
            raise Exception(
                "Cannot iterate over items in reverse when preserve_order is False"
            )

        sql = f"SELECT key_hash, pickled_key FROM kv_index {'ORDER BY updated_at' if self.preserve_order else ''} {'DESC' if reverse else ''}"

        for row in self.__connection.execute(sql):
            if row is None:
                break

            yield self.__decode_key(row[1], row[0])

    def values(self, reverse=False):
        if not self.preserve_order and reverse:
            raise Exception(
                "Cannot iterate over items in reverse when preserve_order is False"
            )

        sql = f"SELECT num_value, string_value, pickled_value FROM kv_index {'ORDER BY updated_at' if self.preserve_order else ''} {'DESC' if reverse else ''}"

        for row in self.__connection.execute(sql):
            if row is None:
                break

            value = row

            yield self.__decode_value(value)

    def __len__(self):
        return self.__connection.execute("SELECT COUNT(*) FROM kv_index").fetchone()[0]

    def __contains__(self, key):
        if self.__connection.execute(
            "SELECT COUNT(*) FROM kv_index WHERE key_hash = ?",
            (self.__encode_and_hash(key)[0],),
        ).fetchone()[0]:
            return True

        return False

    def __delitem__(self, key):
        self.delete([key])

    def delete(self, keys):
        key_hashes = [self.__encode_and_hash(key)[0] for key in keys]

        with self.__connection as conn:
            if self.eviction.max_size_in_mb:
                sizes = conn.execute(
                    f"DELETE FROM kv_index WHERE key_hash IN ({', '.join(['?'] * len(key_hashes))}) RETURNING size_in_bytes",
                    key_hashes,
                ).fetchall()

                if sizes:
                    conn.execute(
                        "UPDATE kv_index_num_metadata SET num = num - ? WHERE key = ?",
                        (
                            sum([size[0] for size in sizes]) / (1024 * 1024),
                            "current_size_in_mb",
                        ),
                    )
                else:
                    raise KeyError
            else:
                conn.execute(
                    f"DELETE FROM kv_index WHERE key_hash IN ({', '.join(['?'] * len(key_hashes))})",
                    key_hashes,
                )

    def pop(self, key):
        with self.__connection as conn:
            # assume delete from returning query is suported and write a single query that returns the value, size_in_bytes and deletes the row
            if self.eviction.max_size_in_mb:
                row = conn.execute(
                    f"DELETE FROM kv_index WHERE key_hash = ? RETURNING pickled_value, size_in_bytes",
                    (self.__encode_and_hash(key)[0]),
                ).fetchone()

                if row is None:
                    raise KeyError

                conn.execute(
                    "UPDATE kv_index_num_metadata SET num = num - ? WHERE key = ?",
                    (
                        row[1] / (1024 * 1024),
                        "current_size_in_mb",
                    ),
                )

                return self.__decode_value(row[0])
            else:
                row = conn.execute(
                    f"DELETE FROM kv_index WHERE key_hash = ? RETURNING pickled_value",
                    (self.__encode_and_hash(key)[0]),
                ).fetchone()

                if row is None:
                    raise KeyError

                return self.__decode_value(row[0])

    def popitems(self, n=1, reverse=True):
        with self.__connection as conn:
            if self.eviction.max_size_in_mb:
                rows = conn.execute(
                    f"DELETE FROM kv_index WHERE ROWID IN (SELECT ROWID FROM kv_index ORDER BY updated_at {'DESC' if reverse else ''} LIMIT {n}) RETURNING pickled_key, pickled_value, size_in_bytes",
                ).fetchall()

                if rows is None:
                    raise KeyError

                conn.execute(
                    "UPDATE kv_index_num_metadata SET num = num - ? WHERE key = ?",
                    (
                        sum([row[2] for row in rows]) / (1024 * 1024),
                        "current_size_in_mb",
                    ),
                )

                return [
                    (self.__decode_key(row[0], None), self.__decode_value(row[1]))
                    for row in rows
                ]
            else:
                rows = conn.execute(
                    f"DELETE FROM kv_index WHERE ROWID IN (SELECT ROWID FROM kv_index ORDER BY updated_at {'DESC' if reverse else ''} LIMIT {n}) RETURNING pickled_key, pickled_value",
                ).fetchall()

                if rows is None:
                    raise KeyError

                return [
                    (self.__decode_key(row[0], None), self.__decode_value(row[1]))
                    for row in rows
                ]

    def __iter__(self):
        return self.keys()

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __encode_and_hash(self, x, return_encoded_key=False):
        x = (
            pickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
            if not isinstance(x, str)
            else x.encode()
        )
        _len = len(x)

        if len(x) <= 32:
            return sqlite3.Binary(x), None
        else:
            return sqlite3.Binary(hashlib.sha256(x).digest()), (
                None if not return_encoded_key else sqlite3.Binary(x)
            )

    def __decode_key(self, k, k_h):
        if k is None:
            k = k_h

        return (
            pickle.loads(k)
            if k and k[0] == 128
            else k.decode()
            if k is not None
            else None
        )

    def __encode_value(self, x):
        num_value, string_value, pickled_value = None, None, None
        value_size_in_bytes = 3

        _type = type(x)

        if x is None:
            pass

        elif type(x) is bool:
            value_size_in_bytes = 10
            num_value = int(x)
            pickled_value = sqlite3.Binary(b"")

        elif _type in {int, float}:
            value_size_in_bytes = 10
            num_value = x

        elif _type is str:
            value_size_in_bytes = len(x) + 2
            string_value = x

        elif _type is bytes:
            value_size_in_bytes = len(x) + 2
            pickled_value = sqlite3.Binary(x)

        else:
            pickled_value = pickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
            value_size_in_bytes = len(pickled_value) + 2
            pickled_value = sqlite3.Binary(pickled_value)

        return num_value, string_value, pickled_value, value_size_in_bytes

    def __decode_value(self, x):
        if x[0] is not None:
            return x[0] if x[2] is None else bool(x[0])
        elif x[1] is not None:
            return x[1]
        elif x[2] is not None:
            return pickle.loads(x[2]) if x[2][0] == 128 else x[2]

    def update(self, items, reverse_order=False):
        # list of tuples of values to insert for executemany
        params_for_execute_many = []
        key_hashes = []

        total_new_size = 0

        for key, value in items.items() if isinstance(items, dict) else items:
            key_hash, _key = self.__encode_and_hash(
                key, return_encoded_key=self.store_key
            )

            (
                num_value,
                string_value,
                pickled_value,
                value_size_in_bytes,
            ) = self.__encode_value(value)

            row_size_in_bytes = value_size_in_bytes + len(key_hash)

            # num_value, string_value, pickled_value

            params_for_execute_many.append(
                [key_hash, num_value, string_value, pickled_value]
            )

            _time = self.__current_time()

            if self.store_key:
                # pickled_key
                params_for_execute_many[-1].append(_key)
                if _key is not None:
                    row_size_in_bytes += len(_key)

            if self.preserve_order:
                # updated_at
                params_for_execute_many[-1].append(
                    (-1 * _time) if reverse_order else _time
                )
                row_size_in_bytes += 8

            if self.eviction.policy == EvictionCfg.EvictLRU:
                # last_accessed_time
                params_for_execute_many[-1].append(_time)
                row_size_in_bytes += 8

            elif self.eviction.policy == EvictionCfg.EvictLFU:
                # access_frequency
                params_for_execute_many[-1].append(1)
                row_size_in_bytes += 4

            if self.eviction.max_size_in_mb:
                # size_in_bytes
                params_for_execute_many[-1].append(value_size_in_bytes)
                row_size_in_bytes += 4

            # key_hash
            key_hashes.append(key_hash)

            total_new_size += row_size_in_bytes

        with self.__connection as conn:
            self.__run_eviction(conn)

            if self.eviction.max_size_in_mb:
                total_old_size = conn.execute(
                    f"SELECT SUM(size_in_bytes) FROM kv_index WHERE key_hash IN ({', '.join(['?'] * len(key_hashes))})",
                    key_hashes,
                ).fetchone()[0]

                conn.execute(
                    "UPDATE kv_index_num_metadata SET num = num + ? WHERE key = ?",
                    (
                        (total_new_size - total_old_size) / (1024 * 1024),
                        "current_size_in_mb",
                    ),
                )

            conn.executemany(
                f"INSERT OR REPLACE INTO kv_index VALUES ({', '.join(['?'] * len(params_for_execute_many[0]))})",
                params_for_execute_many,
            )

    def search(
        self,
        query={},
        sort_by_value=False,
        reversed_sort=False,
        n=None,
        offset=None,
    ):
        if not isinstance(query, dict):
            query = {"$eq": query}

        try:
            query_str, params = create_where_clause(query)
        except:
            query_str, params = create_where_clause({"$eq": query})

        results = {}

        sort_by = "updated_at" if self.preserve_order else "ROWID"

        if sort_by_value:
            if isinstance(params[0], (int, float)):
                sort_by = "num_value"
            else:
                sort_by = "string_value"

        sort_by = f"ORDER BY {sort_by} {'DESC' if reversed_sort else ''}"

        for row in self.__connection.execute(
            f"SELECT key_hash, pickled_key, num_value, string_value, pickled_value FROM kv_index WHERE {query_str} {sort_by} LIMIT {n if n else -1} OFFSET {offset if offset else 0}",
            params,
        ):
            if row is None:
                break

            results[self.__decode_key(row[1], row[0])] = self.__decode_value(row[2:])

        return results

    def math(self, key, value, op):
        ops = {"+": "+", "-": "-", "*": "*", "/": "/", "//": "//", "%": "%", "**": "**"}
        pass

    def __run_eviction(self, conn):
        if self.eviction.policy == EvictionCfg.EvictNone:
            return

        current_number_of_rows = conn.execute(
            "SELECT COUNT(*) FROM kv_index"
        ).fetchone()[0]

        number_of_rows_to_evict = 0
        percent_to_evict = 0.2

        if self.eviction.max_size_in_mb:
            s = time.time()
            current_size_in_mb = conn.execute(
                "SELECT num FROM kv_index_num_metadata WHERE key = current_size_in_mb"
            ).fetchone()[0]

            if current_size_in_mb >= self.eviction.max_size_in_mb:
                number_of_rows_to_evict = max(
                    int(current_number_of_rows * percent_to_evict), 1
                )

        elif self.eviction.max_number_of_items:
            if current_number_of_rows >= self.eviction.max_number_of_items:
                number_of_rows_to_evict = max(
                    int(current_number_of_rows * percent_to_evict), 1
                )

        if number_of_rows_to_evict == 0:
            return

        if not self.eviction.max_size_in_mb:
            if self.eviction.policy == EvictionCfg.EvictLRU:
                conn.execute(
                    f"DELETE FROM kv_index WHERE ROWID IN (SELECT ROWID FROM kv_index ORDER BY last_accessed_time ASC LIMIT {number_of_rows_to_evict})"
                )
            elif self.eviction.policy == EvictionCfg.EvictLFU:
                conn.execute(
                    f"DELETE FROM kv_index WHERE ROWID IN (SELECT ROWID FROM kv_index ORDER BY access_frequency{', updated_at' if self.preserve_order else ''} ASC LIMIT {number_of_rows_to_evict})"
                )
            elif self.eviction.policy == EvictionCfg.EvictAny:
                conn.execute(
                    f"DELETE FROM kv_index WHERE ROWID IN (SELECT ROWID FROM kv_index LIMIT {number_of_rows_to_evict})"
                )
            elif self.eviction.policy == EvictionCfg.EvictFIFO:
                conn.execute(
                    f"DELETE FROM kv_index WHERE ROWID IN (SELECT ROWID FROM kv_index ORDER BY updated_at ASC LIMIT {number_of_rows_to_evict})"
                )
        else:
            if self.eviction.policy == EvictionCfg.EvictLRU:
                sizes = conn.execute(
                    f"DELETE FROM kv_index WHERE ROWID IN (SELECT ROWID FROM kv_index ORDER BY last_accessed_time ASC LIMIT {number_of_rows_to_evict}) RETURNING size_in_bytes"
                ).fetchall()
            elif self.eviction.policy == EvictionCfg.EvictLFU:
                sizes = conn.execute(
                    f"DELETE FROM kv_index WHERE ROWID IN (SELECT ROWID FROM kv_index ORDER BY access_frequency{', updated_at' if self.preserve_order else ''} ASC LIMIT {number_of_rows_to_evict}) RETURNING size_in_bytes"
                ).fetchall()
            elif self.eviction.policy == EvictionCfg.EvictAny:
                sizes = conn.execute(
                    f"DELETE FROM kv_index WHERE ROWID IN (SELECT ROWID FROM kv_index LIMIT {number_of_rows_to_evict}) RETURNING size_in_bytes"
                ).fetchall()
            elif self.eviction.policy == EvictionCfg.EvictFIFO:
                sizes = conn.execute(
                    f"DELETE FROM kv_index WHERE ROWID IN (SELECT ROWID FROM kv_index ORDER BY updated_at ASC LIMIT {number_of_rows_to_evict}) RETURNING size_in_bytes"
                ).fetchall()

            if sizes:
                conn.execute(
                    "UPDATE kv_index_num_metadata SET num = num - ? WHERE key = ?",
                    (
                        sum([size[0] for size in sizes]) / (1024 * 1024),
                        "current_size_in_mb",
                    ),
                )

    def __del__(self):
        if self.__connection:
            self.__connection.close()

    def vaccum(self):
        with self.__connection as conn:
            conn.execute("VACUUM")

    def popitem(self, reverse=True):
        self.popitems(n=1, reverse=reverse)[0]

    def clear(self):
        with self.__connection as conn:
            conn.execute("DELETE FROM kv_index")
            conn.execute(
                "UPDATE kv_index_num_metadata SET num = 0 WHERE key = ?",
                ("current_size_in_mb",),
            )

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

        with self.__connection as conn:
            function_name = f"{trigger_name}_function"

            if for_key:
                conn.create_function(
                    function_name, 0, function_to_trigger, deterministic=False
                )

                if operation == "INSERT":
                    trigger_sql = f"""
                        CREATE TRIGGER {trigger_name}
                        {timing} {operation} ON kv_index

                        WHEN NEW.key_hash = X'{self.__encode_and_hash(for_key)[0].hex()}'

                        BEGIN
                            SELECT {function_name}();
                        END;
                    """

                elif operation == "DELETE":
                    trigger_sql = f"""
                        CREATE TRIGGER {trigger_name}
                        {timing} {operation} ON kv_index

                        WHEN OLD.key_hash = X'{self.__encode_and_hash(for_key)[0].hex()}'

                        BEGIN
                            SELECT {function_name}();
                        END;
                    """
            elif for_keys:
                conn.create_function(
                    function_name, 1, function_to_trigger, deterministic=False
                )

                if operation == "INSERT":
                    trigger_sql = f"""
                        CREATE TRIGGER {trigger_name}
                        {timing} {operation} ON kv_index

                        WHEN NEW.key_hash IN ({', '.join([f"X'{self.__encode_and_hash(key)[0].hex()}'" for key in for_keys])})

                        BEGIN
                            SELECT {function_name}(NEW.key_hash);
                        END;
                    """

                elif operation == "DELETE":
                    trigger_sql = f"""
                        CREATE TRIGGER {trigger_name}
                        {timing} {operation} ON kv_index

                        WHEN OLD.key_hash IN ({', '.join([f"X'{self.__encode_and_hash(key)[0].hex()}'" for key in for_keys])})

                        BEGIN
                            SELECT {function_name}(OLD.key_hash);
                        END;
                    """
            else:
                raise ValueError("for_key or for_keys must be provided")

            if not conn.execute(
                f"SELECT name FROM sqlite_master WHERE type='trigger' AND name='{trigger_name}'"
            ).fetchone():
                conn.execute(trigger_sql)
