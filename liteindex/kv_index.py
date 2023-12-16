from .common_utils import set_ulimit, EvictionCfg

set_ulimit()

import os
import time
import pickle
import hashlib
import sqlite3
import threading


class KVIndex:
    def __init__(
        self,
        db_path=None,
        store_key=True,
        preserve_order=True,
        ram_cache_mb=32,
        eviction=EvictionCfg(EvictionCfg.EvictNone)
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

        columns_needed_and_sql_types = {
            "key_hash": "BLOB",
            "pickled_value": "BLOB",
        }

        if self.store_key:
            columns_needed_and_sql_types["pickled_key"] = "BLOB"
        if self.preserve_order or self.eviction.invalidate_after_seconds > 0:
            columns_needed_and_sql_types["updated_at"] = "INTEGER"

        if self.eviction.policy is EvictionCfg.EvictLRU:
            columns_needed_and_sql_types["last_accessed_time"] = "INTEGER"
        elif self.eviction.policy is EvictionCfg.EvictLFU:
            columns_needed_and_sql_types["access_frequency"] = "INTEGER"

        if self.eviction.max_size_in_mb:
            columns_needed_and_sql_types["size_in_bytes"] = "INTEGER"

        with self.__connection as conn:
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS kv_index ({','.join([f'{col} {sql_type}' for col, sql_type in columns_needed_and_sql_types.items()])}, PRIMARY KEY (key_hash))"
            )

            conn.execute(
                "CREATE TABLE IF NOT EXISTS kv_index_num_metadata (key TEXT PRIMARY KEY, num INTEGER)"
            )

            conn.execute(
                "INSERT OR IGNORE INTO kv_index_num_metadata (key, num) VALUES (?, ?)",
                ("current_size_in_mb", 0),
            )

            if self.eviction.policy is EvictionCfg.EvictLRU:
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS kv_index_last_accessed_time_idx ON kv_index(last_accessed_time)"
                )

            if self.eviction.policy is EvictionCfg.EvictLFU:
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS kv_index_access_frequency_idx ON kv_index(access_frequency)"
                )

            if self.eviction.invalidate_after_seconds > 0:
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS kv_index_updated_at_idx ON kv_index(updated_at)"
                )

                # trigger to delete old rows before read occurs
                conn.execute(
                    f"""
                    CREATE TRIGGER IF NOT EXISTS kv_index_delete_old_rows
                    BEFORE SELECT ON kv_index
                    BEGIN
                        DELETE FROM kv_index WHERE updated_at < {self.__current_time()} - {self.eviction.invalidate_after_seconds * 1000000};
                    END;
                    """
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
        return int(time.time() * 1000000)

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
                "SELECT num FROM kv_index_num_metadata WHERE key = ?",
                ("current_size_in_mb",),
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

    def set(self, key, value, reverse_order=False):
        self.__setitem__(key, value, reverse_order=reverse_order)

    def __setitem__(self, key, value, reverse_order=False):
        if self.store_key:
            key_hash, key = self.__encode_and_hash(
                key, return_encoded_key=self.store_key
            )
        else:
            key_hash = self.__encode_and_hash(key, return_encoded_key=self.store_key)

        value = self.__encode(value)

        row_size_in_bytes = len(key_hash) + len(value)

        data_to_insert = {
            "key_hash": sqlite3.Binary(key_hash),
            "pickled_value": sqlite3.Binary(value),
        }

        _time = self.__current_time()

        if self.store_key and key is not None:
            row_size_in_bytes += len(key)
            data_to_insert["pickled_key"] = sqlite3.Binary(key)

        if self.preserve_order:
            data_to_insert["updated_at"] = (-1 * _time) if reverse_order else _time

        if self.eviction.policy == EvictionCfg.EvictLRU:
            data_to_insert["last_accessed_time"] = _time
        elif self.eviction.policy == EvictionCfg.EvictLFU:
            data_to_insert["access_frequency"] = 1

        if self.eviction.max_size_in_mb:
            data_to_insert["size_in_bytes"] = row_size_in_bytes

        placeholders = ", ".join(["?"] * len(data_to_insert))
        columns = ", ".join(data_to_insert.keys())
        values = tuple(data_to_insert.values())

        update_placeholders = ", ".join(
            [
                f"{col}=excluded.{col}"
                for col in data_to_insert.keys()
                if col != "key_hash"
            ]
        )

        with self.__connection as conn:
            self.__run_eviction(conn)

            old_row_size_in_bytes = 0
            if self.eviction.max_size_in_mb:
                _old_row_size_in_bytes = conn.execute(
                    "SELECT size_in_bytes FROM kv_index WHERE key_hash = ?",
                    (sqlite3.Binary(key_hash),),
                ).fetchone()

                if _old_row_size_in_bytes is not None:
                    old_row_size_in_bytes = _old_row_size_in_bytes[0]

            conn.execute(
                f"""INSERT INTO kv_index ({columns}) VALUES ({placeholders})
                ON CONFLICT(key_hash) DO UPDATE SET {update_placeholders};
                """,
                values,
            )

            conn.execute(
                "UPDATE kv_index_num_metadata SET num = num + ? WHERE key = ?",
                (
                    (row_size_in_bytes - old_row_size_in_bytes) / (1024 * 1024),
                    "current_size_in_mb",
                ),
            )

    def __getitem__(self, key):
        key_hash, _ = self.__encode_and_hash(key, return_encoded_key=False)

        if self.eviction.policy in {EvictionCfg.EvictAny, EvictionCfg.EvictNone}:
            row = self.__connection.execute(
                f"SELECT pickled_value FROM kv_index WHERE key_hash = ?",
                (sqlite3.Binary(key_hash),),
            ).fetchone()

        else:
            with self.__connection as conn:
                if self.eviction.policy == EvictionCfg.EvictLRU:
                    row = conn.execute(
                        f"UPDATE kv_index SET last_accessed_time = ? WHERE key_hash = ? RETURNING pickled_value",
                        (self.__current_time(), sqlite3.Binary(key_hash)),
                    ).fetchone()
                elif self.eviction.policy == EvictionCfg.EvictLFU:
                    row = conn.execute(
                        f"UPDATE kv_index SET access_frequency = access_frequency + 1 WHERE key_hash = ? RETURNING pickled_value",
                        (sqlite3.Binary(key_hash),),
                    ).fetchone()
                elif self.eviction.policy == EvictionCfg.EvictAny:
                    row = conn.execute(
                        f"UPDATE kv_index SET updated_at = ? WHERE key_hash = ? RETURNING pickled_value",
                        (self.__current_time(), sqlite3.Binary(key_hash)),
                    ).fetchone()

        if row is None:
            raise KeyError

        return self.__decode_value(row[0])

    def getvalues(self, keys, default=None):
        key_hashes = [self.__encode_and_hash(key)[0] for key in keys]

        if self.eviction.policy in {EvictionCfg.EvictAny, EvictionCfg.EvictNone}:
            rows = self.__connection.execute(
                f"SELECT key_hash, pickled_value FROM kv_index WHERE key_hash IN ({', '.join(['?'] * len(key_hashes))})",
                key_hashes,
            ).fetchall()

        else:
            with self.__connection as conn:
                if self.eviction.policy == EvictionCfg.EvictLRU:
                    rows = conn.execute(
                        f"UPDATE kv_index SET last_accessed_time = ? WHERE key_hash IN ({', '.join(['?'] * len(key_hashes))}) RETURNING key_hash, pickled_value",
                        (self.__current_time(), *key_hashes),
                    ).fetchall()
                elif self.eviction.policy == EvictionCfg.EvictLFU:
                    rows = conn.execute(
                        f"UPDATE kv_index SET access_frequency = access_frequency + 1 WHERE key_hash IN ({', '.join(['?'] * len(key_hashes))}) RETURNING key_hash, pickled_value",
                        key_hashes,
                    ).fetchall()
                elif self.eviction.policy == EvictionCfg.EvictAny:
                    rows = conn.execute(
                        f"UPDATE kv_index SET updated_at = ? WHERE key_hash IN ({', '.join(['?'] * len(key_hashes))}) RETURNING key_hash, pickled_value",
                        (self.__current_time(), *key_hashes),
                    ).fetchall()

        if rows is None:
            return [default] * len(keys)

        rows = {row[0]: row[1] for row in rows}

        return [
            self.__decode_value(rows.get(key_hash, default)) for key_hash in key_hashes
        ]

    def items(self, reverse=False):
        if not self.store_key:
            raise Exception("Cannot iterate over items when store_key is False")

        sql = f"SELECT key_hash, pickled_key, pickled_value FROM kv_index {'ORDER BY updated_at' if self.preserve_order else ''} {'DESC' if reverse else ''}"

        for row in self.__connection.execute(sql):
            if row is None:
                break

            key_hash = row[0]
            key = row[1]
            value = row[2]

            yield (self.__decode_key(key, key_hash), self.__decode_value(value))

    def keys(self, reverse=False):
        if not self.store_key:
            raise Exception("Cannot iterate over keys when store_key is False")

        sql = f"SELECT key_hash, pickled_key FROM kv_index {'ORDER BY updated_at' if self.preserve_order else ''} {'DESC' if reverse else ''}"

        for row in self.__connection.execute(sql):
            if row is None:
                break

            key_hash = row[0]
            key = row[1]

            yield self.__decode_key(key, key_hash)

    def values(self, reverse=False):
        sql = f"SELECT pickled_value FROM kv_index {'ORDER BY updated_at' if self.preserve_order else ''} {'DESC' if reverse else ''}"

        for row in self.__connection.execute(sql):
            if row is None:
                break

            value = row[0]

            yield self.__decode_value(value)

    def __len__(self):
        return self.__connection.execute("SELECT COUNT(*) FROM kv_index").fetchone()[0]

    def __contains__(self, key):
        if self.__connection.execute(
            "SELECT COUNT(*) FROM kv_index WHERE key_hash = ?",
            (sqlite3.Binary(self.__encode_and_hash(key)[0]),),
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
                    (sqlite3.Binary(self.__encode_and_hash(key)[0]),),
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
                    (sqlite3.Binary(self.__encode_and_hash(key)[0]),),
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

                return [(self.__decode_key(row[0], None), self.__decode_value(row[1])) for row in rows]
            else:
                rows = conn.execute(
                    f"DELETE FROM kv_index WHERE ROWID IN (SELECT ROWID FROM kv_index ORDER BY updated_at {'DESC' if reverse else ''} LIMIT {n}) RETURNING pickled_key, pickled_value",
                ).fetchall()

                if rows is None:
                    raise KeyError

                return [(self.__decode_key(row[0], None), self.__decode_value(row[1])) for row in rows]
    
    def popitem(self, reverse=True):
        self.popitems(n=1, reverse=reverse)[0]
        

    def clear(self):
        with self.__connection as conn:
            conn.execute("DELETE FROM kv_index")
            conn.execute(
                "UPDATE kv_index_num_metadata SET num = 0 WHERE key = ?",
                ("current_size_in_mb",),
            )

    def __iter__(self):
        return self.keys()

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __encode(self, x):
        return (
            pickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
            if not isinstance(x, str)
            else x.encode()
        )

    def __encode_and_hash(self, x, return_encoded_key=False):
        x = self.__encode(x)

        if len(x) <= 32:
            return x, None
        else:
            return hashlib.sha256(x).digest(), (None if not return_encoded_key else x)

    def __decode_key(self, k, k_h):
        if k is None:
            k = k_h

        return pickle.loads(k) if k and k[0] == 128 else k.decode()

    def __decode_value(self, x):
        if x is None:
            return None
        return pickle.loads(x) if x and x[0] == 128 else x.decode()

    def __del__(self):
        if self.__connection:
            self.__connection.close()

    def vaccum(self):
        with self.__connection as conn:
            conn.execute("VACUUM")

    def update(self, items, reverse_order=False):
        data_to_insert = []
        key_hashes = []
        total_new_sizes = 0

        for key, value in reversed(items.items()):
            if self.store_key:
                key_hash, key = self.__encode_and_hash(
                    key, return_encoded_key=self.store_key
                )
            else:
                key_hash = self.__encode_and_hash(
                    key, return_encoded_key=self.store_key
                )

            value = self.__encode(value)

            row_size_in_bytes = len(key_hash) + len(value)

            if self.store_key and key is not None:
                row_size_in_bytes += len(key)

            total_new_sizes += row_size_in_bytes
            key_hashes.append(key_hash)

            data = {
                "key_hash": sqlite3.Binary(key_hash),
                "pickled_value": sqlite3.Binary(value),
            }

            _time = self.__current_time()

            if self.store_key:
                data["pickled_key"] = sqlite3.Binary(key) if key is not None else None

            if self.preserve_order:
                data["updated_at"] = (-1 * _time) if reverse_order else _time

            if self.eviction.policy == EvictionCfg.EvictLRU:
                data["last_accessed_time"] = _time
            elif self.eviction.policy == EvictionCfg.EvictLFU:
                data["access_frequency"] = 1

            if self.eviction.max_size_in_mb:
                data["size_in_bytes"] = row_size_in_bytes

            data_to_insert.append(tuple(data.values()))

        placeholders = ", ".join(["?"] * len(data))
        columns = ", ".join(data.keys())

        update_placeholders = ", ".join(
            [f"{col}=excluded.{col}" for col in data.keys() if col != "key_hash"]
        )

        with self.__connection as conn:
            self.__run_eviction(conn)

            if self.eviction.max_size_in_mb:
                sizes = conn.execute(
                    f"SELECT size_in_bytes FROM kv_index WHERE key_hash IN ({', '.join(['?'] * len(key_hashes))})",
                    key_hashes,
                ).fetchall()

                total_old_sizes = sum(size[0] for size in sizes)

                conn.execute(
                    "UPDATE kv_index_num_metadata SET num = num - ? WHERE key = ?",
                    (total_old_sizes / (1024 * 1024), "current_size_in_mb"),
                )
            else:
                total_old_sizes = 0

            conn.executemany(
                f"""INSERT INTO kv_index ({columns}) VALUES ({placeholders})
                ON CONFLICT(key_hash) DO UPDATE SET {update_placeholders};
                """,
                data_to_insert,
            )

            if self.eviction.max_size_in_mb:
                conn.execute(
                    "UPDATE kv_index_num_metadata SET num = num + ? WHERE key = ?",
                    (
                        (total_new_sizes - total_old_sizes) / (1024 * 1024),
                        "current_size_in_mb",
                    ),
                )
