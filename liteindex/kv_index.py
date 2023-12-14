from common_utils import set_ulimit, EvictionCfg

set_ulimit()

import os
import time
import pickle
import hashlib
import sqlite3
import threading


class KVIndex:
    aaaa = 0

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
        os.makedirs(
            os.path.dirname(self.db_path), exist_ok=True
        ) if db_path is not None else None
        self.ram_cache_mb = ram_cache_mb
        self.preserve_order = preserve_order

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

            if self.eviction.invalidate_after_seconds > 0:
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

    @property
    def size_in_mb(self):
        return (
            self.__connection.execute(
                """
            SELECT
                SUM(pgsize) as total_db_size
            FROM
                dbstat
            WHERE
                aggregate = TRUE;
            """
            ).fetchone()[0]
            / (1024 * 1024)
        )

    def __len__(self):
        return self.__connection.execute("SELECT COUNT(*) FROM kv_index").fetchone()[0]

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
            self.aaaa = self.aaaa + (time.time() - s)

            if current_size_in_mb >= self.eviction.max_size_in_mb:
                number_of_rows_to_evict = max(
                    int(current_number_of_rows * percent_to_evict), 1
                )

        elif self.eviction.max_number_of_rows:
            if current_number_of_rows >= self.eviction.max_number_of_rows:
                number_of_rows_to_evict = max(
                    int(current_number_of_rows * percent_to_evict), 1
                )

        if number_of_rows_to_evict == 0:
            return

        if self.eviction.policy == EvictionCfg.EvictLRU:
            conn.execute(
                f"DELETE FROM kv_index WHERE ROWID IN (SELECT ROWID FROM kv_index ORDER BY last_accessed_time ASC LIMIT {number_of_rows_to_evict})"
            )
        elif self.eviction.policy == EvictionCfg.EvictLFU:
            conn.execute(
                f"DELETE FROM kv_index WHERE ROWID IN (SELECT ROWID FROM kv_index ORDER BY access_frequency ASC LIMIT {number_of_rows_to_evict})"
            )
        elif self.eviction.policy == EvictionCfg.EvictAny:
            conn.execute(
                f"DELETE FROM kv_index WHERE ROWID IN (SELECT ROWID FROM kv_index LIMIT {number_of_rows_to_evict})"
            )

    def __setitem__(self, key, value):
        key = (
            pickle.dumps(key, protocol=pickle.HIGHEST_PROTOCOL)
            if not isinstance(key, str)
            else key.encode()
        )
        value = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        key_hash = hashlib.sha256(key).digest() if len(key) <= 32 else key

        row_size = len(key_hash) + len(value)

        data_to_insert = {
            "key_hash": sqlite3.Binary(key_hash),
            "pickled_value": sqlite3.Binary(value),
        }

        _time = self.__current_time()
        if self.store_key and len(key) <= 32:
            row_size += len(key)
            data_to_insert["pickled_key"] = sqlite3.Binary(key)
        if self.preserve_order:
            data_to_insert["updated_at"] = _time

        if self.eviction.policy == EvictionCfg.EvictLRU:
            data_to_insert["last_accessed_time"] = _time
        elif self.eviction.policy == EvictionCfg.EvictLFU:
            data_to_insert["access_frequency"] = 1

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

            conn.execute(
                f"""INSERT INTO kv_index ({columns}) VALUES ({placeholders})
                ON CONFLICT(key_hash) DO UPDATE SET {update_placeholders};
                """,
                values,
            )

            conn.execute(
                "UPDATE kv_index_num_metadata SET num = num + ? WHERE key = ?",
                (row_size / (1024 * 1024), "current_size_in_mb"),
            )

    def __getitem__(self, key):
        key = pickle.dumps(key, protocol=pickle.HIGHEST_PROTOCOL)
        key_hash = hashlib.sha256(key).digest()

        where_clause = (
            "WHERE key_hash = ?"
            if not self.eviction.invalidate_after_seconds
            else "WHERE key_hash = ? AND updated_at > ?"
        )

        if self.eviction.policy in {EvictionCfg.EvictAny, EvictionCfg.EvictNone}:
            row = conn.execute(
                f"SELECT pickled_value FROM kv_index {where_clause}",
                (sqlite3.Binary(key_hash),),
            ).fetchone()

        else:
            with self.__connection as conn:
                if self.eviction.policy == EvictionCfg.EvictLRU:
                    row = conn.execute(
                        f"UPDATE kv_index SET last_accessed_time = ? {where_clause} RETURNING pickled_value",
                        (self.__current_time(), sqlite3.Binary(key_hash)),
                    ).fetchone()
                elif self.eviction.policy == EvictionCfg.EvictLFU:
                    row = conn.execute(
                        f"UPDATE kv_index SET access_frequency = access_frequency + 1 {where_clause} RETURNING pickled_value",
                        (sqlite3.Binary(key_hash),),
                    ).fetchone()

        if row is None:
            raise KeyError

        return pickle.loads(row[0])

    def get(self, key, default=None, return_metadata=False):
        pass

    def items(self, reverse=False):
        sql = f"SELECT {'pickled_key' if self.store_key else 'key_hash'}, pickled_value FROM kv_index {'ORDER BY updated_at' if self.preserve_order else ''} {'DESC' if reverse else ''}"
        with self.__connection as conn:
            for row in conn.execute(sql):
                yield pickle.loads(row[0]), pickle.loads(row[1])


if __name__ == "__main__":
    import tempfile

    kv_index = KVIndex(
        "tkv/a.db", eviction=EvictionCfg(EvictionCfg.EvictAny, max_size_in_mb=2048)
    )
    start = time.time()
    for i in range(10000):
        kv_index[i] = i
    print("KV set", time.time() - start)

    start = time.time()
    for k, v in kv_index.items():
        pass
    print("KV get", time.time() - start)

    print(len(kv_index), kv_index.size_in_mb, kv_index.aaaa)

    from diskcache import Index

    index = Index("dkv")

    start = time.time()
    for i in range(10000):
        index[i] = i
    print("DKV set", time.time() - start)

    start = time.time()
    for k, v in index.items():
        pass
    print("DKV get", time.time() - start)
