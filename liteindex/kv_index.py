from .common_utils import set_ulimit
set_ulimit()

import os
import time
import pickle
import hashlib
import sqlite3
import threading

EvictAny = "any"
EvictLRU = "lru"
EvictLFU = "lfu"
EvictNone = None

class KVIndex:
    def __init__(
        self,
        db_path=None,
        store_key=True,
        eviction_policy=EvictNone,
        ram_cache_mb=32
    ):
        self.store_key = store_key
        self.eviction_policy = eviction_policy
        self.db_path = db_path if db_path is not None else ":memory:"
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True) if db_path is not None else None
        self.ram_cache_mb = ram_cache_mb

        self.__local_storage = threading.local()

        columns_needed_and_sql_types = {
            "key_hash": "BLOB",
            "pickled_value": "BLOB",
        }

        if self.store_key:
            columns_needed_and_sql_types["pickled_key"] = "BLOB"
        if self.eviction_policy is EvictLRU:
            columns_needed_and_sql_types["last_accessed_time"] = "INTEGER"
        elif self.eviction_policy is EvictLFU:
            columns_needed_and_sql_types["access_frequency"] = "INTEGER"
        
        with self.__connection:
            self.__connection.execute(
                f"CREATE TABLE IF NOT EXISTS kv_index ({','.join([f'{col} {sql_type}' for col, sql_type in columns_needed_and_sql_types.items()])}, PRIMARY KEY (key_hash))"
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

            self.__local_storage.db_conn.execute(
                f"PRAGMA BUSY_TIMEOUT=60000"
            )

        return self.__local_storage.db_conn

    def __setitem__(self, key, value):
        key = pickle.dumps(key, protocol=pickle.HIGHEST_PROTOCOL)
        value = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        key_hash = hashlib.sha256(key).digest()
        
        data_to_insert = {
            "key_hash": sqlite3.Binary(key_hash),
            "pickled_value": sqlite3.Binary(value),
        }
        if self.store_key:
            data_to_insert["pickled_key"] = sqlite3.Binary(key)
        if self.eviction_policy == EvictLRU:
            data_to_insert["last_accessed_time"] = int(time.time() * 1000)
        elif self.eviction_policy == EvictLFU:
            data_to_insert["access_frequency"] = 1
        
        placeholders = ', '.join(['?'] * len(data_to_insert))
        columns = ', '.join(data_to_insert.keys())
        values = tuple(data_to_insert.values())

        update_placeholders = ', '.join([
            f'{col}=excluded.{col}' for col in data_to_insert.keys()
            if col != 'key_hash'
        ])

        with self.__connection as conn:
            conn.execute(
                f'''INSERT INTO kv_index ({columns}) VALUES ({placeholders})
                ON CONFLICT(key_hash) DO UPDATE SET {update_placeholders};
                ''', values
            )

    def __getitem__(self, key):
        key = pickle.dumps(key, protocol=pickle.HIGHEST_PROTOCOL)
        key_hash = hashlib.sha256(key).digest()

        with self.__connection as conn:
            row = conn.execute(
                "SELECT pickled_value FROM kv_index WHERE key_hash = ?", (sqlite3.Binary(key_hash),)
            ).fetchone()
            if row is None:
                raise KeyError(key)
            return pickle.loads(row[0])



if __name__ == "__main__":
    import tempfile
    kv_index = KVIndex("tkv/a.db")
    start = time.time()
    for i in range(10000):
        kv_index[i] = i
    print(time.time() - start)

    from diskcache import Index
    index = Index("dkv")

    start = time.time()
    for i in range(10000):
        index[i] = i
    print(time.time() - start)

    
