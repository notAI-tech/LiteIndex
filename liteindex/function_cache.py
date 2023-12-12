import os
import time
import lmdb
import pickle
import hashlib
import tempfile
from .common_utils import set_ulimit

set_ulimit()

class Cache:
    def __init__(self, dir=tempfile.mkdtemp(), max_size_mb=2048, eviction_policy=None):
        self.dir = dir
        self.max_size_mb = max_size_mb
        self.eviction_policy = eviction_policy if max_size_mb is not None else EvictNone

        self.__env = lmdb.open(
            path=dir,
            subdir=True,
            map_size=max_size_mb * 1024**2 if eviction_policy else 512 ** 5,
            metasync=True,
            sync=True,
            create=True,
            writemap=False,
            max_readers=2048,
            meminit=False,
            max_dbs=3,
        )

        self.__cache_db = self.__env.open_db(b"cache", create=True)
        if eviction_policy == EvictLRU:
            self.__last_accessed_time_to_key_hash_db = self.__env.open_db(
                b"last_accessed_time_to_key_hash",
                create=True,
                dupsort=True,
                integerdup=True,
                integerkey=True,
                dupfixed=True,
            )
            self.__key_hash_to_last_accessed_time_db = self.__env.open_db(
                b"key_hash_to_last_accessed_time", create=True
            )
        elif eviction_policy == EvictLFU:
            self.__access_count_to_key_hash_db = self.__env.open_db(
                b"access_count_to_key_hash",
                create=True,
                dupsort=True,
                integerdup=True,
                integerkey=True,
                dupfixed=True,
            )
            self.__key_hash_to_access_count_db = self.__env.open_db(
                b"key_hash_to_access_count", create=True
            )

    
    def __setitem__(self, key, value):
        with self.__env.begin(write=True) as txn:
            txn.put(
                hashlib.sha256(pickle.dumps(key, protocol=pickle.HIGHEST_PROTOCOL)).digest(),
                pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL),
                db=self.__cache_db,
            )
    
    def __getitem__(self, key):
        with self.__env.begin(write=False) as txn:
            result = txn.get(hashlib.sha256(pickle.dumps(key, protocol=pickle.HIGHEST_PROTOCOL)).digest(), db=self.__cache_db)
            return pickle.loads(result) if result is not None else None
    


if __name__ == "__main__":
    test_cache = Cache(max_size_mb=1000, eviction_policy=EvictNone)
    s = time.time()
    for i in range(100000):
        test_cache[i] = i
    print(time.time() - s)

    import diskcache
    dc_cache = diskcache.Index(tempfile.mkdtemp())
    s = time.time()
    for i in range(100000):
        dc_cache[i] = i
    print(time.time() - s)
    
