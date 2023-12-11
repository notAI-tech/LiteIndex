import os
import time
import lmdb
import pickle
import hashlib
import tempfile
from .common_utils import set_ulimit

set_ulimit()

EvictAny = "any"
EvictLRU = "lru"
EvictLFU = "lfu"


def function_cache(
    dir=tempfile.mkdtemp(),
    max_size_mb=1000,
    eviction_policy=EvictAny,
    fast_mode=True,
    invalidate_older_than_seconds=None,
):
    env = lmdb.open(
        path=dir,
        subdir=True,
        map_size=max_size_mb * 1024**2,
        metasync=not fast_mode,
        sync=not fast_mode,
        create=True,
        writemap=False,
        max_readers=2048,
        meminit=False,
        max_dbs=3,
    )

    cache_db = env.open_db(b"cache", create=True)
    if eviction_policy == EvictLRU:
        last_accessed_time_to_key_hash_db = env.open_db(
            b"last_accessed_time_to_key_hash",
            create=True,
            dupsort=True,
            integerdup=True,
            integerkey=True,
            dupfixed=True,
        )
        key_hash_to_last_accessed_time_db = env.open_db(
            b"key_hash_to_last_accessed_time", create=True
        )
    elif eviction_policy == EvictLFU:
        access_count_to_key_hash_db = env.open_db(
            b"access_count_to_key_hash",
            create=True,
            dupsort=True,
            integerdup=True,
            integerkey=True,
            dupfixed=True,
        )
        key_hash_to_access_count_db = env.open_db(
            b"key_hash_to_access_count", create=True
        )

    def decorator(func):
        def wrapper(*args, **kwargs):
            inputs_hash = hashlib.sha256(
                pickle.dumps((args, kwargs), protocol=pickle.HIGHEST_PROTOCOL)
            ).digest()

            with env.begin(
                write=eviction_policy in {EvictLRU, EvictLFU}, buffers=True
            ) as txn:
                result = txn.get(inputs_hash, db=cache_db)
                if result is not None:
                    if eviction_policy == EvictLRU:
                        _time_in_bytes = int(time.time() * 1e6).to_bytes(
                            8, byteorder="big"
                        )
                        txn.put(
                            _time_in_bytes,
                            inputs_hash,
                            db=last_accessed_time_to_key_hash_db,
                            dupdata=True,
                        )
                        txn.put(
                            inputs_hash,
                            _time_in_bytes,
                            db=key_hash_to_last_accessed_time_db,
                        )
                    elif eviction_policy == EvictLFU:
                        _access_count_in_bytes = txn.get(
                            inputs_hash, db=key_hash_to_access_count_db
                        )
                        _access_count = int.from_bytes(
                            _access_count_in_bytes, byteorder="big"
                        )
                        txn.put(
                            inputs_hash,
                            (_access_count + 1).to_bytes(8, byteorder="big"),
                            db=key_hash_to_access_count_db,
                        )
                        txn.delete(
                            _access_count_in_bytes,
                            inputs_hash,
                            db=access_count_to_key_hash_db,
                        )

                    return pickle.loads(result)

            result = func(*args, **kwargs)

            with env.begin(write=True) as txn:
                stat = txn.stat(db=cache_db)
                current_size_mb = stat["psize"] * stat["leaf_pages"] / 1024**2
                current_count = stat["entries"]
                if current_size_mb > max_size_mb * 0.8:
                    if eviction_policy == EvictAny:
                        cursor = txn.cursor(db=cache_db)
                        while True:
                            for _ in range(current_count // 3):
                                if cursor.first() is None:
                                    break
                                cursor.delete()

                            stat = txn.stat(db=cache_db)
                            current_size_mb = (
                                stat["psize"] * stat["leaf_pages"] / 1024**2
                            )
                            if current_size_mb < max_size_mb * 0.8:
                                break

                txn.put(
                    inputs_hash,
                    pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL),
                    db=cache_db,
                )

                if eviction_policy == EvictLRU:
                    _time_in_bytes = int(time.time()).to_bytes(8, byteorder="big")
                    txn.put(
                        _time_in_bytes,
                        inputs_hash,
                        db=last_accessed_time_to_key_hash_db,
                        dupdata=True,
                    )
                    txn.put(
                        inputs_hash,
                        _time_in_bytes,
                        db=key_hash_to_last_accessed_time_db,
                    )
                elif eviction_policy == EvictLFU:
                    txn.put(
                        inputs_hash,
                        b"\x00\x00\x00\x00\x00\x00\x00\x01",
                        db=key_hash_to_access_count_db,
                    )
                    txn.put(
                        b"\x00\x00\x00\x00\x00\x00\x00\x01",
                        inputs_hash,
                        db=access_count_to_key_hash_db,
                    )

            return result

        return wrapper

    return decorator
