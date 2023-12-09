import os
import time
import lmdb
import pickle
import hashlib
import tempfile


def function_cache(dir=tempfile.mkdtemp(), max_size_mb=1000, eviction_policy="lru"):
    env = lmdb.open(
        path=dir,
        subdir=True,
        map_size=max_size_mb * 1024**2,
        metasync=False,
        sync=False,
        create=True,
        writemap=False,
        max_readers=2048,
        meminit=False,
        max_dbs=3,
    )

    cache_db = env.open_db(b"cache", create=True)
    access_order_db = env.open_db(
        b"access_order",
        create=True,
        integerkey=True,
        dupsort=True,
        dupfixed=True,
        integerdup=True,
    )
    last_access_db = env.open_db(b"last_access", create=True)

    def __update_usage(txn, hash_key):
        _time = int(time.time() * 1e6).to_bytes(8, "big")
        txn.put(_time, hash_key, db=access_order_db, overwrite=False)
        txn.put(hash_key, _time, db=last_access_db, overwrite=True)

    def decorator(func):
        def wrapper(*args, **kwargs):
            inputs_hash = (
                hashlib.sha256(
                    pickle.dumps((args, kwargs), protocol=pickle.HIGHEST_PROTOCOL)
                )
                .hexdigest()
                .encode("utf-8")
            )

            with env.begin(write=True, buffers=True) as txn:
                result = txn.get(inputs_hash, db=cache_db)
                if result is not None:
                    __update_usage(txn, inputs_hash)
                    return pickle.loads(result)

            result = func(*args, **kwargs)

            with env.begin(write=True) as txn:
                txn.put(
                    inputs_hash,
                    pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL),
                    db=cache_db,
                )
                __update_usage(txn, inputs_hash)

            return result

        return wrapper

    return decorator
