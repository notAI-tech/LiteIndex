import os
import lmdb
import pickle
import hashlib
import tempfile


def function_cache(cache_dir=tempfile.mkdtemp(), map_size=1000000):
    env = lmdb.open(cache_dir, map_size=map_size)

    def decorator(func):
        def wrapper(*args, **kwargs):
            with env.begin(write=False) as txn:
                key = hashlib.sha256(pickle.dumps((args, kwargs))).digest()
                result = txn.get(key)
                if result is not None:
                    return pickle.loads(result)

            result = func(*args, **kwargs)

            with env.begin(write=True) as txn:
                txn.put(key, pickle.dumps(result))

            return result

        return wrapper

    return decorator
