import os
import time
import pickle
import hashlib
import tempfile
from .defined_index import DefinedIndex


def function_cache(
    cache_dir=tempfile.mkdtemp(),
    max_size_on_disk_mb=200,
    max_ram_cache_mb=100,
    compression_level=-1,
    eviction_policy="LRU",
):
    index = DefinedIndex(
        name="function_cache",
        schema={"result": "other", "last_read_at": "number"},
        db_path=os.path.join(cache_dir, "function_cache.db"),
        ram_cache_mb=max_ram_cache_mb,
        compression_level=compression_level,
    )

    def decorator(func):
        def wrapper(*args, **kwargs):
            key = hashlib.sha256(
                pickle.dumps((args, kwargs), protocol=pickle.HIGHEST_PROTOCOL)
            ).digest()
            result = None
            try:
                result = index.get(key)["key"]["result"]
                index.update(key, {"last_read_at": time.time()})

            except:
                result = func(*args, **kwargs)
                index.update({key: {"result": result, "last_read_at": time.time()}})

            return result

        return wrapper

    return decorator
