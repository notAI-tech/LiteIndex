import os
import tempfile
from .kv_index import KVIndex, EvictionCfg


def function_cache(
    func,
    path=os.path.join(tempfile.gettempdir(), "cache.db"),
    ram_cache_mb=32,
    eviction_policy=EvictionCfg.EvictFIFO,
    max_number_of_items=100000,
    max_size_in_mb=0,
    invalidate_after_seconds=0,
):
    cache = KVIndex(
        path,
        store_key=False,
        ram_cache_mb=ram_cache_mb,
        eviction=EvictionCfg(
            eviction_policy,
            max_number_of_items=100000,
            max_size_in_mb=0,
            invalidate_after_seconds=0,
        ),
    )

    def wrapper(*args, **kwargs):
        key = (args, kwargs)
        try:
            return cache[key]
        except KeyError:
            value = func(*args, **kwargs)
            cache[key] = value
            return value

    return wrapper
