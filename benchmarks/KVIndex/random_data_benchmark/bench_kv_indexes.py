import sys
sys.path.append("../../../")

import os
import uuid
import time
import json
import shutil
import pickle
import random
import numpy as np
from tqdm import tqdm

from liteindex import KVIndex, EvictionCfg
from diskcache import Index as DiskCacheIndex
from sqlitedict import SqliteDict

from random_data_generator import generate_data

def get_indexes():
    DATA_DIR = f"DATA"

    shutil.rmtree(DATA_DIR, ignore_errors=True)
    os.makedirs(DATA_DIR, exist_ok=True)

    kv_index = KVIndex(f"{DATA_DIR}/example_queue_kvindex.db", ram_cache_mb=32, eviction=EvictionCfg(EvictionCfg.EvictNone))

    diskcache_index = DiskCacheIndex(f"{DATA_DIR}/example_queue_diskcache", )

    sqlitedict_index = SqliteDict(f"{DATA_DIR}/example_queue_sqlitedict.db", autocommit=True)

    return kv_index, diskcache_index, sqlitedict_index


'''
# Test 1 - Small objects test

Insert random keys and values into the index
keys can be numbers or strings
values can be numbers or strings < 100chars or numpy array < 100 dimension or number or string array < 20 elements
'''

def test_1():
    kv_index, diskcache_index, sqlitedict_index = get_indexes()

    _dict = {}
    N = 100
    for i in tqdm(range(N)):
        if random.randint(0, 1) == 0:
            key = random.randint(1, 10000000000000)/ random.randint(1, 10000000000000)
        else:
            key = "".join([chr(random.randint(0, 255)) for _ in range(random.randint(1, 100))])

        if random.randint(0, 5) == 0:
            value = random.randint(1, 10000000000000)/ random.randint(1, 10000000000000)
        elif random.randint(0, 5) == 1:
            value = "".join([chr(random.randint(0, 255)) for _ in range(random.randint(1, 100))])
        elif random.randint(0, 5) == 2:
            value = np.random.rand(random.randint(1, 100))
        elif random.randint(0, 5) == 3:
            value = tuple([random.randint(1, 10000000000000)/ random.randint(1, 10000000000000) for _ in range(random.randint(1, 10))])
        elif random.randint(0, 5) == 4:
            value = tuple(["".join([chr(random.randint(0, 255)) for _ in range(random.randint(1, 100))]) for _ in range(random.randint(1, 10))])
        else:
            value = tuple([np.random.rand(random.randint(1, 100)) for _ in range(random.randint(1, 10))])
    
        _dict[key] = value
    

    kv_index_insert_start = time.time()
    for key, value in tqdm(_dict.items()):
        kv_index[key] = value
    kv_index_insert_end = time.time()

    diskcache_index_insert_start = time.time()
    for key, value in tqdm(_dict.items()):
        diskcache_index[key] = value
    diskcache_index_insert_end = time.time()

    sqlitedict_index_insert_start = time.time()
    for key, value in tqdm(_dict.items()):
        sqlitedict_index[key] = value
    sqlitedict_index_insert_end = time.time()

    print(f"diskcache_index insert time: {diskcache_index_insert_end - diskcache_index_insert_start}")
    print(f"sqlitedict_index insert time: {sqlitedict_index_insert_end - sqlitedict_index_insert_start}")
    print(f"kv_index insert time: {kv_index_insert_end - kv_index_insert_start}")

    diskcache_index_read_start = time.time()
    for key in tqdm(_dict.keys()):
        value = diskcache_index[key]
    diskcache_index_read_end = time.time()

    sqlitedict_index_read_start = time.time()
    for key in tqdm(_dict.keys()):
        value = sqlitedict_index[key]
    sqlitedict_index_read_end = time.time()

    kv_index_read_start = time.time()
    for key in tqdm(_dict.keys()):
        value = kv_index[key]
    kv_index_read_end = time.time()

    print(f"diskcache_index read time: {diskcache_index_read_end - diskcache_index_read_start}")
    print(f"sqlitedict_index read time: {sqlitedict_index_read_end - sqlitedict_index_read_start}")
    print(f"kv_index read time: {kv_index_read_end - kv_index_read_start}")


if __name__ == "__main__":
    test_1()
