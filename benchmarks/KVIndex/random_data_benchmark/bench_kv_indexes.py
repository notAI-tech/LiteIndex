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

    # using 32 cause diskcache uses 8192 pages by default
    kv_index = KVIndex(f"{DATA_DIR}/example_queue_kvindex.db", ram_cache_mb=32)

    diskcache_index = DiskCacheIndex(f"{DATA_DIR}/example_queue_diskcache", )

    sqlitedict_index = SqliteDict(f"{DATA_DIR}/example_queue_sqlitedict.db", autocommit=False)

    return kv_index, diskcache_index, sqlitedict_index


'''
# Test 1 - Small objects test

Insert random keys and values into the index
keys can be numbers or strings
values can be numbers or strings < 100chars or numpy array < 100 dimension or number or string array < 20 elements
'''


def get_batch_dict(N):
    _dict = {}

    for i in range(N):
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
    
    return _dict

def test_simple_data():
    kv_index, diskcache_index, sqlitedict_index = get_indexes()

    RESULTS = {}
    N = 100

    for batch_size in [1]:#, 10, 100, 1000]:
        print(f"Testing batch size {batch_size}")

        total_kv_index_insert_time = 0
        total_diskcache_index_insert_time = 0
        total_sqlitedict_index_insert_time = 0

        total_kv_index_read_time = 0
        total_diskcache_index_read_time = 0
        total_sqlitedict_index_read_time = 0

        total_kv_index_iter_time = 0
        total_diskcache_index_iter_time = 0
        total_sqlitedict_index_iter_time = 0

        total_kv_index_delete_time = 0
        total_diskcache_index_delete_time = 0
        total_sqlitedict_index_delete_time = 0

        key_batches = []
        for _ in range(int(N/batch_size)):
            _dict = get_batch_dict(batch_size)
            key_batches.append(list(_dict.keys()))

            kv_index_insert_start = time.time()
            kv_index.update(_dict)
            kv_index_insert_end = time.time()

            diskcache_index_insert_start = time.time()
            diskcache_index.update(_dict)
            diskcache_index_insert_end = time.time()

            sqlitedict_index_insert_start = time.time()
            sqlitedict_index.update(_dict)
            sqlitedict_index.commit()
            sqlitedict_index_insert_end = time.time()

            total_kv_index_insert_time += kv_index_insert_end - kv_index_insert_start
            total_diskcache_index_insert_time += diskcache_index_insert_end - diskcache_index_insert_start
            total_sqlitedict_index_insert_time += sqlitedict_index_insert_end - sqlitedict_index_insert_start

        print(f"len(kv_index) = {len(kv_index)}, len(diskcache_index) = {len(diskcache_index)}, len(sqlitedict_index) = {len(sqlitedict_index)}")

        for key_batch in key_batches:
            diskcache_index_read_start = time.time()
            with diskcache_index.transact():
                for key in key_batch:
                    value = diskcache_index[key]
            diskcache_index_read_end = time.time()

            sqlitedict_index_read_start = time.time()
            for key in key_batch:
                value = sqlitedict_index[key]
            sqlitedict_index_read_end = time.time()

            kv_index_read_start = time.time()
            values = kv_index.getvalues(key_batch)
            kv_index_read_end = time.time()

            total_kv_index_read_time += kv_index_read_end - kv_index_read_start
            total_diskcache_index_read_time += diskcache_index_read_end - diskcache_index_read_start
            total_sqlitedict_index_read_time += sqlitedict_index_read_end - sqlitedict_index_read_start

        diskcache_index_iter_start = time.time()
        with diskcache_index.transact():
            for key, value in diskcache_index.items():
                pass
        diskcache_index_iter_end = time.time()

        sqlitedict_index_iter_start = time.time()
        for key, value in sqlitedict_index.items():
            pass
        sqlitedict_index_iter_end = time.time()
        
        kv_index_iter_start = time.time()
        for key, value in kv_index.items():
            pass
        kv_index_iter_end = time.time()

        total_kv_index_iter_time += kv_index_iter_end - kv_index_iter_start
        total_diskcache_index_iter_time += diskcache_index_iter_end - diskcache_index_iter_start
        total_sqlitedict_index_iter_time += sqlitedict_index_iter_end - sqlitedict_index_iter_start

        for key_batch in key_batches:
            diskcache_index_delete_start = time.time()
            with diskcache_index.transact():
                for key in key_batch:
                    del diskcache_index[key]
            diskcache_index_delete_end = time.time()

            sqlitedict_index_delete_start = time.time()
            for key in key_batch:
                del sqlitedict_index[key]
            sqlitedict_index.commit()
            sqlitedict_index_delete_end = time.time()

            kv_index_delete_start = time.time()
            kv_index.delete(key_batch)
            kv_index_delete_end = time.time()
            total_kv_index_delete_time += kv_index_delete_end - kv_index_delete_start
            total_diskcache_index_delete_time += diskcache_index_delete_end - diskcache_index_delete_start
            total_sqlitedict_index_delete_time += sqlitedict_index_delete_end - sqlitedict_index_delete_start

        assert len(kv_index) == 0
        assert len(diskcache_index) == 0
        assert len(sqlitedict_index) == 0

        RESULTS[batch_size] = {
            "insert": {
                "KVIndex": total_kv_index_insert_time,
                "DiskCache": total_diskcache_index_insert_time,
                "SqliteDict": total_sqlitedict_index_insert_time
            },
            "read": {
                "KVIndex": total_kv_index_read_time,
                "DiskCache": total_diskcache_index_read_time,
                "SqliteDict": total_sqlitedict_index_read_time
            },
            "iter": {
                "KVIndex": total_kv_index_iter_time,
                "DiskCache": total_diskcache_index_iter_time,
                "SqliteDict": total_sqlitedict_index_iter_time
            },
            "delete": {
                "KVIndex": total_kv_index_delete_time,
                "DiskCache": total_diskcache_index_delete_time,
                "SqliteDict": total_sqlitedict_index_delete_time
            }
        }
    
    return RESULTS


if __name__ == "__main__":
    RESULTS = test_simple_data()
    import matplotlib.pyplot as plt

    batch_size = 1
    operations = ['insert', 'read', 'iter', 'delete']
    libraries = ['KVIndex', 'DiskCache', 'SqliteDict']

    bar_positions = np.arange(len(operations))

    bar_width = 0.25

    plt.figure(figsize=(10, 6))
    for i, lib in enumerate(libraries):
        times = [RESULTS[batch_size][op][lib] for op in operations]
        plt.bar(bar_positions + i * bar_width, times, width=bar_width, label=lib.capitalize().replace('_', ' '))

    plt.xlabel("Operation")
    plt.ylabel("Time (s)")
    plt.title("Operation times at batch size 1")
    plt.xticks(bar_positions + bar_width, operations)
    plt.legend()
    plt.show()

    batch_sizes = sorted(RESULTS.keys())

    plt.figure(figsize=(10, 6))

    for lib in libraries:
        for op in operations:
            times = [RESULTS[b_size][op][lib] for b_size in batch_sizes]
            plt.plot(batch_sizes, times, marker='o', label=f"{lib.capitalize().replace('_', ' ')} {op}")

    plt.xlabel("Batch Size")
    plt.ylabel("Time (s)")
    plt.title("Index performance by batch size")
    plt.legend()
    plt.show()

