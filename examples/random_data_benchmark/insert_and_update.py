import os
import uuid
import time
import json
import shutil
import pickle
from tqdm import tqdm

import lmdb
from liteindex import DefinedIndex
from diskcache import Index as DiskCacheIndex
from sqlitedict import SqliteDict

from random_data_generator import generate_data

DATA_DIR = f"DATA"

shutil.rmtree(DATA_DIR, ignore_errors=True)
os.makedirs(DATA_DIR, exist_ok=True)

BENCHMARKS = {
    "liteindex": {
        "insertion": [],
        "pop": [],
        "read": [],
        "batch_size": []
    },
    "diskcache": {
        "insertion": [],
        "pop": [],
        "read": [],
        "batch_size": []
    },
    "sqlitedict": {
        "insertion": [],
        "pop": [],
        "read": [],
        "batch_size": []
    },
    "lmdb": {
        "insertion": [],
        "pop": [],
        "read": [],
        "batch_size": []
    }
}

N = 10000

# In LiteIndex "other" can be used to store any python object
# Other is not query-able and can be inserted, updated or deleted
# Memory is set to 8MB as diskcache uses 8192 pages by default

liteindex_index = DefinedIndex(
            "example_queue", 
            schema = {
                "data": "other"
            },
            db_path=f"{DATA_DIR}/example_queue.db",
            ram_cache_mb=32,
        )

diskcache_index = DiskCacheIndex(f"{DATA_DIR}/example_queue_diskcache")

sqlitedict_index = SqliteDict(f"{DATA_DIR}/example_queue_sqlitedict.db", autocommit=True)

lmdb_index = lmdb.open(f"{DATA_DIR}/example_queue_lmdb", map_size=int(1e11), create=True)

ALLOWED_TYPES = ["number", "string", "numpy", "bytes"]

for batch_size in [1, 4, 16, 64, 256]:
    liteindex_insertion_time = 0
    diskcache_insertion_time = 0
    sqlitedict_insertion_time = 0
    lmdb_insertion_time = 0

    for i in tqdm(range(0, N, batch_size)):
        batch = {str(uuid.uuid4()): {"data": generate_data(max_depth=3, max_width=5, types=ALLOWED_TYPES)} for _ in range(batch_size)}
        start_time = time.time()
        liteindex_index.update(batch)
        liteindex_insertion_time += (time.time() - start_time)

        batch = {k: v["data"] for k, v in batch.items()}

        start_time = time.time()
        with diskcache_index.transact():
            for k, v in batch.items():
                diskcache_index[k] = v
        
        diskcache_insertion_time += (time.time() - start_time)

        start_time = time.time()
        sqlitedict_index.update(batch)
        sqlitedict_insertion_time += (time.time() - start_time)

        start_time = time.time()
        with lmdb_index.begin(write=True) as txn:
            for key, value in batch.items():
                txn.put(key.encode(), pickle.dumps(value, protocol=5))

        lmdb_insertion_time += (time.time() - start_time)


    
    print(f"Batch Size: {batch_size} insertion finished, liteindex_count: {liteindex_index.count()}, diskcache_count: {len(diskcache_index)}, sqlitedict_count: {len(sqlitedict_index)}, lmdb_count:")

    BENCHMARKS["liteindex"]["insertion"].append(liteindex_insertion_time / N)
    BENCHMARKS["liteindex"]["batch_size"].append(batch_size)

    BENCHMARKS["diskcache"]["insertion"].append(diskcache_insertion_time / N)
    BENCHMARKS["diskcache"]["batch_size"].append(batch_size)

    BENCHMARKS["sqlitedict"]["insertion"].append(sqlitedict_insertion_time / N)
    BENCHMARKS["sqlitedict"]["batch_size"].append(batch_size)

    BENCHMARKS["lmdb"]["insertion"].append(lmdb_insertion_time / N)
    BENCHMARKS["lmdb"]["batch_size"].append(batch_size)

# pop times
# for batch_size in [1, 4, 16, 64, 256]:
#     liteindex_pop_time = 0
#     diskcache_pop_time = 0
#     sqlitedict_pop_time = 0

#     for i in tqdm(range(0, N, batch_size)):
#         start_time = time.time()
#         liteindex_index.pop(n=batch_size)
#         liteindex_pop_time += (time.time() - start_time)

#         start_time = time.time()
#         with diskcache_index.transact():
#             for _ in range(batch_size):
#                 diskcache_index.popitem()
        
#         diskcache_pop_time += (time.time() - start_time)

    
#     print(f"Batch Size: {batch_size} pop finished, liteindex_count: {liteindex_index.count()}, diskcache_count: {len(diskcache_index)}")

#     BENCHMARKS["liteindex"]["pop"].append(liteindex_pop_time / N)
#     BENCHMARKS["diskcache"]["pop"].append(diskcache_pop_time / N)


import json
json.dump(BENCHMARKS, open("random_insertion_benchmarks.json", "w"), indent=4)

import matplotlib.pyplot as plt
import numpy as np

# Function to plot bar graphs
def plot_bar_graph(x, y1, y2, y3, y4, title, xlabel, ylabel):
    barWidth = 0.25
    r1 = np.arange(len(x))
    r2 = [i + barWidth for i in r1]
    r3 = [i + barWidth for i in r2]
    r4 = [i + barWidth for i in r3]

    plt.figure(figsize=(10, 5))
    plt.bar(r1, y1, color='b', width=barWidth, edgecolor='grey', label='LiteIndex')
    plt.bar(r2, y2, color='r', width=barWidth, edgecolor='grey', label='DiskCache')
    plt.bar(r3, y3, color='g', width=barWidth, edgecolor='grey', label='SQLiteDict')
    plt.bar(r4, y4, color='y', width=barWidth, edgecolor='grey', label='LMDB')


    plt.xlabel(xlabel, fontweight='bold')
    plt.xticks([r + barWidth for r in range(len(x))], x)
    plt.ylabel(ylabel)
    plt.title(title)

    plt.legend()
    plt.savefig(f"{title}.png")
    plt.show()

# Insertion Times Bar Graph
plot_bar_graph(
    BENCHMARKS["liteindex"]["batch_size"],
    BENCHMARKS["liteindex"]["insertion"],
    BENCHMARKS["diskcache"]["insertion"],
    BENCHMARKS["sqlitedict"]["insertion"],
    BENCHMARKS["lmdb"]["insertion"],
    title='Insertion Times by Batch Size',
    xlabel='Batch Size',
    ylabel='Time (s)'
)
