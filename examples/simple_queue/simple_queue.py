import os
import uuid
import time
import shutil
import pickle
from tqdm import tqdm

from liteindex import DefinedIndex
from diskcache import Index as DiskCacheIndex
from sqlitedict import SqliteDict

from random_data_generator import generate_data

DATA_DIR = f"DATA"

shutil.rmtree(DATA_DIR, ignore_errors=True)
os.makedirs(DATA_DIR)

print("Generating data")
random_data = []

for _ in tqdm(range(1000)):
    try:
        random_data.append(generate_data())
    except:
        break

# A queue to store any type of data

liteindex_index = DefinedIndex(
            "example_queue", 
            schema = {
                "data": "other", # any picklable data
            },
            db_path=f"{DATA_DIR}/example_queue.db",
        )


for batch_size in [1, 10, 100, 500, 1000]:
    # LiteIndex
    start_time = time.time()
    for _ in range(10):
        for i in range(0, len(random_data), batch_size):
            liteindex_index.update({str(uuid.uuid4()): {'data': _} for _ in random_data[i:i+batch_size]})

    print(f"liteindex batch size: {batch_size} average insertion time", (time.time() - start_time)/ 10000)

    # DiskCache Index
    diskcache_index = DiskCacheIndex(f"{DATA_DIR}/example_queue_diskcache")

    start_time = time.time()
    for _ in range(10):
        for i in range(0, len(random_data), batch_size):
            with diskcache_index.transact():
                for _ in random_data[i:i+batch_size]:
                    diskcache_index[str(uuid.uuid4())] = _

    print(f"diskcache batch size: {batch_size} average insertion time", (time.time() - start_time)/ 10000)

    # SqliteDict

    sqlitedict_index = SqliteDict(f"{DATA_DIR}/example_queue_sqlitedict.db")

    start_time = time.time()
    for _ in range(10):
        for i in range(0, len(random_data), batch_size):
            for _ in random_data[i:i+batch_size]:
                sqlitedict_index[str(uuid.uuid4())] = _
            sqlitedict_index.commit()

    print(f"sqlitedict batch size: {batch_size} average insertion time", (time.time() - start_time)/ 10000)
