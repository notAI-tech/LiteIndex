import sys

sys.path.append("../")

import random
import numpy as np

from liteindex import KVIndex, EvictionCfg

# Create a new index
kv_index = KVIndex(
    db_path="kv_index_example.db"  # if not specified, an in memory db is used
)

# string keys and any value example

for i in range(1000):
    choice = random.randint(0, 2)
    if choice == 0:
        kv_index["aa" * i] = "bb" * (100 - i)
    elif choice == 1:
        kv_index["aa" * i] = i**2
    else:
        kv_index["aa" * i] = np.array([i, i + 1, i + 2])

# get all keys and check if the value is correct
for i in range(1000):
    key = "aa" * i
    value = kv_index[key]
    if isinstance(value, str):
        assert value == "bb" * (100 - i)
    elif isinstance(value, int):
        assert value == i**2
    else:
        assert np.all(value == np.array([i, i + 1, i + 2]))


# check if .items() is returning in insertion order and keys and values are correct
for i, (key, value) in enumerate(kv_index.items()):
    assert key == "aa" * i
    if isinstance(value, str):
        assert value == "bb" * (100 - i)
    elif isinstance(value, int):
        assert value == i**2
    else:
        assert np.all(value == np.array([i, i + 1, i + 2]))

# check if .keys and .values are returning in insertion order and keys and values are correct
for i, key in enumerate(kv_index.keys()):
    assert key == "aa" * i

for i, value in enumerate(kv_index.values()):
    if isinstance(value, str):
        assert value == "bb" * (100 - i)
    elif isinstance(value, int):
        assert value == i**2
    else:
        assert np.all(value == np.array([i, i + 1, i + 2]))

# check if len is correct
assert len(kv_index) == 1000

# check if update() is working

for i in range(100):
    kv_index.update({"aa" * i: "cc" * i})

for i in range(100):
    key = "aa" * i
    value = kv_index[key]
    assert value == "cc" * i

# check order of .items() after update
# "aa" 0 - 100 should now be last 100 items

for i, ((key, value), k, v) in enumerate(
    zip(kv_index.items(), kv_index.keys(), kv_index.values())
):
    assert key == k
    assert value == v if not isinstance(v, np.ndarray) else np.all(value == v)

    if i < 900:
        assert key == "aa" * (i + 100)
    else:
        assert key == "aa" * (i - 900)

# check del

for i in range(200, 300):
    del kv_index["aa" * i]

for i in range(200, 300):
    assert "aa" * i not in kv_index


# test getmulti

keys = ["aa" * i for i in range(150, 250)]

values = kv_index.getvalues(keys)

for i, (key, value) in enumerate(zip(keys, values)):
    assert (
        kv_index.get(key) == value
        if not isinstance(value, np.ndarray)
        else np.all(kv_index[key] == value)
    )


kv_index_with_lru = KVIndex(
    db_path="kv_index_example_with_lru.db",
    eviction=EvictionCfg(policy=EvictionCfg.EvictLRU, max_number_of_items=25),
)

for k, v in kv_index.items():
    kv_index_with_lru[k] = v

assert len(kv_index_with_lru) == 25

last_25_items_in_kv_index = list(kv_index.items())[-25:]

for i, ((key, value), k, v) in enumerate(
    zip(kv_index_with_lru.items(), kv_index_with_lru.keys(), kv_index_with_lru.values())
):
    assert key == k
    assert value == v if not isinstance(v, np.ndarray) else np.all(value == v)

    assert key == last_25_items_in_kv_index[i][0]
    assert (
        value == last_25_items_in_kv_index[i][1]
        if not isinstance(v, np.ndarray)
        else np.all(value == last_25_items_in_kv_index[i][1])
    )


kv_index_with_lfu = KVIndex(
    db_path="kv_index_example_with_lfu.db",
    eviction=EvictionCfg(policy=EvictionCfg.EvictLFU, max_number_of_items=25),
)

for k, v in kv_index.items():
    kv_index_with_lfu[k] = v

assert len(kv_index_with_lfu) == 25

last_25_items_in_kv_index = list(kv_index.items())[-25:]

for i, ((key, value), k, v) in enumerate(
    zip(kv_index_with_lfu.items(), kv_index_with_lfu.keys(), kv_index_with_lfu.values())
):
    assert key == k
    assert value == v if not isinstance(v, np.ndarray) else np.all(value == v)

    assert key == last_25_items_in_kv_index[i][0]
    assert (
        value == last_25_items_in_kv_index[i][1]
        if not isinstance(v, np.ndarray)
        else np.all(value == last_25_items_in_kv_index[i][1])
    )

# now access the first 10 items in kv_index_with_lfu 100 times
# and the last 10 items in kv_index_with_lfu 10 times

keys_in_kv_index_with_lfu = list(kv_index_with_lfu.keys())

for i in range(100):
    for j in range(10):
        kv_index_with_lfu[keys_in_kv_index_with_lfu[j]]

assert len(kv_index_with_lfu) == 25

# add 10 new items
for i in range(10):
    kv_index_with_lfu[f"new_key_{i}"] = "new_value"

assert len(kv_index_with_lfu) == 25

# check if the 10 new items are in the index
for i in range(10):
    assert f"new_key_{i}" in kv_index_with_lfu
