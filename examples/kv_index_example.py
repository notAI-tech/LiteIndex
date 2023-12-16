import sys
sys.path.append('../')

import random
import numpy as np

from liteindex import KVIndex

# Create a new index
kv_index = KVIndex(
                    db_path='kv_index_example.db' # if not specified, an in memory db is used
            )

# string keys and any value example

for i in range(1000):
    choice = random.randint(0, 2)
    if choice == 0:
        kv_index["aa" * i] = "bb" * (100 - i)
    elif choice == 1:
        kv_index["aa" * i] = i ** 2
    else:
        kv_index["aa" * i] = np.array([i, i + 1, i + 2])

# get all keys and check if the value is correct
for i in range(1000):
    key = "aa" * i
    value = kv_index[key]
    if isinstance(value, str):
        assert value == "bb" * (100 - i)
    elif isinstance(value, int):
        assert value == i ** 2
    else:
        assert np.all(value == np.array([i, i + 1, i + 2]))
    

# check if .items() is returning in insertion order and keys and values are correct
for i, (key, value) in enumerate(kv_index.items()):
    assert key == "aa" * i
    if isinstance(value, str):
        assert value == "bb" * (100 - i)
    elif isinstance(value, int):
        assert value == i ** 2
    else:
        assert np.all(value == np.array([i, i + 1, i + 2]))

# check if .keys and .values are returning in insertion order and keys and values are correct
for i, key in enumerate(kv_index.keys()):
    assert key == "aa" * i

for i, value in enumerate(kv_index.values()):
    if isinstance(value, str):
        assert value == "bb" * (100 - i)
    elif isinstance(value, int):
        assert value == i ** 2
    else:
        assert np.all(value == np.array([i, i + 1, i + 2]))

# check if len is correct
assert len(kv_index) == 1000


