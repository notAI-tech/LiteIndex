import time

from liteindex import KVIndex
from diskcache import Index 
from pyscript import display

kv_index = KVIndex("kv.db")
dc_index = Index("dc", eviction_policy="none")

TIMES = {
    "insert": {
        "liteindex": 0,
        "diskcache": 0
    },
    "update_existing": {
        "liteindex": 0,
        "diskcache": 0
    },
    "read": {
        "liteindex": 0,
        "diskcache": 0
    },
    "iterate": {
        "liteindex": 0,
        "diskcache": 0,
    },
    "delete": {
        "liteindex": 0,
        "diskcache": 0,
    }
}

N = 10000
print(f"Benching small string keys and values, {N} items")

print("Liteindex: KVIndex Insert test running")
# insert
s = time.time()
for _ in range(1, N):
    _ = str(_)
    kv_index[_] = _
TIMES["insert"]["liteindex"] = (time.time() - s)/N

print("Diskcache: Index Insert test running")
s = time.time()
for _ in range(1, N):
    _ = str(_)
    dc_index[_] = _
TIMES["insert"]["diskcache"] = (time.time() - s)/N

print("Liteindex: KVIndex Update Existing test running")
# update existing
s = time.time()
for _ in range(1, N):
    _ = str(_ + 1)
    kv_index[_] = _
TIMES["update_existing"]["liteindex"] = (time.time() - s)/N

print("Diskcache: Index Update Existing test running")
s = time.time()
for _ in range(1, N):
    _ = str(_ + 1)
    dc_index[_] = _
TIMES["update_existing"]["diskcache"] = (time.time() - s)/N

print("Liteindex: KVIndex Read test running")
# read
s = time.time()
for _ in range(1, N):
    _ = str(_)
    _ = kv_index[_]
TIMES["read"]["liteindex"] = (time.time() - s)/N

print("Diskcache: Index Read test running")
s = time.time()
for _ in range(1, N):
    _ = str(_)
    _ = dc_index[_]
TIMES["read"]["diskcache"] = (time.time() - s)/N

print("Liteindex: KVIndex Iterate test running")
# iterate items
s = time.time()
for _ in kv_index.items():
    pass
TIMES["iterate"]["liteindex"] = (time.time() - s)/N

print("Diskcache: Index Iterate test running")
s = time.time()
for _ in dc_index.items():
    pass
TIMES["iterate"]["diskcache"] = (time.time() - s)/N

print("Liteindex: KVIndex Delete test running")
# delete
s = time.time()
for _ in range(1, N):
    _ = str(_)
    del kv_index[_]
kv_index.vaccum()
TIMES["delete"]["liteindex"] = (time.time() - s)/N

print("Diskcache: Index Delete test running")
s = time.time()
for _ in range(1, N):
    _ = str(_)
    del dc_index[_]
TIMES["delete"]["diskcache"] = (time.time() - s)/N

del kv_index
del dc_index

# print sizes of databases
import os
print(f"Liteindex: KVIndex size: {os.path.getsize('kv.db')}")
print(f"Diskcache: Index size: {os.path.getsize('dc/cache.db')}")

import matplotlib.pyplot as plt
import numpy as np

# Extract operations and their respective times for liteindex and diskcache
operations = list(TIMES.keys())
liteindex_times = [TIMES[op]["liteindex"] * 1000 for op in operations]
diskcache_times = [TIMES[op]["diskcache"] * 1000 for op in operations]

# Setting the positions and width for the bars
pos = np.arange(len(operations))
bar_width = 0.35

# Create subplots
fig, ax = plt.subplots()

# Plotting the bars
ax.bar(pos, liteindex_times, bar_width, label='Liteindex', color='blue')
ax.bar(pos + bar_width, diskcache_times, bar_width, label='Diskcache', color='green')

# Adding labels and title
ax.set_xlabel('Operations')
ax.set_ylabel('Time (ms)')
ax.set_title('Avg time in milliseconds per operation')
ax.set_xticks(pos + bar_width / 2)
ax.set_xticklabels(operations)

# Adding a legend
ax.legend()

# Display the figure in the div with id 'mpl'
display(fig, target="mpl")
