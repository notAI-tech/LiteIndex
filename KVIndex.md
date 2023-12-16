# KVIndex

- process, thread safe, mimics python dict interface with ACID properties
- support key-value pair with expiration time, LFU, LRU, FIFO, random; size or count based eviction

### Initialize
***params***
- `db_path`: path to the index file. `defaults to None`, in-memory index is created
- `store_key`: `defaults to True` if False, index can only be used for lookup (useful if your keys are large and no need to store them)
- `preserve_order`: `defaults to True` if False insert/update order is not preserved
- `ram_cache_mb`: size of the ram cache in MB. `defaults to 32`
- `eviction`: eviction policy to use. `defaults to EvictionCfg(EvictionCfg.EvictNone)`


```python
from liteindex import KVIndex
kv_index = KVIndex(db_path="./test.liteindex")
```

### Insert/Update single or multiple
- key or value can be any python object
- `update` is a batch operation

```python
kv_index["key1"] = "value1"
kv_index.update({"key2": "value2", "key3": "value3"})
```

### Get single or multiple
```python
kv_index["key1"]
kv_index.get("key1", "default_value")
kv_index.getvalues(["key1", "key2"])
```

### Delete single or multiple, clear
```python
del kv_index["key1"]
kv_index.delete(["key1", "key2"])

kv_index.clear()
```

### Iteration and reverse iteration
```python
for key in kv_index: pass

for key in kv_index.keys(): pass
for value in kv_index.values(): pass
for key, value in kv_index.items(): pass

# reverse
for key in kv_index.keys(reverse=True): pass
for value in kv_index.values(reverse=True): pass
for key, value in kv_index.items(reverse=True): pass
```

### len, contains
```python
len(kv_index)
"key1" in kv_index
```

### EvictionCFG
- EvictionCfg class is used to configure eviction policy
- `EvictNone`: no eviction
- `EvictLRU`: least recently used
- `EvictLFU`: least frequently used in order of last update or insert
- `EvictAny`: FIFO if preserve_order is True, random otherwise
- `max_size_in_mb`: 0 default, max size of the index in MB
- `max_number_of_items`: 0 default, max number of items in the index
- `invalidate_after_seconds`: 0 default, max age of an item in seconds

- only one of `max_size_in_mb`, `max_number_of_items` can be set to non-zero value
- `invalidate_after_seconds` works along with all eviction policies, including `EvictNone`

```python
from liteindex import EvictionCfg
eviction_cfg = EvictionCfg(
    eviction_policy=EvictionCfg.EvictNone, # EvictNone, EvictLRU, EvictLFU, EvictAny
    max_size_in_mb=0, # 0 means no size based eviction
    max_number_of_items=0, # 0 means no count based eviction
    invalidate_after_seconds=0, # 0 means no time based eviction
)
```




