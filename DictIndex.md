### DictIndex

- memory mapped, persistent, Key-Value store
- thread and process safe
- `lookup by value` is supported
- Works like a Python dict
- keys and values can be anything
- updation order is preserved

#### Example

```python
from liteindex import KVIndex

index = KVIndex(dir='myindex')

# Anything can be used as key and value
index['key'] = 'value'
index[np.array([1,2,3])] = ["some list example"]
index[[1, 2, "4"]] = {"some": "dict example"}

# batch update

```
