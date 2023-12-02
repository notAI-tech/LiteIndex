# LiteIndex
Embedded, thread and process safe, disk backed, easy to use, query-able Index implementations

```python
pip install --upgrade liteindex
```

### DictIndex
[Documentation](https://github.com/notAI-tech/LiteIndex/blob/main/DictIndex.md) - [Benchmarks](https://github.com/notAI-tech/LiteIndex/blob/main/DictIndex.md) - [Examples](https://github.com/notAI-tech/LiteIndex/blob/main/DictIndex.md)

```python
from liteindex import DictIndex
dict_index = DictIndex("sample_directory")

dict_index["key"] = "value"
dict_index[2] = {"key": "value", "key2": "value2"}
dict_index["embedding"] = np.array([1,2,3,4,5])
```

- python dict like interface
- fastest of all, uses lmdb as backend
- can have any python object as key and value
- compression focused: all data is compressed before storing, custom dictionary for compression is also supported
- utility functions: query by value equality, pop or delete multiple at once

### DefinedIndex
- [Documentation](https://github.com/notAI-tech/LiteIndex/blob/main/DefinedIndex.md)
- Query language is copy of mongodb queries. All queries are documented
- strings, numbers, bools, blobs, flat lists and dicts, nested jsons and any other python objects can be stored and be queried upon at various levels
- works across threads, processes seamlessly
- 

### AnyIndex (***In development***)

- Like mongodb, any python dict can be inserted at run time
- Doesn't need pre-defined structure unlike DefinedIndex
