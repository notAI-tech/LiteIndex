# LiteIndex
Embedded, thread and process safe, disk backed, easy to use, query-able, fast Index implementations

```python
pip install --upgrade liteindex
```

### DefinedIndex
- [Documentation](https://github.com/notAI-tech/LiteIndex/blob/main/DefinedIndex.md) | [Detailed example](https://github.com/notAI-tech/LiteIndex/blob/main/examples/defined_index_example.py) | [Benchmarks](https://github.com/notAI-tech/LiteIndex/tree/main/benchmarks/DefinedIndex)
- fixed schema index i.e: schema has to be defined before hand
- `number`, `boolean`, `datetime`, `string`, `compressed_string`, `blob`, `json` and `other` types are supported and `can be queried upon`
- can store any python objects with varying levels of query-ability
- `Query language is subset of mongodb's`. All queries are documented
- works across threads, processes seamlessly
- compression is supported natively and optional custom compression dictionaries can be built

### KVIndex
- [Documentation](https://github.com/notAI-tech/LiteIndex/blob/main/KVIndex.md) | [Detailed example](https://github.com/notAI-tech/LiteIndex/blob/main/examples/defined_index_example.py) | [Benchmarks](https://github.com/notAI-tech/LiteIndex/tree/main/benchmarks/DefinedIndex)
- simple key value store, can store any python objects, no querying supported
- ultra fast, works across threads, processes seamlessly
- Eviction policies supported: `LRU`, `LFU`, `any` and age based invalidation and size, count based eviction

### function_cache
- [Documentation](https://github.com/notAI-tech/LiteIndex/blob/main/function_cache.md) | [Detailed example](https://github.com/notAI-tech/LiteIndex/blob/main/examples/function_cache_example.py) | [Benchmarks](https://github.com/notAI-tech/LiteIndex/tree/main/benchmarks/function_cache)
- based on KVIndex, can be used to cache function calls of any type
- batch inference caching friendly
