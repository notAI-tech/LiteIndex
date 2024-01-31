# LiteIndex
Embedded, thread and process safe, disk backed, easy to use, query-able, fast Index implementations

```python
pip install --upgrade liteindex
```

### DefinedIndex
- [Documentation](https://github.com/notAI-tech/LiteIndex/blob/main/DefinedIndex.md) | [Detailed example](https://github.com/notAI-tech/LiteIndex/blob/main/examples/defined_index_example.py) | [Benchmarks](https://github.com/notAI-tech/LiteIndex/tree/main/benchmarks/DefinedIndex)
- fixed schema index i.e: schema has to be defined before hand
- `number`, `boolean`, `datetime`, `string`, `compressed_string`, `blob`, `json`, `normalized_embedding`, and `other` types are supported and `can be queried upon`
- can store any python objects with varying levels of query-ability
- `Query language is subset of mongodb's`. All queries are documented
- seamless, very fast nearest neighbor search with filtering. a practical wrapper of faiss + sqlite. Approximate nearest neighbor search is not supported. Intended for few million vector search
- works across threads, processes seamlessly
- handy features like search and update in single query, batch operation support for update, search, del, pop etc ..
- in-place math operations support wherever applicable i.e: += etc can be done in single query optimally
- compression is supported natively and optional custom compression dictionaries can be built automatically

### KVIndex
- [Documentation](https://github.com/notAI-tech/LiteIndex/blob/main/KVIndex.md) | [Detailed example](https://github.com/notAI-tech/LiteIndex/blob/main/examples/defined_index_example.py) | [Benchmarks](https://github.com/notAI-tech/LiteIndex/tree/main/benchmarks/DefinedIndex)
- simple key value store, can store any python objects, can be queried for equality, sorting, max, min etc wherever applicable
- in-place math operations support wherever applicable i.e: += etc can be done in single query optimally
- has a python dict like interface
- batch operation support for update, search, del, pop etc ..
- ultra fast, works across threads, processes seamlessly
- Eviction policies supported: `LRU`, `LFU`, `any` and age based invalidation and size, count based eviction

### function_cache
- [Documentation](https://github.com/notAI-tech/LiteIndex/blob/main/function_cache.md) | [Detailed example](https://github.com/notAI-tech/LiteIndex/blob/main/examples/function_cache_example.py) | [Benchmarks](https://github.com/notAI-tech/LiteIndex/tree/main/benchmarks/function_cache)
- based on KVIndex, can be used to cache function calls of any type
- batch inference caching friendly
