# LiteIndex
Embedded, thread and process safe, disk backed, easy to use, query-able, fast Index implementations

```python
pip install --upgrade liteindex
```

### DefinedIndex
- [Documentation](https://github.com/notAI-tech/LiteIndex/blob/main/DefinedIndex.md) | [Detailed example](https://github.com/notAI-tech/LiteIndex/blob/main/examples/defined_index_example.py) | [Benchmarks](https://github.com/notAI-tech/LiteIndex/tree/main/benchmarks/DefinedIndex)
- fixed schema index i.e: schema has to be defined before hand
- `number`, `boolean`, `datetime`, `string`, `compressed_string`, `blob`, `json` and `other` types are supported and can be queried upon
- can store any python objects with varying levels of query-ability
- Query language is copy of mongodb queries. All queries are documented
- works across threads, processes seamlessly
- compression is supported natively and optional custom compression dictionaries can be built


### function_cache
- [Documentation](https://github.com/notAI-tech/LiteIndex/blob/main/function_cache.md) | [Detailed example](https://github.com/notAI-tech/LiteIndex/blob/main/examples/function_cache_example.py) | [Benchmarks](https://github.com/notAI-tech/LiteIndex/tree/main/benchmarks/function_cache)
- based on DefinedIndex, easy to use decorator for caching heavy function calls with arguments
- works across threads, processes seamlessly
