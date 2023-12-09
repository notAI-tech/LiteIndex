# LiteIndex
Embedded, thread and process safe, disk backed, easy to use, query-able, fast Index implementations

```python
pip install --upgrade liteindex
```

### DefinedIndex
- [Documentation](https://github.com/notAI-tech/LiteIndex/blob/main/DefinedIndex.md)
- fixed schema index i.e: schema has to be defined before hand
- `number`, `boolean`, `datetime`, `string`, `compressed_string`, `blob`, `json` and `other` types are supported and can be queried upon
- Query language is copy of mongodb queries. All queries are documented
- works across threads, processes seamlessly
- compression is supported natively and optional custom compression dictionaries can be built



### function_cache
- [Documentation](https://github.com/notAI-tech/LiteIndex/blob/main/function_cache.md)
- A decorator to cache function calls with arguments
