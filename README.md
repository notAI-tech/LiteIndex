# LiteIndex
Embedded, thread and process safe, easy to use, query-able document store for Python

### Some use cases, benchmarks and examples:
- persistent, fast in-process caches and query-able queues
- store and query large number of records without using up memory
- ultra-fast, query-able file storage (faster than creating files on disk and way better for querying)
- exchange data between threads or processes easily
- store application data locally in easy to query and stable format
- Trigger threads/ processes with your custom functions on any specific updates or deletes (combined with gevent or ray, this allows for a extremely easy to use and deploy sqs + lambda like pipeline)
- easily import from csv, tsv, jsonl, any iterable and run queries



```python
pip install --upgrade liteindex
```

## DefinedIndex

- [Documentation](https://github.com/notAI-tech/LiteIndex/blob/main/DefinedIndex.md)
- Query language is copy of mongodb queries. All queries are documented
- strings, numbers, bools, blobs, flat lists and dicts, nested jsons and any other python objects can be stored and be queried upon at various levels
- works across threads, processes seamlessly
- 

## AnyIndex (***In development***)

- Like mongodb, any python dict can be inserted at run time
- Doesn't need pre-defined structure unlike DefinedIndex
