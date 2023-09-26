# LiteIndex
in-process, thread and process safe, easy to use, query-able object storage implenetation for Python

### Some use cases, benchmarks and examples:
- cache for a python function
- store and query large number of records without using up memory
- ultra-fast, query-able file storage (faster than creating files on disk and way better for querying)
- exchange data between threads or processes easily
- store application data locally in easy to query and stable format



```python
pip install --upgrade liteindex
```

## DefinedIndex

- [Documentation](https://github.com/notAI-tech/LiteIndex/blob/main/DefinedIndex.md)
- `schema` has to be specified when creating a new DefinedIndex
- Any python object can be stored in DefinedIndex
- Querying can be done on `string`, `number`, `boolean`, `json`, `datetime` fields.
- `blob` and `other` values can only be set, deleted, updated. Cannot be queried upon.
- Insertion order is preserved 

```python
from liteindex import DefinedIndex

user_details_schema = {
    "name": "string",
    "age": "number",
    "password": "string",
    "verified": "boolean",
    "address_details": "json",
    "profile_picture": "blob",
    "tag_line": "string",
    "tag_line_embedding": "other",
    "birth_day": "datetime"
}

# db path defaults to memory
index = DefinedIndex(
            name="user_details",
            schema=schema,
            db_path="./test_db.liteindex"
        )
```
- [DefinedIndex Documentation](https://github.com/notAI-tech/LiteIndex/blob/main/DefinedIndex.md)


## AnyIndex (***In development***)

- Like mongodb, any python dict can be inserted at run time
- Doesn't need pre-defined structure unlike DefinedIndex
