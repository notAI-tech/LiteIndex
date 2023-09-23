# LiteIndex
in-process, easy to use, query-able object storage implenetations based on sqlite


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
