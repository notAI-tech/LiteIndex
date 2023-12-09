# DefinedIndex

### Schema

| Type      | Description   |
| ----------- | ----------- |
| boolean    | boolean values       |
| number   | any type of numbers. int, float ..        |
| string   | any type of texts        |
| compressed_string   | compressed text, queryable only for equality        |
| datetime   | datetime.datetime objects        |
| blob   | files as bytes, queryable for equality and sorted on size |
| json   | any type of json dump-able objects        |
| other   | any objects. stored as pickled blobs internally, queryable for equality and sorted on size|


- A schema has to be specified at first initialisation of the index and cannot be modified later on
- data is accessed or updated as a dict of the format {id: record} where record is a dict of format following schema and id is a string
- keys of record /schema can be anything that can be keys in a python dict. eg: `schema_1 = {0: "string", "a": "number"}`
- An in-memory index is created by default, i.e: no `db_path` is specified, and cannot be accessed from other processes and threads
- If `db_path` is specified, disk-based index is initiated which is accessible from all processes, threads and is persistent
- `None` is allowed as a value for all keys and is the default value for all keys

### Initialize
- params

| Param      | Description   | Default   |
| ----------- | ----------- | ----------- |
| name    | name of the index       | None        |
| schema   | schema, will be loaded if index exists, otherwise has to be provided        | None        |
| db_path   | path to the index file. If None, in-memory index is created        | None        |
| ram_cache_mb   | size of the ram cache in MB        | 64        |
| compression_level   | compression level for strings, blobs etc        | -1        |


- example

```python
from liteindex import DefinedIndex

schema = {
    "name": "string",
    "age": "number",
    "password": "string",
    "verified": "boolean",
    "birthday": "datetime",
    "profile_picture": "blob",
    "nicknames": "json",
    "user_embedding": "other",
    "user_bio": "compressed_string",
}

index = DefinedIndex(
            name="user_details",
            schema=schema,
            db_path="./test.liteindex"
        )
```

### Insert/Update
- Insert or update single or multiple, partial or full records at once
- Is atomic operation
- input format: `{id: record, id1: record, ....}`

```python
index.update(
    {
        "john_doe": {
            "name": "John Doe",
            "age": 25,
            "password": "password",
            "verified": True,
            "birthday": datetime.datetime(1995, 1, 1),
            "profile_picture": b"......",
            "nicknames": ["John", "Doe"],
            "user_embedding": np.array([1, 2, 3]),
            "user_bio": "This is a long string that will be compressed and stored"
        },
        "jane_doe": {
                "name": "Jane Doe",
                "age": 28,
                "verified": True,
            }
    }
)
```

### Get
- accepts a single key or a list of keys
- always  returns a dict of format `{id: record, id1: record, ....}`
```python
index.get("john_doe")
# {"john_doe": record_for_john_doe}
index.get(["john_doe", "jane_doe"])
# {"john_doe": record_for_john_doe, "jane_doe": record_for_jane_doe}
```

### Delete
```python
# delete a single record or multiple records or by query
# returns dict of format {key: record} of deleted records
index.delete("john_doe")
index.delete(["john_doe", "jane_doe"])
index.delete(query={"name": "John Doe"})
```

### Drop or clear Index
```python
# clear the index
index.clear()

# drop/ delete the index completely
index.drop()
```

### Search
- `query`: Query dictionary. Defaults to `{}` which will return all records. 
[Full list of queries supported](https://github.com/notAI-tech/LiteIndex/blob/main/Query.md)

- `sort_by`: A key from schema. Defaults to `updated_at` which will return records in insertion order.
- `reversed_sort`: Defaults to `False`. If `True`, will return records in reverse order.
- `n`: Defaults to `None` which will return all records.
- `page_no`: Defaults to `1` which will return the first page of results.
- `select_keys`: A list of keys from schema. Defaults to `None` which will return all keys.
- `update`: Optional dictionary of format `{key: record}`. If provided, will update the records in the index that match the query and return the updated records.

[Full list of queries supported](https://github.com/notAI-tech/LiteIndex/blob/main/Query.md)

```python
index.search()
# Returns {id_1: record_1, id_2: record_2, ...}
# Each record is a dict of format following schema, as inserted.
# By default ordered by insertion order, and returns all records.

index.search(
    query={"name": "John Doe"},
    sort_by="age",
    reversed_sort=True,
    n=10,
    page_no=1,
    select_keys=["name", "age"],
    update={"verified": True}
)
```

### Count
``` python
index.count()
index.count({"name": "Joe Biden"})
```


### Distinct
```python
index.distinct("name")
index.distinct("name", query={"gender": "female"})
```

### Group by
``` python
index.group()
```

### Optimize for Search
```python
index.optimize_for_query()
```
