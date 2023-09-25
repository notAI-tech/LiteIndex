# DefinedIndex

### Schema

| Type      | Description   |
| ----------- | ----------- |
| boolean    | use for storing boolean values       |
| number   | use for storing any type of numbers. int, float ..        |
| string   | use for storing any type of texts        |
| json   | use for storing any type of json dump-able objects. lists, dicts        |
| blob   | use for storing files as bytes        |
| datetime   | use for storing datetime.datetime objects        |
| other   | use for any other type of objects that are not in the ones above        |


- A schema has to be specified at first initialisation of the index
- schema cannot be modified later on
- keys in schema can be anything that can be keys in a python dict. eg: `schema_1 = {0: "string", "a": "number"}`
- id of the records has to be a string. eg: `index.set({"a": {0: "0"}}) is allowed` but `index.set({0: {0: "0"}}) is not allowed`
- An in-memory index cannot be accessed from other processes and threads
- If `db_path` is specified, disk-based index is initiated which is accessible from all processes, threads and is persistent

### Initialize DefinedIndex
```python
from liteindex import DefinedIndex

# Define the schema for the index
# strings, bools, ints, floats, bytes, datetime objects, json serializable nested dicts and lists are supported natively
# those fields will be filterable, queryable, and sortable
# Any other fields will be stored as a blob and can be read and updated, but not filtered, queried, or sorted
# keys of schema can be anything python allows as a dict key
schema = {
    "name": "string",
    "age": "number",
    "password": "string",
    "verified": "boolean",
    "nicknames": "json",
    "address_details": "json",
    "profile_picture": "blob",
    "tag_line": "string",
    "tag_line_embedding": "other",
    "birthday": "datetime"
}

# db path defaults to memory
# auto_key defaults to False
index = DefinedIndex(
            name="user_details",
            schema=schema,
            db_path="./test.liteindex"
        )
```

### Insert/Update data
```python
# Set a single or multiple, partial or full records.
index.update(
    {
        "john_doe": {
            "name": "John Doe",
            "age": 25,
            "password": "password",
            "verified": True,
            "nicknames": ["John", "Doe"],
            "address_details": {
                "street": "123 Fake Street",
                "city": "Springfield",
                "state": "IL",
                "zip": "12345"
            },
            "profile_picture": b"......"
        },
        "jane_doe": {
                "name": "Jane Doe",
                "age": 28,
                "verified": True,
            }
    }
)
```

### Get Data
```python
# get a single or multiple records
# a dict of format {key: record} is returned
# if a key is not found, it won't be in the returned dict
index.get("john_doe")
# {"john_doe": record_for_john_doe}
index.get(["john_doe", "jane_doe"])
# {"john_doe": record_for_john_doe, "jane_doe": record_for_jane_doe}
```

### Delete Data
```python
# delete a single record or multiple records
# returns dict of format {key: record} of deleted records
index.delete("john_doe")
index.delete(["john_doe", "jane_doe"])
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
    select_keys=["name", "age"]
)
```
