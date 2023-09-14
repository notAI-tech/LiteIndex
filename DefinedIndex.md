# DefinedIndex

**Initialize DefinedIndex**
```python
from liteindex import DefinedIndex

# Define the schema for the index
# strings, bools, ints, floats, and bytes, json serializable nested dicts, lists are supported natively
# those fields will be filterable, queryable, and sortable
# Any other fields will be stored as a blob and can be read and updated, but not filtered, queried, or sorted
schema = {
    "name": "",
    "age": 0,
    "password": "",
    "verified": False,
    "nicknames": [],
    "address_details": {},
    "profile_picture": b"",
}

# db path defaults to memory
# auto_key defaults to False
index = DefinedIndex(
            name="user_details",
            schema=schema,
            db_path="./test.liteindex"
        )
```

**Insert/Update data**
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

**Get Data**
```python
# get a single or multiple records
# a dict of format {key: record} is returned
# if a key is not found, it won't be in the returned dict
index.get("john_doe")
index.get("john_doe", "jane_doe")
```

**Delete Data**
```python
# delete a single record or multiple records
# returns dict of format {key: record} of deleted records
index.delete("john_doe", return_deleted=True)
# returns dict of format {key: {}}
index.delete("john_doe", "jane_doe", return_deleted=False)
```

**Drop or clear Index**
```python
index.clear()
index.drop()
```
