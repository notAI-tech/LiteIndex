# LiteIndex
fast, thread and process safe, easily queryable Indexes for Python.


# DefinedIndex

**Initialize the DefinedIndex.**
- if `db_path` defaults to in-memory.

```python
from liteindex import DefinedIndex

# Define the schema for the index
schema = {
    "name": "",
    "age": 0,
    "password": "",
    "verified": False
}

# Create a DefinedIndex instance
index = DefinedIndex(name="people", schema=schema, db_path="./test.liteindex")
```

**Set, Delete**
```python
# set can be partial or full, can be an existing key or new key.
index.set("alice", {
    "name": "Alicee",
    "password": "xxxjjssjsjsjsksk",
    "age": 30
})

# Set value for a sub key
index.set(("alice", "name"), "Alice")

# Same as
index.set("alice", {"name": "Alice"})

# Get value of a key
index.get("alice")

# Get value of a sub key
index.get("alice", "name")

# Delete
```
