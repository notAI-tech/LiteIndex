# LiteIndex
ultra fast, thread and process safe, easily queryable Indexes for Python.


# DefinedIndex

```python
from liteindex import DefinedIndex

# Define the schema for the index
schema = {
    "name": "",
    "age": 0,
    "address": {
        "street": "",
        "city": "",
        "country": ""
    }
}

# Create a DefinedIndex instance
index = DefinedIndex(name="people", schema=schema) #optional: db_path="path to a file" #auto_key=False or True. 

# Insert or update a single item in the index if auto_key=False (default)
index.set(user_name, {
    "name": "Alice",
    "password": "xxxjjssjsjsjsksk",
    "age": 30,
    "address": {
        "street": "123 Main St",
        "city": "New York",
        "country": "USA"
    }
})

# If auto_key=True
integer_id = index.add({
    "name": "Alice",
    "password": "xxxjjssjsjsjsksk",
    "age": 30,
    "address": {
        "street": "123 Main St",
        "city": "New York",
        "country": "USA"
    }
})

# Set full or partial value for a key
index.set(key, value)

# Set value for a key
index.set((key, sub_key), value)

# Get value of a key
index.get(key)

# Get value of a sub key
index.get(key, sub_key)

# index.search()

# index.count()

# index.delete()

# index.drop()

```
