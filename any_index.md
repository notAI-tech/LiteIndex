# AnyIndex

`AnyIndex` is a flexible, SQLite3 based key-value store class that supports indexing and searching JSON objects with nested structures. It is a lightweight and easy-to-use alternative to full-fledged databases like MongoDB when you need to store and query hierarchical data.

## Features

- Indexing and searching on nested JSON objects and arrays
- Inserting, updating, and deleting keys and nested values
- Searching and indexing by nested keys
- Sorting and filtering by nested keys
- Aggregations and analytics on nested keys

## Usage

Here's a simple guide on how to use the `AnyIndex` class:

### Creating an instance

```python
from liteindex import AnyIndex

index = AnyIndex("my_index")
```

### Inserting data

```python
data = {
    "user1": {
        "name": "Alice",
        "age": 30,
        "scores": [85, 90, 95],
        "address": {
            "city": "New York",
            "state": "NY"
        }
    },
    "user2": {
        "name": "Bob",
        "age": 25,
        "scores": [70, 75, 80],
        "address": {
            "city": "Los Angeles",
            "state": "CA"
        }
    },
    "user3": {
        "title": "Employee",
        "department": "HR",
        "projects": [
            {"name": "Project A", "duration": 3},
            {"name": "Project B", "duration": 6}
        ]
    },
    "user4": {
        "title": "Manager",
        "department": "Finance",
        "projects": [
            {"name": "Project C", "duration": 12},
            {"name": "Project D", "duration": 24}
        ]
    },
    "user5": "John",
    "user6": 42
}

index.update(data)
```

### Getting and setting values

```python
# Get a value
value = index["user1"]  # Returns the value of the key "user1"

# Set a value
index["user5"] = "Jane"
```

### Searching and indexing

```python
# Search by nested key
results = index.search_by_key(["address", "city"])
# Returns an iterator of (key, value) tuples where the nested key ["address", "city"] is present

# Search by nested value
results = index.search_by_value(["address", "city"], "New York")
# Returns an iterator of (key, value) tuples where the nested key ["address", "city"] has the value "New York"

# Search by nested value in a list
results = index.search_value_in_list(["projects"], {"name": "Project A", "duration": 3})
# Returns an iterator of (key, value) tuples where the list at the nested key ["projects"] contains the specified value

# Create an index on a nested key
index.create_index(["department"])
# Creates an index for the nested key ["department"]

# Get sorted items by a nested key
sorted_results = index.get_sorted_by_key(["age"])
# Returns an iterator of (key, value) tuples sorted by the nested key ["age"]
```

By using the `AnyIndex` class, you can efficiently store, index, and search values of different types, including nested dictionaries and lists, as demonstrated in the examples above. The class provides iterators of (key, value) tuples for search results, allowing for easy iteration and further processing.