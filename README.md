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
index = DefinedIndex(name="people", schema=schema)

# Insert or update a single item in the index
index.set("1", {
    "name": "Alice",
    "age": 30,
    "address": {
        "street": "123 Main St",
        "city": "New York",
        "country": "USA"
    }
})

# Query the index to retrieve items based on certain conditions
# (e.g., age <= 25, city = "New York")
query = {
    "age": (None, 25),
    "address": {
        "city": "New York"
    }
}
results = index.search(query)

for result in results:
    print(f"ID: {result['id']}, Name: {result['name']}, Age: {result['age']}, Address: {result['address']}")

```
