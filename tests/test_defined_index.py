from liteindex import DefinedIndex

# ---------- UNIT TESTS -----------

schema = {
    "name": "string",
    "age": "number",
    "password": "string",
    "email": "string",
    "email_verified": "boolean",
    "nicknames": "json",
    "address_details": "json",
    "profile_picture": "blob",
    "description_vector": "other",
}

index = DefinedIndex(name="user_details", schema=schema)

index.update(
    {
        "user1": {
            "name": "John Doe",
            "age": 25,
            "password": "password123",
            "email_verified": True,
            "nicknames": ["John", "Johnny"],
            "address_details": {
                "city": "New York",
                "state": "New York",
                "country": "USA",
            },
            "profile_picture": b"some binary data here",
        },
        "user2": {
            "name": "Jane Doe",
            "age": 22,
        },
    }
)

index.update(
    {
        "user3": {
            "name": "John Doe",
            "age": 25,
            "password": "password123",
            "email_verified": True,
            "nicknames": ["John", "Johnny"],
            "address_details": {
                "city": "New York",
                "state": "New York",
                "country": "USA",
            },
            "profile_picture": b"some binary data here    aaaa bbb",
        },
        "user4": {
            "name": "Jane Doe",
            "age": 22,
        },
    }
)

print("-->", index.list_optimized_keys())
index.optimize_key_for_querying("name")
print("-->", index.list_optimized_keys())

print(
    "---->",
    index.math("age", "sum", query={"age": {"$gt": 20}}),
    index.math("age", "sum", query={"age": {"$gt": 60}}),
)

# print(index.get("user1", "user2", "user3"))

print(index.search(query={"age": {"$gt": 20}}))

print("Get:", index.get(["user1", "user2", "user3", "user4"]))

print(index.distinct(key="name", query={"age": {"$gt": 20}}))

print(index.group(keys="name", query={"age": {"$gt": 20}}))

print(index.count(query={"age": {"$gt": 20}}), index.count())

index.delete(ids=["user1", "user2"])

print(index.group(keys="name", query={"age": {"$gt": 20}}))

print(index.count(query={"age": {"$gt": 20}}))

index.clear()

print(index.count(query={"age": {"$gt": 20}}), index.count())

index.drop()

# should throw exception
print(index.count(query={"age": {"$gt": 20}}))
