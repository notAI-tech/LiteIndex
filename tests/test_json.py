from liteindex import DefinedIndex, DefinedTypes

index = DefinedIndex(
    "json_test",
    schema={
        "json_key": DefinedTypes.json,
        "age": DefinedTypes.number,
        "name": DefinedTypes.string,
    }
)

index.update(
    {
        "k1": {
            "json_key": {"a": 1, "b": 2},
        }
    }
)

index.update(
    {
        "k2": {
            "json_key": {"e": 3, "f": 4, "a": 2},
        }
    }
)

index.update(
    {
        "k3": {
            "json_key": ["c", "d"],
        }
    }
)

index.update(
    {
        "k4": {
            "json_key": {"a": None, "b": 2},
        }
    }
)

index.update(
    {
        "k5": {
            "json_key": {"a": "a", "b": 2},
        }
    }
)

# print(index.search({"json_key": {"$in": ["a", "c"]}}))

assert list(index.search({"json_key": {"a": 1}}).keys()) == ["k1"]

assert list(index.search({"json_key": {"a": None}}).keys()) == ["k4"]

assert list(index.search({"json_key": {"a": {"$gte": 1}}}).keys()) == ["k1", "k2", "k5"]

# assert list(index.search({"json_key": {"a": {"$in": [-1, 99]}}}).keys()) == []

assert list(index.search({"json_key": {"a": {"$ne": 1}}}).keys()) == ["k2", "k3", "k4", "k5"]

assert list(index.search({"json_key": {"a": {"$ne": None}}}).keys()) == ["k1", "k2", "k5"]

print(index.distinct("json_key"))
