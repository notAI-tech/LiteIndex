import sys

sys.path.append("./")

from liteindex import KVIndex

index = KVIndex()

index["key1"] = "value1"
index["key2"] = "value2"
index["key3"] = "value3"

index.update(
    {
        "key4": "value4",
        "key5": "value5",
        "key6": 6,
        "key7": 7,
        "key8": 8,
        "key9": 9,
        "key10": 10,
        "keyTrue": True,
        "keyFalse": False,
        "keyNone": None,
    }
)

assert index.search("value1") == {"key1": "value1"}

assert index.search("value") == {}

assert index.search("value1", n=10, offset=0) == {"key1": "value1"}
assert index.search("value1", n=10, offset=1) == {}

assert index.search(None) == {"keyNone": None}
assert index.search(True) == {"keyTrue": True}

assert index.search({"$gt": 6}, n=10, offset=0) == {
    "key7": 7,
    "key8": 8,
    "key9": 9,
    "key10": 10,
}

assert index.search({"$gte": 6}, n=10, offset=0) == {
    "key6": 6,
    "key7": 7,
    "key8": 8,
    "key9": 9,
    "key10": 10,
}

