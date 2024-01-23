import sys

sys.path.append(".")

from liteindex import KVIndex

index = KVIndex("test_trigger_kv_index.db")


def on_key_1_set():
    print("Key 1 set to", index["key1"])


def on_key_1_before_set():
    print("Key 1 before set", index.get("key1"))


def on_key_1_del():
    print("Key 1 deleted", index["key1"])


index.create_trigger(
    "key_1_set", for_key="key1", function_to_trigger=on_key_1_set, after_set=True
)
index.create_trigger(
    "key_1_before_set",
    for_key="key1",
    function_to_trigger=on_key_1_before_set,
    before_set=True,
)
index.create_trigger(
    "key_1_del", for_key="key1", function_to_trigger=on_key_1_del, before_delete=True
)

index["key1"] = "value1"
index["key2"] = "value2"

index["key1"] = "value1_updated"
index["key1"] = 2
index["key1"] = {"key": "value"}
del index["key1"]

print("key1" in index)

index["key1"] = "set_after_delete"
