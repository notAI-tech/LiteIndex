from test_kv_index_trigger_1 import index

index["key1"] = "set_in_test_2"

# index.create_trigger("test_multi_trigger", for_key=None, for_keys=["k1", "k2"], function_to_trigger=print, after_set=True)

# index["k1"] = "set_in_test_2"
# index["k2"] = "set_in_test_2"
