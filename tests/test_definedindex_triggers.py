from liteindex import DefinedIndex, DefinedTypes

index = DefinedIndex(
    "test",
    schema={
        "a": DefinedTypes.number,
        "b": DefinedTypes.string,
    }
)

index.create_trigger(
    "test",
    for_key="a",
    function_to_trigger=print,
    after_set=True
)

index.update(
    {
        "1": {
            "a": 1,
            "b": "test"
        }
    }
)
