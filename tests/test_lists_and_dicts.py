from liteindex import DefinedIndex

schema = {
    "age": "number",
    "name": "string",
    "marks": "string:number",
    "friends_ids": "string[]",
}

index = DefinedIndex(name="test", schema=schema)

index.update(
    {
        "user_1": {
            "age": 20,
            "name": "John",
            "marks": {"math": 20, "english": 30},
            "friends_ids": ["user_2", "user_3"],
        },
        "user_2": {
            "age": 21,
            "name": "Jane",
            "marks": {"math": 20, "english": 30},
            "friends_ids": ["user_1", "user_3"],
        },
        "user_3": {
            "age": 22,
            "name": "Jack",
            "marks": {"math": 20, "english": 30},
            "friends_ids": ["user_1", "user_2"],
        },
        "user_4": {
            "age": 23,
            "name": "Jill",
            "friends_ids": ["user_1", "user_2", "user_3"],
        },
        "user_5": {
            "age": 24,
            "name": "Jim",
            "marks": {"math": 20, "english": 30},
        },
    }
)


print(
    index.get("user_1")["user_1"]
    == {
        "age": 20,
        "name": "John",
        "marks": {"math": 20, "english": 30},
        "friends_ids": ["user_2", "user_3"],
    }
)
