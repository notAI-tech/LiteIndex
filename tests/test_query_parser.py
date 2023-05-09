from defined_index import DefinedIndex

m = DefinedIndex("a", {"a": 10})
_process_query_conditions = m._process_query_conditions

def test_process_query_conditions():
    # Test simple equality query
    where_conditions, params = _process_query_conditions(query={"age": 30})
    assert where_conditions == ["age = ?"]
    assert params == [30]

    # Test simple range query
    where_conditions, params = _process_query_conditions(query={"age": (30, 40)})
    assert where_conditions == ["age >= ?", "age <= ?"]
    assert params == [30, 40]

    # Test simple inclusion query
    where_conditions, params = _process_query_conditions(query={"age": [11, 14, 16]})
    assert where_conditions == ["age IN (?, ?, ?)"]
    assert params == [11, 14, 16]

    # Test nested JSON range query
    where_conditions, params = _process_query_conditions(query={"birth_day": {"year": (None, 2019)}})
    assert where_conditions == ["json_extract(birth_day, '$.year') <= ?"]
    assert params == [2019]

    # Test nested JSON inclusion query
    where_conditions, params = _process_query_conditions(query={"birth_day": {"year": [2017, 2018, 2019]}})
    assert where_conditions == ["json_extract(birth_day, '$.year') IN (?, ?, ?)"]
    assert params == [2017, 2018, 2019]

    # Test multiple top-level conditions
    where_conditions, params = _process_query_conditions(query={"age": 30, "city": "New York"})
    assert set(where_conditions) == {"age = ?", "city = ?"}
    assert set(params) == {30, "New York"}

    # Test deeply nested JSON range query
    where_conditions, params = _process_query_conditions(query={"person": {"birth_day": {"year": (None, 2019)}}})
    assert where_conditions == ["json_extract(person, '$.birth_day.year') <= ?"]
    assert params == [2019]

    # Test deeply nested JSON inclusion query
    where_conditions, params = _process_query_conditions(query={"person": {"birth_day": {"year": [2017, 2018, 2019]}}})
    assert where_conditions == ["json_extract(person, '$.birth_day.year') IN (?, ?, ?)"]
    assert params == [2017, 2018, 2019]

    # Test a mix of different query types
    where_conditions, params = _process_query_conditions(query={
        "age": (30, 40),
        "city": "New York",
        "birth_day": {"year": (None, 2019)},
        "gender": ["male", "female"]
    })
    assert set(where_conditions) == {
        "age >= ?",
        "age <= ?",
        "city = ?",
        "json_extract(birth_day, '$.year') <= ?",
        "gender IN (?, ?)"
    }
    assert set(params) == {30, 40, "New York", 2019, "male", "female"}

test_process_query_conditions()
