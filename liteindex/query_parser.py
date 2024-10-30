import json
import pickle

try:
    from .defined_serializers import hash_bytes
except ImportError:
    from defined_serializers import hash_bytes

import json
import pickle

try:
    from .defined_serializers import hash_bytes
except ImportError:
    from defined_serializers import hash_bytes


def parse_query(query, schema, prefix=None):
    where_conditions = []
    params = []

    if prefix is None:
        prefix = []

    def process_nested_query(value, prefix):
        nonlocal where_conditions, params
        conditions = (
            []
        )  # Local conditions list instead of using where_conditions directly

        # Determine if we're dealing with a JSON field
        is_json_field = schema.get(prefix[0]) == "json" if prefix else False

        # Build column reference
        if is_json_field and len(prefix) > 1:
            column = f"json_extract({prefix[0]}, '$.{'.'.join(prefix[1:])}')"
        else:
            column = f'"{prefix[0]}"' if not is_json_field else prefix[0]

        column_type = schema.get(prefix[0])

        if isinstance(value, dict):
            sub_conditions = []
            for sub_key, sub_value in value.items():
                # Handle special operators
                if sub_key in ["$ne", "$like", "$gt", "$lt", "$gte", "$lte", "$eq"]:
                    # Special handling for NULL values
                    if sub_value is None:
                        if sub_key == "$ne":
                            sub_conditions.append(f"({column} IS NOT NULL)")
                        elif sub_key == "$eq":
                            sub_conditions.append(f"{column} IS NULL")
                        continue

                    operator = {
                        "$ne": "!=",
                        "$eq": "=",
                        "$like": "LIKE",
                        "$gt": ">",
                        "$lt": "<",
                        "$gte": ">=",
                        "$lte": "<=",
                    }[sub_key]

                    if is_json_field and sub_key == "$like":
                        sub_conditions.append(
                            f"EXISTS(SELECT 1 FROM json_each({column}) WHERE value {operator} ?)"
                        )
                    else:
                        if sub_key == "$ne":
                            sub_conditions.append(
                                f"({column} {operator} ? OR {column} IS NULL)"
                            )
                        else:
                            sub_conditions.append(f"{column} {operator} ?")

                    processed_value = sub_value
                    if column_type == "other":
                        processed_value = hash_bytes(
                            pickle.dumps(sub_value, protocol=pickle.HIGHEST_PROTOCOL)
                        )
                    elif column_type == "blob":
                        processed_value = hash_bytes(sub_value)
                    elif column_type == "datetime":
                        processed_value = sub_value.timestamp()
                    elif column_type == "boolean":
                        processed_value = int(sub_value)
                    elif column_type == "compressed_string":
                        processed_value = sub_value.encode()
                        # TODO: Handle compressed strings
                        pass

                    params.append(processed_value)

                elif sub_key == "$in":
                    if is_json_field:
                        json_conditions = []
                        for val in sub_value:
                            json_conditions.append(
                                f"EXISTS(SELECT 1 FROM json_each({column}) WHERE value = ?)"
                            )
                            params.append(val)
                        sub_conditions.append(f"({ ' OR '.join(json_conditions) })")
                    else:
                        placeholders = ", ".join(["?" for _ in sub_value])
                        sub_conditions.append(
                            f"({column} IN ({placeholders}) OR {column} IS NULL)"
                        )
                        params.extend(sub_value)

                elif sub_key == "$nin":
                    if is_json_field:
                        json_conditions = []
                        non_null_values = [v for v in sub_value if v is not None]
                        has_null = None in sub_value

                        for val in non_null_values:
                            json_conditions.append(
                                f"NOT EXISTS(SELECT 1 FROM json_each({column}) WHERE value = ?)"
                            )
                            params.append(val)

                        _conditions = (
                            [f"({ ' AND '.join(json_conditions) })"]
                            if json_conditions
                            else []
                        )
                        if has_null:
                            _conditions.append(
                                f"json_extract({prefix[0]}, '$') IS NOT NULL"
                            )

                        sub_conditions.append(f"({ ' AND '.join(_conditions) })")
                    else:
                        non_null_values = [v for v in sub_value if v is not None]
                        has_null = None in sub_value

                        _conditions = []
                        if non_null_values:
                            placeholders = ", ".join(["?" for _ in non_null_values])
                            _conditions.append(f"{column} NOT IN ({placeholders})")
                            params.extend(non_null_values)

                        if has_null:
                            _conditions.append(f"{column} IS NOT NULL")

                        sub_conditions.append(f"({ ' AND '.join(_conditions) })")

                elif sub_key in ["$and", "$or"]:
                    # Handle nested logical operators
                    nested_conditions = []
                    for nested_value in sub_value:
                        nested_result = parse_query({"dummy": nested_value}, schema)[0]
                        if nested_result:
                            nested_conditions.append(
                                nested_result[0].replace('"dummy"', column)
                            )
                    if nested_conditions:
                        join_op = " OR " if sub_key == "$or" else " AND "
                        sub_conditions.append(f"({join_op.join(nested_conditions)})")

                else:
                    # Handle nested fields
                    nested_result = process_nested_query(sub_value, prefix + [sub_key])
                    if nested_result:
                        sub_conditions.extend(nested_result)

            if sub_conditions:
                conditions.extend(sub_conditions)

        elif isinstance(value, list):
            if is_json_field:
                json_conditions = []
                for val in value:
                    json_conditions.append(
                        f"EXISTS(SELECT 1 FROM json_each({column}) WHERE value = ?)"
                    )
                    params.append(json.dumps(val))
                conditions.append(f"({ ' OR '.join(json_conditions) })")
            else:
                placeholders = ", ".join(["?" for _ in value])
                conditions.append(f"({column} IN ({placeholders}) OR {column} IS NULL)")
                params.extend(value)

        elif value is None:
            if is_json_field and len(prefix) > 1:
                json_path = ".".join(prefix[1:])
                conditions.append(
                    f"(json_type({prefix[0]}, '$.{json_path}') IS NOT NULL AND json_extract({prefix[0]}, '$.{json_path}') IS NULL)"
                )
            else:
                conditions.append(f"{column} IS NULL")
        else:
            processed_value = value

            if column_type == "other":
                column = f'"__hash_{prefix[0]}"'
                processed_value = hash_bytes(
                    pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
                )
            elif column_type == "blob":
                column = f'"__hash_{prefix[0]}"'
                processed_value = hash_bytes(value)
            elif column_type == "datetime":
                processed_value = value.timestamp()
            elif column_type == "boolean":
                processed_value = int(value)
            elif column_type == "compressed_string":
                processed_value = value.encode()
                # TODO: Handle compressed strings
                pass

            conditions.append(f"{column} = ?")
            params.append(processed_value)

        return conditions

    # Process top-level query
    for key, value in query.items():
        if key in ["$and", "$or"]:
            sub_conditions = []
            for cond in value:
                # Process each condition in the $and/$or array
                cond_results = []
                for sub_key, sub_value in cond.items():
                    nested_conditions = process_nested_query(sub_value, [sub_key])
                    if nested_conditions:
                        # Join conditions within the same object with AND
                        cond_results.append(f"({' AND '.join(nested_conditions)})")
                if cond_results:
                    # Add the grouped conditions
                    sub_conditions.append(f"({' AND '.join(cond_results)})")

            # Join all sub-conditions with the appropriate operator
            if sub_conditions:
                join_operator = " OR " if key == "$or" else " AND "
                where_conditions.append(f"({join_operator.join(sub_conditions)})")
        else:
            nested_conditions = process_nested_query(value, [key])
            if nested_conditions:
                where_conditions.extend(nested_conditions)

    return where_conditions, params


def pop_query(table_name, query, schema, sort_by=None, reversed_sort=False, n=None):
    # Prepare the query
    where_conditions, params = parse_query(query, schema)

    # Build the query string
    query_str = f"DELETE FROM {table_name}"
    if where_conditions:
        query_str += f" WHERE id IN (SELECT id FROM {table_name} WHERE {' AND '.join(where_conditions)}"
    else:
        query_str += f" WHERE id IN (SELECT id FROM {table_name}"

    if sort_by:
        query_str += f" ORDER BY {sort_by} {'DESC' if reversed_sort else 'ASC'}"

    if n is not None:
        query_str += f" LIMIT {n}"

    query_str += ")"

    query_str += " RETURNING *"

    return query_str, params


def search_query(
    table_name,
    query,
    schema,
    sort_by=None,
    reversed_sort=False,
    n=None,
    offset=None,
    select_columns=None,
    sort_by_embedding=None,
    sort_by_embedding_metric="cosine",
    is_update=False,
):
    if select_columns is None:
        select_columns = tuple(schema)

    where_conditions, params = parse_query(query, schema)

    # Add distance calculation if sort_by_embedding is provided
    if sort_by_embedding is not None:
        distance_func = (
            f"""vector_distance(?, "{sort_by}", '{sort_by_embedding_metric}')"""
        )
        select_columns = (
            "integer_id",
            "id",
            "updated_at",
            (f"{distance_func} AS __distance",),
        ) + select_columns

        params.insert(0, sort_by_embedding.tobytes())
    else:
        if is_update:
            select_columns = tuple(select_columns)
        else:
            select_columns = ("integer_id", "id", "updated_at") + tuple(select_columns)

    selected_columns = (
        ", ".join([f'"{_}"' if isinstance(_, str) else _[0] for _ in select_columns])
        if select_columns
        else "*"
    )

    query_str = f"SELECT {selected_columns} FROM {table_name}"

    if where_conditions:
        query_str += f" WHERE {' AND '.join(where_conditions)}"

    if sort_by_embedding is not None:
        query_str += f""" ORDER BY __distance {'DESC' if reversed_sort else 'ASC'}"""
    elif sort_by:
        if isinstance(sort_by, list):
            query_str += " ORDER BY "
            sort_list = []
            for sort_item in sort_by:
                if isinstance(sort_item, tuple):
                    sort_list.append(
                        f""""{sort_item[0]}" {'DESC' if sort_item[1] else 'ASC'}"""
                    )
                else:
                    sort_list.append(
                        f""""{sort_item}" {'DESC' if reversed_sort else 'ASC'}"""
                    )
            query_str += ", ".join(sort_list)
        else:
            query_str += (
                f""" ORDER BY "{sort_by}" {'DESC' if reversed_sort else 'ASC'}"""
            )

    query_str += f" LIMIT {n if n is not None else -1} OFFSET {offset if offset is not None else 0}"

    return query_str, params


def distinct_query(table_name, column, query, schema):
    # Prepare the query
    where_conditions, params = parse_query(query, schema)

    # Build the query string
    if schema[column] == "json":
        query_str = f"""
        SELECT DISTINCT value
        FROM (
            SELECT json_each.value
            FROM {table_name}, json_each({table_name}.{column})
        ) subquery
        """
    else:
        query_str = f"SELECT DISTINCT {column} FROM {table_name}"

    if where_conditions:
        query_str += f" WHERE {' AND '.join(where_conditions)}"

    return query_str, params


def distinct_count_query(table_name, column, query, schema, min_count=0, top_n=None):
    where_conditions, params = parse_query(query, schema)

    if schema[column] == "json":
        query_str = f"""
        SELECT value, COUNT(*) AS count
        FROM (
            SELECT json_each.value
            FROM {table_name}, json_each({table_name}.{column})
        ) subquery
        """
    else:
        query_str = f"SELECT {column}, COUNT(*) AS count FROM {table_name}"

    if where_conditions:
        query_str += f" WHERE {' AND '.join(where_conditions)}"

    query_str += " GROUP BY value"
    query_str += f" HAVING COUNT(*) >= {min_count}"

    if top_n is not None:
        query_str += f" ORDER BY COUNT(*) DESC LIMIT {top_n}"

    return query_str, params


def group_by_query(table_name, columns, query, schema):
    # Prepare the query
    where_conditions, params = parse_query(query, schema)

    # Build the query string
    separator = chr(31)  # unit separator
    columns_query = ", ".join(
        [
            f"JSON_EXTRACT({column}, '$[*]')" if schema[column] == "json" else column
            for column in columns
        ]
    )

    query_str = f"SELECT {columns_query}, GROUP_CONCAT(id, '{separator}') as ids FROM {table_name}"

    if where_conditions:
        query_str += f" WHERE {' AND '.join(where_conditions)}"

    columns_group = ", ".join(columns)
    query_str += f" GROUP BY {columns_group}"

    return query_str, params


def count_query(table_name, query, schema):
    # Prepare the query
    where_conditions, params = parse_query(query, schema)

    # Build the query string
    query_str = f"SELECT COUNT(*) FROM {table_name}"
    if where_conditions:
        query_str += f" WHERE {' AND '.join(where_conditions)}"

    return query_str, params


def delete_query(table_name, query, schema):
    # Check if the query is empty
    if not query:
        # Optimize by clearing the table using DELETE without WHERE
        query_str = f"DELETE FROM {table_name}"
        params = []
    else:
        # Prepare the query
        where_conditions, params = parse_query(query, schema)

        # Build the query string
        query_str = f"DELETE FROM {table_name}"
        if where_conditions:
            query_str += f" WHERE {' AND '.join(where_conditions)}"

    return query_str, params


def sum_query(table_name, column, query, schema):
    if column not in schema:
        raise ValueError(f"Invalid column '{column}' specified for sum")
    if schema[column] != "number":
        raise ValueError("Sum operation can only be applied on numeric columns")

    # Prepare the query
    where_conditions, params = parse_query(query, schema)

    # Build the query string
    query_str = f'SELECT SUM("{column}") FROM {table_name}'
    if where_conditions:
        query_str += f" WHERE {' AND '.join(where_conditions)}"

    return query_str, params


def avg_query(table_name, column, query, schema):
    if column not in schema:
        raise ValueError(f"Invalid column '{column}' specified for average")
    if schema[column] != "number":
        raise ValueError("Average operation can only be applied on numeric columns")

    # Prepare the query
    where_conditions, params = parse_query(query, schema)

    # Build the query string
    query_str = f'SELECT AVG("{column}") FROM {table_name}'
    if where_conditions:
        query_str += f" WHERE {' AND '.join(where_conditions)}"

    return query_str, params


def min_query(table_name, column, query, schema):
    if column not in schema:
        raise ValueError(f"Invalid column '{column}' specified for minimum")

    # Prepare the query
    where_conditions, params = parse_query(query, schema)

    # Build the query string
    query_str = f'SELECT MIN("{column}") FROM {table_name}'
    if where_conditions:
        query_str += f" WHERE {' AND '.join(where_conditions)}"

    return query_str, params


def max_query(table_name, column, query, schema):
    if column not in schema:
        raise ValueError(f"Invalid column '{column}' specified for maximum")

    # Prepare the query
    where_conditions, params = parse_query(query, schema)

    # Build the query string
    query_str = f'SELECT MAX("{column}") FROM {table_name}'
    if where_conditions:
        query_str += f" WHERE {' AND '.join(where_conditions)}"

    return query_str, params


def plus_equals_query(table_name, column, value):
    query_str = (
        f'UPDATE {table_name} SET "{column}" = "{column}" + ? WHERE {column} = ?'
    )
    params = (value, column)
    return query_str, params


def minus_equals_query(table_name, column, value):
    query_str = (
        f'UPDATE {table_name} SET "{column}" = "{column}" - ? WHERE "{column}" = ?'
    )
    params = (value, column)
    return query_str, params


def multiply_equals_query(table_name, column, value):
    query_str = (
        f'UPDATE {table_name} SET "{column}" = "{column}" * ? WHERE "{column}" = ?'
    )
    params = (value, column)
    return query_str, params


def divide_equals_query(table_name, column, value):
    query_str = (
        f'UPDATE {table_name} SET "{column}" = "{column}" / ? WHERE "{column}" = ?'
    )
    params = (value, column)
    return query_str, params


def floor_divide_equals_query(table_name, column, value):
    query_str = (
        f'UPDATE {table_name} SET "{column}" = "{column}" // ? WHERE "{column}" = ?'
    )
    params = (value, column)
    return query_str, params


def modulo_equals_query(table_name, column, value):
    query_str = (
        f'UPDATE {table_name} SET "{column}" = "{column}" % ? WHERE "{column}" = ?'
    )
    params = (value, column)
    return query_str, params


if __name__ == "__main__":
    import unittest
    import json

    class TestSearchQuery(unittest.TestCase):
        def setUp(self):
            self.schema = {
                "age": "number",
                "name": "string",
                "tags_list": "json",
                "tag_id_to_name": "json",
                "is_true": "boolean",
                "profile_picture": "blob",
            }

        def test_basic_equality(self):
            # Test basic equality comparison with a simple string value
            query = {"name": "John"}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(conditions, ['"name" = ?'])
            self.assertEqual(params, ["John"])

        def test_null_comparison(self):
            # Test handling of NULL values in basic comparison
            query = {"name": None}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(conditions, ['"name" IS NULL'])
            self.assertEqual(params, [])

        def test_multi_comparison(self):
            query = {"name": "John", "age": 25, "is_true": True}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(conditions, ['"name" = ?', '"age" = ?', '"is_true" = ?'])
            self.assertEqual(params, ["John", 25, 1])

        def test_number_comparison(self):
            # Test handling of numeric values
            query = {"age": 25}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(conditions, ['"age" = ?'])
            self.assertEqual(params, [25])

        def test_boolean_comparison(self):
            # Test handling of boolean values (converts to int for SQLite)
            query = {"is_true": True}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(conditions, ['"is_true" = ?'])
            self.assertEqual(params, [1])

        def test_not_equal_operator(self):
            # Test $ne operator including NULL handling
            query = {"age": {"$ne": 25}}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(conditions, ['("age" != ? OR "age" IS NULL)'])
            self.assertEqual(params, [25])

        def test_not_equal_null(self):
            # Test $ne operator with NULL value
            query = {"name": {"$ne": None}}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(conditions, ['("name" IS NOT NULL)'])
            self.assertEqual(params, [])

        def test_greater_than(self):
            # Test greater than operator
            query = {"age": {"$gt": 25}}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(conditions, ['"age" > ?'])
            self.assertEqual(params, [25])

        def test_less_than(self):
            # Test less than operator
            query = {"age": {"$lt": 25}}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(conditions, ['"age" < ?'])
            self.assertEqual(params, [25])

        def test_greater_equal(self):
            # Test greater than or equal operator
            query = {"age": {"$gte": 25}}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(conditions, ['"age" >= ?'])
            self.assertEqual(params, [25])

        def test_less_equal(self):
            # Test less than or equal operator
            query = {"age": {"$lte": 25}}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(conditions, ['"age" <= ?'])
            self.assertEqual(params, [25])

        def test_in_operator(self):
            # Test IN operator with array of values
            query = {"age": {"$in": [25, 30, 35]}}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(conditions, ['("age" IN (?, ?, ?) OR "age" IS NULL)'])
            self.assertEqual(params, [25, 30, 35])

        def test_not_in_operator(self):
            # Test NOT IN operator with array of values
            query = {"age": {"$nin": [25, 30, 35]}}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(conditions, ['("age" NOT IN (?, ?, ?))'])
            self.assertEqual(params, [25, 30, 35])

        def test_not_in_with_null(self):
            # Test NOT IN operator including NULL value
            query = {"age": {"$nin": [25, None, 35]}}
            conditions, params = parse_query(query, self.schema)
            print(conditions, "<<>>", params)
            self.assertEqual(
                conditions, ['("age" NOT IN (?, ?) AND "age" IS NOT NULL)']
            )
            self.assertEqual(params, [25, 35])

        def test_like_operator(self):
            # Test LIKE operator for pattern matching
            query = {"name": {"$like": "John%"}}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(conditions, ['"name" LIKE ?'])
            self.assertEqual(params, ["John%"])

        def test_json_array_contains(self):
            # Test JSON array contains condition
            query = {"tags_list": ["tag1"]}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(
                conditions,
                ["(EXISTS(SELECT 1 FROM json_each(tags_list) WHERE value = ?))"],
            )
            self.assertEqual(params, [json.dumps("tag1")])

        def test_json_field_equality(self):
            # Test JSON field exact match
            query = {"tag_id_to_name": {"user_1": "John"}}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(
                conditions, ["json_extract(tag_id_to_name, '$.user_1') = ?"]
            )
            self.assertEqual(params, ["John"])

        def test_json_field_null(self):
            # Test JSON field NULL check
            query = {"tag_id_to_name": {"user_1": None}}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(
                conditions,
                [
                    "(json_type(tag_id_to_name, '$.user_1') IS NOT NULL AND json_extract(tag_id_to_name, '$.user_1') IS NULL)"
                ],
            )
            self.assertEqual(params, [])

        def test_json_like_operator(self):
            # Test LIKE operator on JSON array elements
            query = {"tags_list": {"$like": "%test%"}}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(
                conditions,
                ["EXISTS(SELECT 1 FROM json_each(tags_list) WHERE value LIKE ?)"],
            )
            self.assertEqual(params, ["%test%"])

        def test_json_in_operator(self):
            # Test IN operator on JSON array elements
            query = {"tags_list": {"$in": ["tag1", "tag2"]}}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(
                conditions,
                [
                    "(EXISTS(SELECT 1 FROM json_each(tags_list) WHERE value = ?) OR EXISTS(SELECT 1 FROM json_each(tags_list) WHERE value = ?))"
                ],
            )
            self.assertEqual(params, ["tag1", "tag2"])

        def test_json_not_in_operator(self):
            # Test NOT IN operator on JSON array elements
            query = {"tags_list": {"$nin": ["tag1", "tag2"]}}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(
                conditions,
                [
                    "((NOT EXISTS(SELECT 1 FROM json_each(tags_list) WHERE value = ?) AND NOT EXISTS(SELECT 1 FROM json_each(tags_list) WHERE value = ?)))"
                ],
            )
            self.assertEqual(params, ["tag1", "tag2"])

        def test_multiple_conditions(self):
            # Test multiple conditions combined with implicit AND
            query = {"name": "John", "age": {"$gt": 25}}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(conditions, ['"name" = ?', '"age" > ?'])
            self.assertEqual(params, ["John", 25])

        def test_or_operator(self):
            # Test $or operator combining multiple conditions
            query = {"$or": [{"name": "John"}, {"age": {"$gt": 25}}]}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(conditions, ['((("name" = ?)) OR (("age" > ?)))'])
            self.assertEqual(params, ["John", 25])

        def test_and_operator(self):
            # Test $and operator combining multiple conditions
            query = {"$and": [{"name": "John"}, {"age": {"$gt": 25}}]}
            conditions, params = parse_query(query, self.schema)
            self.assertEqual(conditions, ['((("name" = ?)) AND (("age" > ?)))'])
            self.assertEqual(params, ["John", 25])

        def test_complex_nested_query(self):
            # Test complex nested query with multiple operators and conditions
            query = {
                "$or": [
                    {"name": {"$like": "John%"}, "age": {"$gt": 25}},
                    {"tags_list": {"$in": ["important", "urgent"]}, "is_true": True},
                ]
            }
            conditions, params = parse_query(query, self.schema)

            expected_conditions = [
                '((("name" LIKE ?) AND ("age" > ?)) OR (((EXISTS(SELECT 1 FROM json_each(tags_list) WHERE value = ?) OR EXISTS(SELECT 1 FROM json_each(tags_list) WHERE value = ?))) AND ("is_true" = ?)))'
            ]

            self.assertEqual(conditions, expected_conditions)
            self.assertEqual(params, ["John%", 25, "important", "urgent", 1])

    if __name__ == "__main__":
        unittest.main()
