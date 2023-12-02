import json


def parse_query(query, schema, prefix=None):
    where_conditions = []
    params = []

    if prefix is None:
        prefix = []

    def process_nested_query(value, prefix):
        nonlocal where_conditions, params
        column = (
            "json_extract(" + prefix[0] + ", '$." + ".".join(prefix[1:]) + "')"
            if len(prefix) > 1
            else prefix[0]
        )

        if isinstance(value, dict):
            sub_conditions = []
            for sub_key, sub_value in value.items():
                if sub_key in ["$ne", "$like", "$gt", "$lt", "$gte", "$lte"]:
                    operator = {
                        "$ne": "!=",
                        "$like": "LIKE",
                        "$gt": ">",
                        "$lt": "<",
                        "$gte": ">=",
                        "$lte": "<=",
                    }[sub_key]

                    # Handle the case for JSON columns with "$like" operator
                    if schema[prefix[0]] == "json" and sub_key == "$like":
                        sub_conditions.append(
                            f"JSON_EXTRACT({column}, '$[*]') {operator} ?"
                        )
                    else:
                        sub_conditions.append(f"{column} {operator} ?")

                    params.append(sub_value)
                elif sub_key == "$in":
                    sub_conditions.append(
                        f"{column} IN ({', '.join(['?' for _ in sub_value])})"
                    )
                    params.extend(sub_value)
                elif sub_key == "$nin":
                    sub_conditions.append(
                        f"{column} NOT IN ({', '.join(['?' for _ in sub_value])})"
                    )
                    params.extend(sub_value)
                else:
                    process_nested_query(sub_value, prefix + [sub_key])
            if sub_conditions:
                where_conditions.append(f"({' AND '.join(sub_conditions)})")

        elif isinstance(value, list):
            if schema[prefix[0]] == "json":
                json_conditions = [f"JSON_CONTAINS({column}, ?)" for _ in value]
                # Add parentheses around the OR condition
                where_conditions.append(f"({ ' OR '.join(json_conditions) })")
                params.extend(json.dumps(val) for val in value)
            else:
                where_conditions.append(
                    f"{column} IN ({', '.join(['?' for _ in value])})"
                )
                params.extend(value)

        elif value is None:
            where_conditions.append(f"{column} IS NULL")
        else:
            where_conditions.append(f"{column} = ?")
            params.append(value)

    for key, value in query.items():
        if key in ["$and", "$or"]:
            sub_conditions, sub_params = zip(
                *(parse_query(cond, schema) for cond in value)
            )
            # Flatten the sub_conditions
            sub_conditions = [cond for sublist in sub_conditions for cond in sublist]
            where_conditions.append(
                f"({' OR '.join(sub_conditions) if key == '$or' else ' AND '.join(sub_conditions)})"
            )
            params.extend(item for sublist in sub_params for item in sublist)
        else:
            process_nested_query(value, [key])

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
    page=None,
    page_size=50,
    select_columns=None,
):
    # Prepare the query
    where_conditions, params = parse_query(query, schema)

    selected_columns = ", ".join(select_columns) if select_columns else "*"

    # Build the query string
    query_str = f"SELECT {selected_columns} FROM {table_name}"
    if where_conditions:
        query_str += f" WHERE {' AND '.join(where_conditions)}"
    if sort_by:
        if isinstance(sort_by, list):
            query_str += " ORDER BY "
            sort_list = []
            for sort_item in sort_by:
                if isinstance(sort_item, tuple):
                    sort_list.append(
                        f"{sort_item[0]} {'DESC' if sort_item[1] else 'ASC'}"
                    )
                else:
                    sort_list.append(
                        f"{sort_item} {'DESC' if reversed_sort else 'ASC'}"
                    )
            query_str += ", ".join(sort_list)
        else:
            query_str += f" ORDER BY {sort_by} {'DESC' if reversed_sort else 'ASC'}"

    if n is not None:
        query_str += f" LIMIT {n}"
    elif page is not None:
        start = (page - 1) * page_size
        query_str += f" LIMIT {start}, {page_size}"

    return query_str, params


def distinct_query(table_name, column, query, schema):
    # Prepare the query
    where_conditions, params = parse_query(query, schema)

    # Build the query string
    if schema[column] == "json":
        query_str = f"SELECT DISTINCT JSON_EXTRACT({column}, '$[*]') FROM {table_name}"
    else:
        query_str = f"SELECT DISTINCT {column} FROM {table_name}"
    if where_conditions:
        query_str += f" WHERE {' AND '.join(where_conditions)}"

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
    query_str = f"SELECT SUM({column}) FROM {table_name}"
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
    query_str = f"SELECT AVG({column}) FROM {table_name}"
    if where_conditions:
        query_str += f" WHERE {' AND '.join(where_conditions)}"

    return query_str, params


def min_query(table_name, column, query, schema):
    if column not in schema:
        raise ValueError(f"Invalid column '{column}' specified for minimum")

    # Prepare the query
    where_conditions, params = parse_query(query, schema)

    # Build the query string
    query_str = f"SELECT MIN({column}) FROM {table_name}"
    if where_conditions:
        query_str += f" WHERE {' AND '.join(where_conditions)}"

    return query_str, params


def max_query(table_name, column, query, schema):
    if column not in schema:
        raise ValueError(f"Invalid column '{column}' specified for maximum")

    # Prepare the query
    where_conditions, params = parse_query(query, schema)

    # Build the query string
    query_str = f"SELECT MAX({column}) FROM {table_name}"
    if where_conditions:
        query_str += f" WHERE {' AND '.join(where_conditions)}"

    return query_str, params


def plus_equals_query(table_name, column, value):
    query_str = f"UPDATE {table_name} SET {column} = {column} + ? WHERE {column} = ?"
    params = (value, column)
    return query_str, params


def minus_equals_query(table_name, column, value):
    query_str = f"UPDATE {table_name} SET {column} = {column} - ? WHERE {column} = ?"
    params = (value, column)
    return query_str, params


def multiply_equals_query(table_name, column, value):
    query_str = f"UPDATE {table_name} SET {column} = {column} * ? WHERE {column} = ?"
    params = (value, column)
    return query_str, params


def divide_equals_query(table_name, column, value):
    query_str = f"UPDATE {table_name} SET {column} = {column} / ? WHERE {column} = ?"
    params = (value, column)
    return query_str, params


def floor_divide_equals_query(table_name, column, value):
    query_str = f"UPDATE {table_name} SET {column} = {column} // ? WHERE {column} = ?"
    params = (value, column)
    return query_str, params


def modulo_equals_query(table_name, column, value):
    query_str = f"UPDATE {table_name} SET {column} = {column} % ? WHERE {column} = ?"
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
            }

        def test_single_condition(self):
            query, params = search_query("users", {"age": 25}, self.schema)
            self.assertEqual(query, "SELECT * FROM users WHERE age = ?")
            self.assertEqual(params, [25])

        def test_multiple_conditions(self):
            query, params = search_query(
                "users", {"age": 25, "name": "john"}, self.schema
            )
            self.assertEqual(query, "SELECT * FROM users WHERE age = ? AND name = ?")
            self.assertEqual(params, [25, "john"])

        def test_nested_conditions(self):
            query, params = search_query(
                "users",
                {"$and": [{"age": {"$gte": 20, "$lte": 30}}, {"name": "john"}]},
                self.schema,
            )
            self.assertEqual(
                query,
                "SELECT * FROM users WHERE ((age >= ? AND age <= ?) AND name = ?)",
            )
            self.assertEqual(params, [20, 30, "john"])

        def test_json_conditions(self):
            query, params = search_query(
                "users", {"tags_list": ["tag1", "tag2"]}, self.schema
            )
            expected_query = "SELECT * FROM users WHERE (JSON_CONTAINS(tags_list, ?) OR JSON_CONTAINS(tags_list, ?))"
            self.assertEqual(query, expected_query)
            self.assertEqual(params, ['"tag1"', '"tag2"'])

        def test_sorting(self):
            query, params = search_query(
                "users", {"age": 25}, self.schema, sort_by="name"
            )
            self.assertEqual(
                query, "SELECT * FROM users WHERE age = ? ORDER BY name ASC"
            )
            self.assertEqual(params, [25])

        def test_multiple_sorting(self):
            query, params = search_query(
                "users",
                {"$and": [{"age": {"$gte": 20, "$lte": 30}}, {"name": "john"}]},
                self.schema,
                sort_by=[("age", True), ("name", False)],
            )
            self.assertEqual(
                query,
                "SELECT * FROM users WHERE ((age >= ? AND age <= ?) AND name = ?) ORDER BY age DESC, name ASC",
            )
            self.assertEqual(params, [20, 30, "john"])

        def test_pagination(self):
            query, params = search_query(
                "users", {"age": 25}, self.schema, page=2, page_size=10
            )
            self.assertEqual(query, "SELECT * FROM users WHERE age = ? LIMIT 10, 10")
            self.assertEqual(params, [25])

        def test_select_columns(self):
            query, params = search_query(
                "users",
                {"age": 25},
                self.schema,
                select_columns=["name", "age"],
            )
            self.assertEqual(query, "SELECT name, age FROM users WHERE age = ?")
            self.assertEqual(params, [25])

        def test_or_operator(self):
            query, params = search_query(
                "users", {"$or": [{"age": 25}, {"name": "john"}]}, self.schema
            )
            self.assertEqual(query, "SELECT * FROM users WHERE (age = ? OR name = ?)")
            self.assertEqual(params, [25, "john"])

        def test_in_and_nin_operators(self):
            query, params = search_query(
                "users",
                {"age": {"$in": [25, 30]}, "name": {"$nin": ["john", "jane"]}},
                self.schema,
            )
            self.assertEqual(
                query,
                "SELECT * FROM users WHERE (age IN (?, ?)) AND (name NOT IN (?, ?))",
            )
            self.assertEqual(params, [25, 30, "john", "jane"])

        def test_like_operator(self):
            query, params = search_query(
                "users", {"name": {"$like": "jo%"}}, self.schema
            )
            self.assertEqual(query, "SELECT * FROM users WHERE (name LIKE ?)")
            self.assertEqual(params, ["jo%"])

        def test_json_dict_condition(self):
            query, params = search_query(
                "users",
                {"tag_id_to_name": {"1": "tag1", "2": "tag2"}},
                self.schema,
            )
            expected_query = "SELECT * FROM users WHERE json_extract(tag_id_to_name, '$.1') = ? AND json_extract(tag_id_to_name, '$.2') = ?"
            self.assertEqual(query, expected_query)
            self.assertEqual(params, ["tag1", "tag2"])

        def test_null_values(self):
            query, params = search_query("users", {"age": None}, self.schema)
            self.assertEqual(query, "SELECT * FROM users WHERE age IS NULL")
            self.assertEqual(params, [])

        def test_mix_of_conditions(self):
            query, params = search_query(
                "users",
                {
                    "$and": [
                        {"age": {"$gte": 20, "$lte": 30}},
                        {"name": {"$like": "jo%"}},
                        {"is_true": 1},
                        {"tags_list": ["tag1", "tag2"]},
                        {"tag_id_to_name": {"1": "tag1", "2": "tag2"}},
                    ]
                },
                self.schema,
            )
            expected_query = "SELECT * FROM users WHERE ((age >= ? AND age <= ?) AND (name LIKE ?) AND is_true = ? AND (JSON_CONTAINS(tags_list, ?) OR JSON_CONTAINS(tags_list, ?)) AND json_extract(tag_id_to_name, '$.1') = ? AND json_extract(tag_id_to_name, '$.2') = ?)"
            self.assertEqual(query, expected_query)
            self.assertEqual(
                params, [20, 30, "jo%", 1, '"tag1"', '"tag2"', "tag1", "tag2"]
            )

        def test_sorting_desc(self):
            query, params = search_query(
                "users",
                {"age": 25},
                self.schema,
                sort_by="name",
                reversed_sort=True,
            )
            self.assertEqual(
                query, "SELECT * FROM users WHERE age = ? ORDER BY name DESC"
            )
            self.assertEqual(params, [25])

        def test_single_tuple_sorting(self):
            query, params = search_query(
                "users", {"age": 25}, self.schema, sort_by=[("name", True)]
            )
            self.assertEqual(
                query, "SELECT * FROM users WHERE age = ? ORDER BY name DESC"
            )
            self.assertEqual(params, [25])

        def test_ne_operator(self):
            query, params = search_query("users", {"age": {"$ne": 25}}, self.schema)
            self.assertEqual(query, "SELECT * FROM users WHERE (age != ?)")
            self.assertEqual(params, [25])

        def test_gt_lt_operators(self):
            query, params = search_query(
                "users", {"age": {"$gt": 20, "$lt": 30}}, self.schema
            )
            self.assertEqual(query, "SELECT * FROM users WHERE (age > ? AND age < ?)")
            self.assertEqual(params, [20, 30])

        def test_gte_lte_or_operators(self):
            query, params = search_query(
                "users",
                {"$or": [{"age": {"$gte": 20, "$lte": 30}}, {"name": "john"}]},
                self.schema,
            )
            self.assertEqual(
                query, "SELECT * FROM users WHERE ((age >= ? AND age <= ?) OR name = ?)"
            )
            self.assertEqual(params, [20, 30, "john"])

        def test_empty_query(self):
            query, params = search_query("users", {}, self.schema)
            self.assertEqual(query, "SELECT * FROM users")
            self.assertEqual(params, [])

        def test_limit_results(self):
            query, params = search_query("users", {"age": 25}, self.schema, n=5)
            self.assertEqual(query, "SELECT * FROM users WHERE age = ? LIMIT 5")
            self.assertEqual(params, [25])

    class TestDistinctAndCountQuery(unittest.TestCase):
        def setUp(self):
            self.schema = {
                "age": "NUMBER",
                "name": "TEXT",
                "tags_list": "json",
                "tag_id_to_name": "json",
                "is_true": "INTEGER",
            }

        def test_distinct_query(self):
            query_str, params = distinct_query(
                "test_table", "name", {"age": {"$gt": 30}}, self.schema
            )
            self.assertEqual(
                query_str, "SELECT DISTINCT name FROM test_table WHERE (age > ?)"
            )
            self.assertEqual(params, [30])

            query_str, params = distinct_query(
                "test_table",
                "tags_list",
                {"tags_list": ["a", "b"]},
                self.schema,
            )

            self.assertEqual(
                query_str,
                "SELECT DISTINCT JSON_EXTRACT(tags_list, '$[*]') FROM test_table WHERE (JSON_CONTAINS(tags_list, ?) OR JSON_CONTAINS(tags_list, ?))",
            )
            self.assertEqual(params, ['"a"', '"b"'])

        def test_count_query(self):
            query_str, params = count_query(
                "test_table",
                {"name": "John", "age": {"$lt": 50}, "is_true": 1},
                self.schema,
            )

            self.assertEqual(
                query_str,
                "SELECT COUNT(*) FROM test_table WHERE name = ? AND (age < ?) AND is_true = ?",
            )
            self.assertEqual(params, ["John", 50, 1])

            query_str, params = count_query(
                "test_table",
                {"$or": [{"name": "John"}, {"tags_list": {"$like": "%a%"}}]},
                self.schema,
            )

            self.assertEqual(
                query_str,
                "SELECT COUNT(*) FROM test_table WHERE (name = ? OR (JSON_EXTRACT(tags_list, '$[*]') LIKE ?))",
            )
            self.assertEqual(params, ["John", "%a%"])


if __name__ == "__main__":
    unittest.main()
