from common_utils import EvictionCfg

__policy_to_number_int_id = {
    EvictionCfg.EvictAny: 1,
    EvictionCfg.EvictFIFO: 2,
    EvictionCfg.EvictLRU: 3,
    EvictionCfg.EvictLFU: 4,
    EvictionCfg.EvictNone: 5,
}

__policy_id_to_original = {v: k for k, v in __policy_to_number_int_id.items()}


# store_key=True: This adds an additional 'pickled_key' BLOB column to store serialized keys.
#   TABLE kv_index: key_hash BLOB, num_value NUMBER, string_value TEXT, pickled_value BLOB, pickled_key BLOB, PRIMARY KEY (key_hash)

# store_key=False: Without 'store_key', the 'pickled_key' column is not created.
#   TABLE kv_index: key_hash BLOB, num_value NUMBER, string_value TEXT, pickled_value BLOB, PRIMARY KEY (key_hash)

# preserve_order=True or eviction.invalidate_after_seconds > 0: Adds an 'updated_at' INTEGER column for storing timestamps to maintain order or handle time-based eviction.
#   TABLE kv_index with 'updated_at': key_hash BLOB, ..., updated_at INTEGER, PRIMARY KEY (key_hash)

# eviction.policy=EvictLRU (Least Recently Used): Includes a 'last_accessed_time' INTEGER column to track access time for LRU eviction policy.
#   TABLE kv_index with LRU: key_hash BLOB, ..., last_accessed_time INTEGER, PRIMARY KEY (key_hash)
#   INDEX kv_index_last_accessed_time_idx ON kv_index(last_accessed_time) for efficient LRU eviction querying.

# eviction.policy=EvictLFU (Least Frequently Used): Includes an 'access_frequency' INTEGER column to track access count for LFU eviction policy.
#   TABLE kv_index with LFU: key_hash BLOB, ..., access_frequency INTEGER, PRIMARY KEY (key_hash)
#   INDEX kv_index_access_frequency_idx ON kv_index(access_frequency) for efficient LFU eviction querying.

# eviction.max_size_in_mb is set: An additional 'size_in_bytes' INTEGER column is added to track the size of each stored item in bytes.
#   TABLE kv_index with size tracking: key_hash BLOB, ..., size_in_bytes INTEGER, PRIMARY KEY (key_hash)

# The function also creates a 'kv_index_num_metadata' table to store numeric metadata about the key-value index.
#   TABLE kv_index_num_metadata: key TEXT PRIMARY KEY, num INTEGER
# This metadata table includes entries for current size (in MB), flags for store_key and preserve_order, eviction policies and their parameters like max size, max number of items, and invalidation period.

# An additional index is created for the 'updated_at' column if `preserve_order=True` or `eviction.invalidate_after_seconds > 0` to enable efficient querying by update time.
#   INDEX kv_index_updated_at_idx ON kv_index(updated_at)


def create_tables(store_key, preserve_order, eviction, conn):
    columns_needed_and_sql_types = {
        "key_hash": "BLOB",
        "num_value": "NUMBER",
        "string_value": "TEXT",
        "pickled_value": "BLOB",
    }

    if store_key:
        columns_needed_and_sql_types["pickled_key"] = "BLOB"
    if preserve_order or eviction.invalidate_after_seconds > 0:
        columns_needed_and_sql_types["updated_at"] = "INTEGER"

    if eviction.policy is EvictionCfg.EvictLRU:
        columns_needed_and_sql_types["last_accessed_time"] = "INTEGER"
    elif eviction.policy is EvictionCfg.EvictLFU:
        columns_needed_and_sql_types["access_frequency"] = "INTEGER"

    if eviction.max_size_in_mb:
        columns_needed_and_sql_types["size_in_bytes"] = "INTEGER"

    conn.execute(
        f"CREATE TABLE IF NOT EXISTS kv_index ({','.join([f'{col} {sql_type}' for col, sql_type in columns_needed_and_sql_types.items()])}, PRIMARY KEY (key_hash))"
    )

    conn.execute(
        "CREATE TABLE IF NOT EXISTS kv_index_num_metadata (key TEXT PRIMARY KEY, num INTEGER)"
    )

    conn.executemany(
        "INSERT OR IGNORE INTO kv_index_num_metadata (key, num) VALUES (?, ?)",
        (
            ("current_size_in_mb", 0),
            ("store_key", 1 if store_key else 0),
            ("preserve_order", 1 if preserve_order else 0),
            ("policy", __policy_to_number_int_id[eviction.policy]),
            ("max_size_in_mb", int(eviction.max_size_in_mb)),
            ("max_number_of_items", eviction.max_number_of_items),
            ("invalidate_after_seconds", eviction.invalidate_after_seconds),
        ),
    )

    if eviction.policy is EvictionCfg.EvictLRU:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS kv_index_last_accessed_time_idx ON kv_index(last_accessed_time)"
        )

    if eviction.policy is EvictionCfg.EvictLFU:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS kv_index_access_frequency_idx ON kv_index(access_frequency)"
        )

    if eviction.invalidate_after_seconds > 0:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS kv_index_updated_at_idx ON kv_index(updated_at)"
        )


import pickle
import sqlite3

def serialize(value):
    return sqlite3.Binary(pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL))

def get_column_by_value(value):
    if isinstance(value, (int, float)):
        return 'num_value'
    elif isinstance(value, str):
        return 'string_value'
    else:
        return 'pickled_value'

def handle_like(operator, value):
    # This only applies to strings
    column = 'string_value'
    if operator == '$startswith':
        value = f"{value}%"
    elif operator == '$endswith':
        value = f"%{value}"
    elif operator == '$like':
        # The value in this case should already have % or _ for the LIKE pattern
        value = value
    else:
        raise ValueError(f"Unknown operator for LIKE: {operator}")

    return f"{column} LIKE ?", (value,)

def handle_comparison(operator, value):
    column = get_column_by_value(value)

    operators_map = {
        '$eq': '=',
        '$ne': '!=',
        '$gt': '>',
        '$lt': '<',
        '$gte': '>=',
        '$lte': '<=',
    }

    sql_operator = operators_map.get(operator)
    if sql_operator is None:
        raise ValueError(f"Unknown comparison operator: {operator}")

    if column == 'pickled_value':
        # Using parameterized queries to prevent injection
        return f"{column} {sql_operator} ?", (serialize(value),)
    else:
        return f"{column} {sql_operator} ?", (value,)

def handle_in_operator(value, operator):
    column = get_column_by_value(value[0])

    placeholders = ', '.join(['?'] * len(value))
    if column == 'pickled_value':
        value = list(map(serialize, value))

    operator = 'IN' if operator == '$in' else 'NOT IN'
    sql = f"{column} {operator} ({placeholders})"
    return sql, tuple(value)

def handle_logical(operator, values):
    clauses = []
    params = ()

    sub_operators = {
        '$and': 'AND',
        '$or': 'OR',
        '$not': 'NOT',
    }

    logical_operator = sub_operators.get(operator)
    if logical_operator is None:
        raise ValueError(f"Unknown logical operator: {operator}")

    if operator == '$not' and isinstance(values, list):
        raise ValueError("$not operator expects a single subcondition, received a list")

    subqueries = [values] if logical_operator == 'NOT' else values

    for sub_query in subqueries:
        for sub_key, sub_value in sub_query.items():
            if sub_key in sub_operators:
                sub_clause, sub_params = handle_logical(sub_key, sub_value)
            else:
                sub_clause, sub_params = handle_comparison(sub_key, sub_value)
            clauses.append(sub_clause)
            params += sub_params

    combined_clauses = f" {logical_operator} ".join(f"({clause})" for clause in clauses)
    return combined_clauses, params

def parse_query(query):
    if not isinstance(query, dict):
        raise ValueError("Query must be a dictionary")

    sql, params = '', ()

    for operator, value in query.items():
        if operator in ['$and', '$or', '$not']:
            sub_sql, sub_params = handle_logical(operator, value)
            sql = f"({sub_sql})" if sql else sub_sql
            params += sub_params
        elif operator in ['$eq', '$ne', '$gt', '$lt', '$gte', '$lte']:
            sql, param = handle_comparison(operator, value)
            params += param
        elif operator in ['$like', '$startswith', '$endswith']:
            sql, param = handle_like(operator, value)
            params += param
        elif operator in ['$in', '$nin']:
            sub_sql, sub_param = handle_in_operator(value, operator)
            sql = f"{sql} AND {sub_sql}" if sql else sub_sql
            params += sub_param
        else:
            raise ValueError(f"Unknown operator: {operator}")
        sql = f"({sql})" if sql else sql

    return sql, params

if __name__ == "__main__":
    import unittest

    class TestQueryParser(unittest.TestCase):
        
        def test_eq_num(self):
            query = {'$eq': 5}
            expected_sql = 'num_value = ?'
            expected_params = (5,)
            sql, params = parse_query(query)
            self.assertEqual(sql, expected_sql)
            self.assertEqual(params, expected_params)
        
        def test_eq_string(self):
            query = {'$eq': 'apple'}
            expected_sql = 'string_value = ?'
            expected_params = ('apple',)
            sql, params = parse_query(query)
            self.assertEqual(sql, expected_sql)
            self.assertEqual(params, expected_params)

        def test_eq_pickle(self):
            query = {'$eq': [1, 2, 3]}
            expected_sql = 'pickled_value = ?'
            expected_params = (serialize([1, 2, 3]),)
            sql, params = parse_query(query)
            self.assertEqual(sql, expected_sql)
            self.assertEqual(params, expected_params)

        def test_gt(self):
            query = {'$gt': 10}
            expected_sql = 'num_value > ?'
            expected_params = (10,)
            sql, params = parse_query(query)
            self.assertEqual(sql, expected_sql)
            self.assertEqual(params, expected_params)

        def test_in(self):
            query = {'$in': [1, 2, 3]}
            expected_sql = 'num_value IN (?,?,?)'
            expected_params = (1, 2, 3)
            sql, params = parse_query(query)
            self.assertEqual(sql, expected_sql)
            self.assertEqual(params, expected_params)

        def test_nin(self):
            query = {'$nin': ['apple', 'banana']}
            expected_sql = 'string_value NOT IN (?,?)'
            expected_params = ('apple', 'banana')
            sql, params = parse_query(query)
            self.assertEqual(sql, expected_sql)
            self.assertEqual(params, expected_params)

        def test_like(self):
            query = {'$like': '%test%'}
            expected_sql = 'string_value LIKE ?'
            expected_params = ('%test%',)
            sql, params = parse_query(query)
            self.assertEqual(sql, expected_sql)
            self.assertEqual(params, expected_params)

        def test_startswith(self):
            query = {'$startswith': 'test'}
            expected_sql = 'string_value LIKE ?'
            expected_params = ('test%',)
            sql, params = parse_query(query)
            self.assertEqual(sql, expected_sql)
            self.assertEqual(params, expected_params)

        def test_endswith(self):
            query = {'$endswith': 'test'}
            expected_sql = 'string_value LIKE ?'
            expected_params = ('%test',)
            sql, params = parse_query(query)
            self.assertEqual(sql, expected_sql)
            self.assertEqual(params, expected_params)
        
        def test_or(self):
            query = {'$or': [{'$eq': 5}, {'$eq': 'apple'}]}
            expected_sql = '(num_value = ? OR string_value = ?)'
            expected_params = (5, 'apple')
            sql, params = parse_query(query)
            self.assertEqual(sql, expected_sql)
            self.assertEqual(params, expected_params)
        
        def test_and(self):
            query = {'$and': [{'$gt': 5}, {'$lt': 10}]}
            expected_sql = '(num_value > ? AND num_value < ?)'
            expected_params = (5, 10)
            sql, params = parse_query(query)
            print('-->', sql, '||', params)
            self.assertEqual(sql, expected_sql)
            self.assertEqual(params, expected_params)

        def test_not(self):
            query = {'$not': {'$eq': 5}}
            expected_sql = 'NOT (num_value = ?)'
            expected_params = (5,)
            sql, params = parse_query(query)
            self.assertEqual(sql, expected_sql)
            self.assertEqual(params, expected_params)
    
    unittest.main()

