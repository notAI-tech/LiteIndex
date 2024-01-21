try:
    from .common_utils import EvictionCfg
except ImportError:
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


def __get_column_name(value):
    if isinstance(value, (int, float)):
        return "num_value"
    elif isinstance(value, bool):
        return "num_value"
    elif isinstance(value, str):
        return "string_value"
    else:
        return "pickled_value"


def create_where_clause(query):
    op_map = {
        "$eq": "=",
        "$gt": ">",
        "$gte": ">=",
        "$lt": "<",
        "$lte": "<=",
        "$neq": "!=",
        "$in": "IN",
        "$nin": "NOT IN",
        "$startswith": "LIKE",
        "$endswith": "LIKE",
        "$regex": "REGEXP",
    }

    wheres = []
    args = []
    for op, value in query.items():
        if op == "$and" or op == "$or":
            subwheres = []
            for sv in value:
                sw, sa = create_where_clause(sv)
                subwheres.append(sw)
                args.extend(sa)
            wheres.append(
                " ({}) ".format(
                    " AND ".join(subwheres) if op == "$and" else " OR ".join(subwheres)
                )
            )
        elif op == "$startswith":
            wheres.append("{} LIKE ?".format(__get_column_name(value)))
            args.append(value + "%")
        elif op == "$endswith":
            wheres.append("{} LIKE ?".format(__get_column_name(value)))
            args.append("%" + value)
        elif op in ("$in", "$nin"):
            wheres.append(
                "{} {} ({})".format(
                    __get_column_name(value[0]), op_map[op], ",".join("?" * len(value))
                )
            )
            args.extend(value)
        elif op == "$regex":
            wheres.append("{} REGEXP ?".format(__get_column_name(value)))
            args.append(value)
        else:
            if value is None:
                # num_value, string_value, pickled_value all should be NULL
                wheres.append(
                    "num_value IS NULL AND string_value IS NULL AND pickled_value IS NULL"
                )
            else:
                column_name = __get_column_name(value)
                if column_name == "pickled_value":
                    if not isinstance(value, bytes):
                        value = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)

                if isinstance(value, bool):
                    wheres.append(
                        f"(pickled_value {op_map[op]} ? AND num_value = {int(value)})"
                    )
                    args.append(b"")
                else:
                    wheres.append(f"{column_name} {op_map[op]} ?")
                    args.append(value)

    return " AND ".join(wheres), args


if __name__ == "__main__":
    print(create_where_clause({"$eq": 1}))
    print(create_where_clause({"$eq": True}))
    print(create_where_clause({"$or": [{"$eq": 1}, {"$eq": False}]}))
    print(create_where_clause({"$gt": 1}))
    print(create_where_clause({"$gte": 1}))
    print(create_where_clause({"$lt": 1}))
    print(create_where_clause({"$lte": 1}))
    print(create_where_clause({"$neq": 1}))
    print(create_where_clause({"$in": [1, 2, 3]}))
    print(create_where_clause({"$nin": [1, 2, 3]}))
    print(create_where_clause({"$startswith": "abc"}))
    print(create_where_clause({"$endswith": "abc"}))
    print(create_where_clause({"$or": [{"$eq": 1}, {"$eq": 2}]}))
    print(create_where_clause({"$and": [{"$eq": 1}, {"$eq": 2}]}))

    # and (a or b) and (c or d)
    print(
        create_where_clause(
            {
                "$and": [
                    {"$or": [{"$eq": 1}, {"$eq": 2}]},
                    {"$or": [{"$eq": 3}, {"$eq": 4}]},
                ]
            }
        )
    )
