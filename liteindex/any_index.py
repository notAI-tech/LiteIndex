import re
import os
import time
import json
import pickle
import datetime
import sqlite3
from query_parser import search_query, distinct_query, count_query, delete_query

import hashlib
import pickle


def stable_hash(obj):
    return hashlib.sha256(pickle.dumps(obj)).hexdigest()


class DefinedIndex:
    def __init__(self, name, schema=None, db_path=":memory:"):
        if name.startswith("__"):
            raise ValueError("Index name cannot start with '__'")

        self.name = name
        self.schema = schema
        self.meta_table_name = f"__{name}_meta"
        self.db_path = db_path
        self.key_hash_to_original_key = {}
        self.original_key_to_key_hash = {}

        if not db_path == ":memory:":
            db_dir = os.path.dirname(self.db_path).strip()
            if not db_dir:
                db_dir = "./"

            if not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)

        self._connection = sqlite3.connect(self.db_path, uri=True)

        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=NORMAL")

        self._validate_set_schema_if_exists()
        self._parse_schema()

        self._create_table_and_meta_table()
