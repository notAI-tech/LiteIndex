import json
import pickle
import sqlite3
import hashlib
import datetime

schema_property_to_column_type = {
    "boolean": "INTEGER",
    "string": "TEXT",
    "number": "NUMBER",
    "datetime": "NUMBER",
    "compressed_string": "BLOB",
    "blob": "BLOB",
    "other": "BLOB",
    "json": "JSON",
}


def hash_bytes(data):
    hash_obj = hashlib.sha256(data)
    hex_dig = hash_obj.hexdigest()
    return hex_dig


def serialize_record(original_key_to_key_hash, schema, record, compressor):
    _record = {}

    for k, _type in schema.items():
        if k not in record:
            continue

        v = record[k]

        hashed_key = original_key_to_key_hash[k]

        if _type == "boolean":
            _record[hashed_key] = None if v is None else int(v)

        elif _type == "string":
            _record[hashed_key] = v

        elif _type == "number":
            _record[hashed_key] = v

        elif _type == "datetime":
            _record[hashed_key] = None if v is None else v.timestamp()

        elif _type == "compressed_string":
            _record[hashed_key] = (
                None if v is None else compressor.compress(v.encode()) if compressor is not False else v.encode()
            )

        # blob
        elif _type == "blob":
            _record[f"__size_{hashed_key}"] = None if v is None else len(v)

            _record[f"__hash_{hashed_key}"] = None if v is None else hash_bytes(v)

            _record[hashed_key] = (
                None if v is None else compressor.compress(v) if compressor is not False else v
            )

        elif _type == "other":
            v = (
                None if v is None else pickle.dumps(v, protocol=pickle.HIGHEST_PROTOCOL)
            )

            _record[f"__size_{hashed_key}"] = None if v is None else len(v)
            _record[f"__hash_{hashed_key}"] = None if v is None else hash_bytes(v)

            _record[hashed_key] = (
                None if v is None else compressor.compress(v) if compressor is not False else v
            )

        elif _type == "json":
            _record[hashed_key] = None if v is None else json.dumps(v)

    return _record


def deserialize_record(
    key_hash_to_original_key, hashed_key_schema, record, decompressor
):
    _record = {}
    for k, v in record.items():
        original_key = key_hash_to_original_key[k]
        key_type = hashed_key_schema[k]

        if key_type == "boolean":
            _record[original_key] = None if v is None else bool(v)

        elif key_type == "string":
            _record[original_key] = v

        elif key_type == "number":
            _record[original_key] = v

        elif key_type == "datetime":
            _record[original_key] = (
                None if v is None else datetime.datetime.fromtimestamp(v)
            )

        elif key_type == "compressed_string":
            _record[original_key] = (
                None if v is None else decompressor.decompress(v).decode() if decompressor is not False else v.decode()
            )

        elif key_type == "blob":
            _record[original_key] = (
                None if v is None else decompressor.decompress(v) if decompressor is not False else v
            )

        elif key_type == "other":
            _record[original_key] = (
                None if v is None else pickle.loads(decompressor.decompress(v)) if decompressor is not False else pickle.loads(v)
            )

        elif key_type == "json":
            _record[original_key] = None if v is None else json.loads(v)

    return _record
