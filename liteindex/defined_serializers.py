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
            _record[hashed_key] = int(v) if v is not None else None

        elif _type == "string":
            _record[hashed_key] = v if v is not None else None

        elif _type == "number":
            _record[hashed_key] = v if v is not None else None

        elif _type == "datetime":
            _record[hashed_key] = v.timestamp() if v is not None else None

        elif _type == "compressed_string":
            _record[hashed_key] = (
                sqlite3.Binary(compressor.compress(v.encode()))
                if compressor is not False
                else v.encode()
                if v is not None
                else None
            )

        # blob
        elif _type == "blob":
            _record[f"__size_{hashed_key}"] = len(v) if v is not None else None

            _record[f"__hash_{hashed_key}"] = hash_bytes(v) if v is not None else None

            _record[hashed_key] = (
                sqlite3.Binary(compressor.compress(v) if compressor is not False else v)
                if v is not None
                else None
            )

        elif _type == "other":
            v = (
                pickle.dumps(v, protocol=pickle.HIGHEST_PROTOCOL)
                if v is not None
                else None
            )

            _record[f"__size_{hashed_key}"] = len(v) if v is not None else None
            _record[f"__hash_{hashed_key}"] = hash_bytes(v) if v is not None else None

            _record[hashed_key] = (
                sqlite3.Binary(compressor.compress(v) if compressor is not False else v)
                if v is not None
                else None
            )

        elif _type == "json":
            _record[hashed_key] = json.dumps(v) if v is not None else None

    return _record


def deserialize_record(
    key_hash_to_original_key, hashed_key_schema, record, decompressor
):
    _record = {}
    for k, v in record.items():
        original_key = key_hash_to_original_key[k]
        key_type = hashed_key_schema[k]

        if key_type == "boolean":
            _record[original_key] = bool(v) if v is not None else None

        elif key_type == "string":
            _record[original_key] = v if v is not None else None

        elif key_type == "number":
            _record[original_key] = v if v is not None else None

        elif key_type == "datetime":
            _record[original_key] = (
                datetime.datetime.fromtimestamp(v) if v is not None else None
            )

        elif key_type == "compressed_string":
            _record[original_key] = (
                decompressor.decompress(v).decode()
                if decompressor is not False
                else v.decode()
                if v is not None
                else None
            )

        elif key_type == "blob":
            _record[original_key] = (
                decompressor.decompress(v)
                if decompressor is not False
                else v
                if v is not None
                else None
            )

        elif key_type == "other":
            _record[original_key] = (
                pickle.loads(decompressor.decompress(v))
                if decompressor is not False
                else pickle.loads(v)
                if v is not None
                else None
            )

        elif key_type == "json":
            _record[original_key] = json.loads(v) if v is not None else None

    return _record
