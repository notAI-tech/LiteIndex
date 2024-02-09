import json
import pickle
import sqlite3
import hashlib
import datetime

try:
    import numpy as np
except ImportError:
    np = None


class DefinedTypes:
    number = "number"
    string = "string"
    boolean = "boolean"
    datetime = "datetime"
    compressed_string = "compressed_string"
    blob = "blob"
    other = "other"
    json = "json"
    normalized_embedding = "normalized_embedding"


schema_property_to_column_type = {
    DefinedTypes.boolean: "INTEGER",
    DefinedTypes.string: "TEXT",
    DefinedTypes.number: "NUMBER",
    DefinedTypes.datetime: "NUMBER",
    DefinedTypes.compressed_string: "BLOB",
    DefinedTypes.blob: "BLOB",
    DefinedTypes.other: "BLOB",
    DefinedTypes.json: "JSON",
    DefinedTypes.normalized_embedding: "BLOB",
}


def hash_bytes(data):
    hash_obj = hashlib.sha256(data)
    hex_dig = hash_obj.hexdigest()
    return hex_dig


def serialize_record(schema, record, compressor, _id=None, _updated_at=None):
    _record = {} if _id is None else {"id": _id, "updated_at": _updated_at}

    for k, _type in schema.items():
        if k not in record:
            continue

        v = record[k]

        if _type == "boolean":
            _record[k] = None if v is None else int(v)

        elif _type == "string":
            _record[k] = v

        elif _type == "number":
            _record[k] = v

        elif _type == "datetime":
            _record[k] = None if v is None else v.timestamp()

        elif _type == "compressed_string":
            _record[k] = (
                None
                if v is None
                else compressor.compress(v.encode())
                if compressor is not False
                else v.encode()
            )

        # blob
        elif _type == "blob":
            _record[f"__size_{k}"] = None if v is None else len(v)

            _record[f"__hash_{k}"] = None if v is None else hash_bytes(v)

            _record[k] = (
                None
                if v is None
                else compressor.compress(v)
                if compressor is not False
                else v
            )

        elif _type == "other":
            v = None if v is None else pickle.dumps(v, protocol=pickle.HIGHEST_PROTOCOL)

            _record[f"__size_{k}"] = None if v is None else len(v)
            _record[f"__hash_{k}"] = None if v is None else hash_bytes(v)

            _record[k] = (
                None
                if v is None
                else compressor.compress(v)
                if compressor is not False
                else v
            )

        elif _type == "json":
            _record[k] = None if v is None else json.dumps(v)

        elif _type == "normalized_embedding":
            if v is not None:
                try:
                    if v.ndim == 1 and v.dtype == np.float32:
                        v = v.tobytes()
                    else:
                        raise ValueError("Invalid embedding")
                except Exception:
                    raise ValueError("Invalid embedding")

            _record[k] = v

    return _record


def deserialize_record(schema, record, decompressor):
    _record = {}
    for k, v in record.items():
        key_type = schema[k]

        if key_type == "boolean":
            _record[k] = None if v is None else bool(v)

        elif key_type == "string":
            _record[k] = v

        elif key_type == "number":
            _record[k] = v

        elif key_type == "datetime":
            _record[k] = None if v is None else datetime.datetime.fromtimestamp(v)

        elif key_type == "compressed_string":
            _record[k] = (
                None
                if v is None
                else decompressor.decompress(v).decode()
                if decompressor is not False
                else v.decode()
            )

        elif key_type == "blob":
            _record[k] = (
                None
                if v is None
                else decompressor.decompress(v)
                if decompressor is not False
                else v
            )

        elif key_type == "other":
            _record[k] = (
                None
                if v is None
                else pickle.loads(decompressor.decompress(v))
                if decompressor is not False
                else pickle.loads(v)
            )

        elif key_type == "json":
            _record[k] = None if v is None else json.loads(v)

        elif key_type == "normalized_embedding":
            _record[k] = None if v is None else np.frombuffer(v, dtype=np.float32)

    return _record
