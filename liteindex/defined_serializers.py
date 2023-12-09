import json
import pickle
import sqlite3
import hashlib

schema_property_to_column_type = {
    "boolean": "INTEGER",
    "boolean[]": None,
    "string:boolean": None,
    "string": "TEXT",
    "string[]": None,
    "string:string": None,
    "number": "NUMBER",
    "number[]": None,
    "string:number": None,
    "datetime": "NUMBER",
    "datetime[]": None,
    "string:datetime": None,
    "compressed_string": "BLOB",
    "compressed_string[]": None,
    "string:compressed_string": None,
    "blob": "BLOB",
    "blob[]": None,
    "string:blob": None,
    "other": "BLOB",
    "json": "JSON",
}


def hash_bytes(data):
    hash_obj = hashlib.sha256(data)
    hex_dig = hash_obj.hexdigest()
    return hex_dig


def lists_and_dicts_record_to_sqlite_records(
    _id, lists_and_dicts_record, key_indice_number_to_key_hash, hashed_key_schema
):
    lists_and_dicts_transactions = []
    lists_and_dicts_delete_transactions = []

    for key_indice, __data in lists_and_dicts_record.items():
        if __data is None:
            # TODO: delete from lists_and_dicts_table
            pass
        elif hashed_key_schema[key_indice_number_to_key_hash[key_indice]] in {
            "boolean[]",
            "number[]",
            "datetime[]",
        }:
            for i, _ in enumerate(__data):
                lists_and_dicts_transactions.append(
                    (
                        _id,
                        key_indice,
                        i,
                        None,
                        _,
                        None,
                        None,
                    )
                )
        elif hashed_key_schema[key_indice_number_to_key_hash[key_indice]] in {
            "string:boolean",
            "string:number",
            "string:datetime",
        }:
            for __k, __v in __data.items():
                lists_and_dicts_transactions.append(
                    (
                        _id,
                        key_indice,
                        None,
                        __k,
                        __v,
                        None,
                        None,
                    )
                )
        elif (
            hashed_key_schema[key_indice_number_to_key_hash[key_indice]]
            == "compressed_string[]"
        ):
            for i, _ in enumerate(__data):
                lists_and_dicts_transactions.append(
                    (
                        _id,
                        key_indice,
                        i,
                        None,
                        None,
                        None,
                        _,
                    )
                )
        elif (
            hashed_key_schema[key_indice_number_to_key_hash[key_indice]]
            == "string:compressed_string"
        ):
            for __k, __v in __data.items():
                lists_and_dicts_transactions.append(
                    (
                        _id,
                        key_indice,
                        None,
                        __k,
                        None,
                        None,
                        __v,
                    )
                )
        elif hashed_key_schema[key_indice_number_to_key_hash[key_indice]] == "blob[]":
            for __sizes, __hashes, _data in __data:
                for i, (__size, __hash, _) in enumerate(zip(__sizes, __hashes, _data)):
                    lists_and_dicts_transactions.append(
                        (
                            _id,
                            key_indice,
                            i,
                            None,
                            __size,
                            __hash,
                            _,
                        )
                    )

        elif (
            hashed_key_schema[key_indice_number_to_key_hash[key_indice]]
            == "string:blob"
        ):
            for __k, (__size, __hash, _) in __data.items():
                lists_and_dicts_transactions.append(
                    (
                        _id,
                        key_indice,
                        None,
                        __k,
                        __size,
                        __hash,
                        _,
                    )
                )
        elif hashed_key_schema[key_indice_number_to_key_hash[key_indice]] == "string[]":
            for i, _ in enumerate(__data):
                lists_and_dicts_transactions.append(
                    (
                        _id,
                        key_indice,
                        i,
                        None,
                        None,
                        _,
                        None,
                    )
                )
        elif (
            hashed_key_schema[key_indice_number_to_key_hash[key_indice]]
            == "string:string"
        ):
            for __k, __v in __data.items():
                lists_and_dicts_transactions.append(
                    (
                        _id,
                        key_indice,
                        None,
                        __k,
                        None,
                        __v,
                        None,
                    )
                )

    return lists_and_dicts_transactions, lists_and_dicts_delete_transactions


def serialize_record(
    original_key_to_key_hash, key_hash_to_key_indice_number, schema, record, compressor
):
    _record = {}
    _lists_and_dicts_record = {}

    for k, _type in schema.items():
        if k not in record:
            continue

        v = record[k]

        hashed_key = original_key_to_key_hash[k]
        key_indice = key_hash_to_key_indice_number[hashed_key]

        # Booleans
        if _type == "boolean":
            _record[hashed_key] = int(v) if v is not None else None

        elif _type == "boolean[]":
            _lists_and_dicts_record[key_indice] = (
                [int(_v) for _v in v] if v is not None else None
            )

        elif _type == "string:boolean":
            _lists_and_dicts_record[key_indice] = (
                {_k: int(_v) for _k, _v in v.items()} if v is not None else None
            )

        # Strings
        elif _type == "string":
            _record[hashed_key] = v if v is not None else None

        elif _type == "string[]":
            _lists_and_dicts_record[key_indice] = v if v is not None else None

        elif _type == "string:string":
            _lists_and_dicts_record[key_indice] = v if v is not None else None

        # numbers

        elif _type == "number":
            _record[hashed_key] = v if v is not None else None

        elif _type == "number[]":
            _lists_and_dicts_record[key_indice] = v if v is not None else None

        elif _type == "string:number":
            _lists_and_dicts_record[key_indice] = v if v is not None else None

        # datetime

        elif _type == "datetime":
            _record[hashed_key] = v.timestamp() if v is not None else None

        elif _type == "datetime[]":
            _lists_and_dicts_record[key_indice] = (
                [_v.timestamp() for _v in v] if v is not None else None
            )

        elif _type == "string:datetime":
            _lists_and_dicts_record[key_indice] = (
                {_k: _v.timestamp() for _k, _v in v.items()} if v is not None else None
            )

        # compressed string
        elif _type == "compressed_string":
            _record[hashed_key] = (
                sqlite3.Binary(compressor.compress(v.encode()))
                if v is not None
                else None
            )
        elif _type == "compressed_string[]":
            _lists_and_dicts_record[key_indice] = (
                [sqlite3.Binary(compressor.compress(_v.encode())) for _v in v]
                if v is not None
                else None
            )
        elif _type == "string:compressed_string":
            _lists_and_dicts_record[key_indice] = (
                {
                    _k: sqlite3.Binary(compressor.compress(_v.encode()))
                    for _k, _v in v.items()
                }
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

        elif _type == "blob[]":
            _lists_and_dicts_record[key_indice] = (
                (
                    [len(_v) for _v in v],
                    [hash_bytes(_v) for _v in v],
                    [
                        sqlite3.Binary(
                            compressor.compress(_v) if compressor is not False else _v
                        )
                        for _v in v
                    ],
                )
                if v is not None
                else None
            )

        elif _type == "string:blob":
            _lists_and_dicts_record[key_indice] = (
                (
                    {_k: len(_v) for _k, _v in v.items()},
                    {_k: hash_bytes(_v) for _k, _v in v.items()},
                    {
                        _k: sqlite3.Binary(
                            compressor.compress(_v) if compressor is not False else _v
                        )
                        for _k, _v in v.items()
                    },
                )
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

    return _record, _lists_and_dicts_record


def deserialize_record(
    key_hash_to_original_key, hashed_key_schema, record, decompressor
):
    _record = {}
    for k, v in record.items():
        original_key = key_hash_to_original_key[k]
        key_type = hashed_key_schema[k]

        if key_type == "boolean":
            _record[original_key] = bool(v) if v is not None else None

        elif key_type == "boolean[]":
            _record[original_key] = [bool(_v) for _v in v] if v is not None else None
        elif key_type == "string:boolean":
            _record[original_key] = (
                {_k: bool(_v) for _k, _v in v.items()} if v is not None else None
            )

        elif key_type == "string":
            _record[original_key] = v if v is not None else None
        elif key_type == "string[]":
            _record[original_key] = v if v is not None else None
        elif key_type == "string:string":
            _record[original_key] = v if v is not None else None

        elif key_type == "number":
            _record[original_key] = v if v is not None else None
        elif key_type == "number[]":
            _record[original_key] = v if v is not None else None
        elif key_type == "string:number":
            _record[original_key] = v if v is not None else None

        elif key_type == "datetime":
            _record[original_key] = (
                datetime.datetime.fromtimestamp(v) if v is not None else None
            )
        elif key_type == "datetime[]":
            _record[original_key] = (
                [datetime.datetime.fromtimestamp(_v) for _v in v]
                if v is not None
                else None
            )
        elif key_type == "string:datetime":
            _record[original_key] = (
                {_k: datetime.datetime.fromtimestamp(_v) for _k, _v in v.items()}
                if v is not None
                else None
            )

        elif key_type == "compressed_string":
            _record[original_key] = (
                decompressor.decompress(v).decode() if v is not None else None
            )
        elif key_type == "compressed_string[]":
            _record[original_key] = (
                [decompressor.decompress(_v).decode() for _v in v]
                if v is not None
                else None
            )
        elif key_type == "string:compressed_string":
            _record[original_key] = (
                {_k: decompressor.decompress(_v).decode() for _k, _v in v.items()}
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
        elif key_type == "blob[]":
            _record[original_key] = (
                [
                    decompressor.decompress(_v) if decompressor is not False else _v
                    for _v in v
                ]
                if v is not None
                else None
            )
        elif key_type == "string:blob":
            _record[original_key] = (
                {
                    _k: decompressor.decompress(_v) if decompressor is not False else _v
                    for _k, _v in v.items()
                }
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
