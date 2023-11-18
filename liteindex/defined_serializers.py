import json
import pickle
import sqlite3
import hashlib

def hash_bytes(data):
    hash_obj = hashlib.sha256(data)
    hex_dig = hash_obj.hexdigest()
    return hex_dig

def serialize_record(original_key_to_key_hash, schema, record, compressor):
    _record = {}
    for k, v in record.items():
        hashed_key = original_key_to_key_hash[k]
        if v is None:
            _record[hashed_key] = None

        elif schema[k] == "other":
            v = pickle.dumps(v, protocol=5)
            _record[f"__size_{hashed_key}"] = len(v)
            _record[
                f"__hash_{hashed_key}"
            ] = common_utils.hash_bytes(v)

            _record[hashed_key] = sqlite3.Binary(
                compressor.compress()
                if compressor is not False
                else pickle.dumps(v, protocol=5)
            )
        
        elif schema[k] == "datetime":
            _record[hashed_key] = v.timestamp()

        elif schema[k] == "datetime[]":
            _record[hashed_key] = [
                _v.timestamp() for _v in v
            ]
        
        elif schema[k] == "string:datetime":
            _record[hashed_key] = {
                _k: _v.timestamp() for _k, _v in v.items()
            }

        elif schema[k] == "json":
            _record[hashed_key] = json.dumps(v)
        
        elif schema[k] == "compressed_json":
            _record[hashed_key] = sqlite3.Binary(compressor.compress(json.dumps(v).encode()))

        elif schema[k] == "boolean":
            _record[hashed_key] = int(v)

        elif schema[k] == "boolean[]":
            _record[hashed_key] = [int(_v) for _v in v]
        
        elif schema[k] == "string:boolean":
            _record[hashed_key] = {
                _k: int(_v) for _k, _v in v.items()
            }

        elif schema[k] == "blob":
            _record[f"__size_{hashed_key}"] = len(v)
            _record[
                f"__hash_{hashed_key}"
            ] = common_utils.hash_bytes(v)
            _record[hashed_key] = sqlite3.Binary(
                compressor.compress(v) if compressor is not False else v
            )
        
        elif schema[k] == "blob[]":
            _record[f"__size_{hashed_key}"] = [
                len(_v) for _v in v
            ]
            
            _record[
                f"__hash_{hashed_key}"
            ] = [
                common_utils.hash_bytes(_v) for _v in v
            ]

            _record[hashed_key] = [
                sqlite3.Binary(
                    compressor.compress(_v)
                    if compressor is not False
                    else _v
                )
                for _v in v
            ]
        
        elif schema[k] == "string:blob":
            _record[f"__size_{hashed_key}"] = {
                _k: len(_v) for _k, _v in v.items()
            }

            _record[
                f"__hash_{hashed_key}"
            ] = {
                _k: common_utils.hash_bytes(_v) for _k, _v in v.items()
            }

            _record[hashed_key] = {
                _k: sqlite3.Binary(
                    compressor.compress(_v)
                    if compressor is not False
                    else _v
                )
                for _k, _v in v.items()
            }
        
        elif schema[k] == "compressed_string":
            _record[hashed_key] = sqlite3.Binary(compressor.compress(v.encode()))
        elif schema[k] == "compressed_string[]":
            _record[hashed_key] = [
                sqlite3.Binary(compressor.compress(_v.encode()))
                for _v in v
            ]
        elif schema[k] == "string:compressed_string":
            _record[hashed_key] = {
                _k: sqlite3.Binary(compressor.compress(_v.encode()))
                for _k, _v in v.items()
            }
            
        else:
            _record[hashed_key] = v

    return _record


def deserialize_record(key_hash_to_original_key, schema, record, decompressor, return_compressed=False):
    _record = {}
    for k, v in record.items():
        if v is None:
            _record[key_hash_to_original_key[k]] = None

        elif schema[key_hash_to_original_key[k]] == "other":
            _record[key_hash_to_original_key[k]] = (
                pickle.loads(
                    decompressor.decompress(v)
                    if (decompressor is not False or return_compressed)
                    else v
                )
                if not return_compressed
                else v
            )
        elif schema[key_hash_to_original_key[k]] == "datetime":
            _record[
                key_hash_to_original_key[k]
            ] = datetime.datetime.fromtimestamp(v)
        elif schema[key_hash_to_original_key[k]] == "json":
            _record[key_hash_to_original_key[k]] = json.loads(v)
        elif schema[key_hash_to_original_key[k]] == "boolean":
            _record[key_hash_to_original_key[k]] = bool(v)
        elif schema[key_hash_to_original_key[k]] == "blob":
            _record[key_hash_to_original_key[k]] = (
                bytes(
                    decompressor.decompress(v)
                    if decompressor is not False
                    else v
                )
                if not return_compressed
                else v
            )
        else:
            _record[key_hash_to_original_key[k]] = v

    return _record




