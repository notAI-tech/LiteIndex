import lmdb
import time
import pickle
import hashlib
import threading

try:
    import zstandard
except:
    zstandard = None


class DictIndex:
    def __init__(self, dir, compression_level=-1):
        self.dir = dir
        self.compression_level = compression_level

        if compression_level is None:
            zstandard = None

        self.local_storage = threading.local()
        self.total_time_putting = 0
        self.total_time_others = 0

    @property
    def __env(self):
        if not hasattr(self.local_storage, "env") or self.local_storage.env is None:
            self.local_storage.env = lmdb.open(
                self.dir,
                map_size=1024 * 1024 * 1024 * 1024,
                create=True,
                max_readers=4096,
                max_dbs=5,
            )
            self.__key_hash_to_value_db = self.local_storage.env.open_db(
                b"key_hash_to_value"
            )
            self.__value_hash_to_key_hash_db = self.local_storage.env.open_db(
                b"value_hash_to_key_hash", dupsort=True
            )
            self.__key_hash_to_key_db = self.local_storage.env.open_db(
                b"key_hash_to_key"
            )
            self.__updated_time_to_key_hash_db = self.local_storage.env.open_db(
                b"updated_time_to_key_hash", dupsort=True, integerkey=True
            )

        return self.local_storage.env

    @property
    def _compressor(self):
        if (
            not hasattr(self.local_storage, "compressor")
            or self.local_storage.compressor is None
        ):
            self.local_storage.compressor = (
                zstandard.ZstdCompressor(level=self.compression_level)
                if self.compression_level is not None
                else False
            )
        return self.local_storage.compressor

    @property
    def _decompressor(self):
        if (
            not hasattr(self.local_storage, "decompressor")
            or self.local_storage.decompressor is None
        ):
            self.local_storage.decompressor = (
                zstandard.ZstdDecompressor()
                if self.compression_level is not None
                else False
            )
        return self.local_storage.decompressor

    def __get_hash_and_value(self, obj):
        value_serialized = (
            pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
            if self._compressor is False
            else self._compressor.compress(
                pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
            )
        )
        value_hash = hashlib.sha256(value_serialized).hexdigest().encode("utf-8")
        return value_hash, value_serialized

    def __deserialize_value(self, value_serialized):
        return (
            pickle.loads(value_serialized)
            if self._decompressor is False
            else pickle.loads(self._decompressor.decompress(value_serialized))
        )

    def __getitem__(self, key):
        key_hash, _ = self.__get_hash_and_value(key)

        with self.__env.begin(write=False) as txn:
            value_serialized = txn.get(key_hash, db=self.__key_hash_to_value_db)
            if value_serialized is None:
                raise KeyError("Key not found")

            return self.__deserialize_value(value_serialized)

    def __setitem__(self, key, value):
        value_hash, value_serialized = self.__get_hash_and_value(value)
        key_hash, key_serialized = self.__get_hash_and_value(key)
        updated_time_bytes = int(time.time() * 1e6).to_bytes(8, "big")

        if txn is None:
            with self.__env.begin(write=True) as txn:
                self.__setitem__(key, value, txn)
        else:
            txn.put(key_hash, value_serialized, db=self.__key_hash_to_value_db)
            txn.put(value_hash, key_hash, db=self.__value_hash_to_key_hash_db)
            txn.put(key_hash, key_serialized, db=self.__key_hash_to_key_db)
            txn.put(updated_time_bytes, key_hash, db=self.__updated_time_to_key_hash_db)

    def __delitem__(self, key):
        key_hash, _ = self.__get_hash_and_value(key)
        with self.__env.begin(write=True) as txn:
            # First fetch the value_serialized in order to remove the reverse mapping
            value_serialized = txn.get(key_hash, db=self.__key_hash_to_value_db)
            if value_serialized is None:
                raise KeyError("Key not found")

            # Compute the value_hash so we can clean up the reverse mapping
            value_hash = hashlib.sha256(value_serialized).hexdigest().encode("utf-8")

            # Remove the key and value associations
            txn.delete(key_hash, db=self.__key_hash_to_value_db)
            txn.delete(key_hash, db=self.__key_hash_to_key_db)
            txn.delete(value_hash, db=self.__value_hash_to_key_hash_db)

    def pop(self, key):
        key_hash, _ = self.__get_hash_and_value(key)
        with self.__env.begin(write=True) as txn:
            value_serialized = txn.pop(key_hash, db=self.__key_hash_to_value_db)
            if value_serialized is None:
                raise KeyError("Key not found")

            txn.pop(key_hash, db=self.__key_hash_to_key_db)

            # Compute the value_hash for the reverse mapping deletion
            value_hash = hashlib.sha256(value_serialized).hexdigest().encode("utf-8")
            txn.delete(value_hash, db=self.__value_hash_to_key_hash_db)

            return self.__deserialize_value(value_serialized)

    def popitem(self):
        with self.__env.begin(write=True) as txn:
            cursor = txn.cursor(db=self.__updated_time_to_key_hash_db)
            try:
                _, key_hash = cursor.first()
            except lmdb.CursorEmptyError:
                raise KeyError("Index is empty")

            # Deleting the values referenced by the key hash
            value_serialized = txn.pop(key_hash, db=self.__key_hash_to_value_db)
            txn.pop(key_hash, db=self.__key_hash_to_key_db)

            # Compute the value_hash for the reverse mapping deletion
            value_hash = hashlib.sha256(value_serialized).hexdigest().encode("utf-8")
            txn.delete(value_hash, db=self.__value_hash_to_key_hash_db)

            # Deleting the associated timestamp entry
            cursor.pop(key_hash)

            return self.__deserialize_value(value_serialized)

    def update(self, _data):
        start = time.time()
        write_data = []
        for key, value in _data.items():
            value_hash, value_serialized = self.__get_hash_and_value(value)
            key_hash, key_serialized = self.__get_hash_and_value(key)
            updated_time_bytes = int(time.time() * 1e6).to_bytes(8, "big")
            write_data.append(
                (
                    key_hash,
                    key_serialized,
                    value_hash,
                    value_serialized,
                    updated_time_bytes,
                )
            )
        self.total_time_others += time.time() - start

        start = time.time()
        with self.__env.begin(write=True) as txn:
            for (
                key_hash,
                key_serialized,
                value_hash,
                value_serialized,
                updated_time_bytes,
            ) in write_data:
                txn.put(key_hash, value_serialized, db=self.__key_hash_to_value_db)
                txn.put(value_hash, key_hash, db=self.__value_hash_to_key_hash_db)
                txn.put(key_hash, key_serialized, db=self.__key_hash_to_key_db)
                txn.put(
                    updated_time_bytes, key_hash, db=self.__updated_time_to_key_hash_db
                )
        self.total_time_putting += time.time() - start


if __name__ == "__main__":
    index = KVIndex("test")
    index["test"] = "test"
    print(index["test"])
    index["test"] = "test2"
    print(index["test"])
    index["test2"] = "test"
    print(index["test2"])
    index["test2"] = "test2"
    print(index["test2"])
    index["test3"] = "test3"
    print(index["test3"])
    index["test3"] = "test3"
    print(index["test3"])
    index["test3"] = "test3"
