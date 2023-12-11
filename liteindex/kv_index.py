import os
import time
import lmdb
import pickle
import hashlib
import tempfile


class KVIndex:
    EvictNone = "none"
    EvictAny = "any"

    def __init__(
        self,
        dir=tempfile.mkdtemp(),
        fast_mode=False,
        store_key=True,
        eviction_policy=EvictNone,
        max_size_mb=0,
        max_no_of_elements=0,
    ):
        self.fast_mode = fast_mode
        self.store_key = store_key

        self.eviction_policy = eviction_policy

        if self.eviction_policy == KVIndex.EvictNone:
            self.max_no_of_elements = 0
            self.max_size_mb = 0
        elif self.eviction_policy == KVIndex.EvictAny:
            if max_size_mb == 0 and max_no_of_elements == 0:
                raise ValueError(
                    "At least one of max_size_mb or max_no_of_elements must be set when eviction_policy is EvictAny"
                )
        else:
            raise ValueError(f"Unknown eviction_policy: {eviction_policy}")

        self.max_size_mb = max_size_mb
        self.max_no_of_elements = max_no_of_elements

        self.__env = lmdb.open(
            path=dir,
            subdir=True,
            map_size=int(max_size_mb * 1024**2) if max_size_mb > 0 else 512**5,
            metasync=not fast_mode,
            sync=not fast_mode,
            create=True,
            writemap=fast_mode,
            max_readers=2048,
            meminit=False,
            max_dbs=4,
        )

        self.__key_hash_to_value_db = self.__env.open_db(
            b"key_hash_to_value", create=True
        )

        if self.store_key:
            self.__key_hash_to_key_db = self.__env.open_db(
                b"key_hash_to_key", create=True
            )

    def __evict(self, txn, min_to_delete=1):
        if self.max_size_mb:
            stat = txn.stat(db=self.__key_hash_to_value_db)

            current_size_mb = stat["psize"] * stat["leaf_pages"] / 1024**2
            current_record_count = stat["entries"]
            if current_size_mb <= self.max_size_mb * 0.8:
                return

            cursor = txn.cursor(db=self.__key_hash_to_value_db)

            while True:
                for _ in range(max(current_record_count // 3, min_to_delete)):
                    if cursor.first():
                        if self.store_key:
                            key = cursor.key()
                            txn.delete(key, db=self.__key_hash_to_key_db)
                        cursor.delete()
                    else:
                        break

                stat = txn.stat(db=self.__key_hash_to_value_db)
                current_size_mb = stat["psize"] * stat["leaf_pages"] / 1024**2
                current_record_count = stat["entries"]
                if current_size_mb <= self.max_size_mb * 0.8:
                    cursor.close()
                    return
        elif self.max_no_of_elements:
            stat = txn.stat(db=self.__key_hash_to_value_db)
            current_record_count = stat["entries"]
            if current_record_count <= self.max_no_of_elements:
                return

            cursor = txn.cursor(db=self.__key_hash_to_value_db)

            while True:
                for _ in range(max(current_record_count // 3, min_to_delete)):
                    if cursor.first():
                        if self.store_key:
                            key = cursor.key()
                            txn.delete(key, db=self.__key_hash_to_key_db)
                        cursor.delete()
                    else:
                        break

    def __setitem__(self, key, value):
        with self.__env.begin(write=True) as txn:
            self.__evict(txn)
            key = (
                pickle.dumps(key, protocol=pickle.HIGHEST_PROTOCOL)
                if isinstance(key, str)
                else key.encode()
            )
            pickled_value = (
                pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
                if isinstance(value, str)
                else value.encode()
            )

            key_hash = hashlib.sha256(key).digest()

            txn.put(
                key_hash, pickled_value, db=self.__key_hash_to_value_db, overwrite=True
            )

            if self.store_key:
                txn.put(key_hash, key, db=self.__key_hash_to_key_db, overwrite=False)

    def __getitem__(self, key):
        with self.__env.begin(write=False, buffers=True) as txn:
            key = (
                pickle.dumps(key, protocol=pickle.HIGHEST_PROTOCOL)
                if isinstance(key, str)
                else key.encode()
            )
            key_hash = hashlib.sha256(key).digest()

            result = txn.get(key_hash, db=self.__key_hash_to_value_db)

            if result is None:
                raise KeyError(key)

            return pickle.loads(result) if result[0] == 128 else result.decode()

    def update(self, items):
        with self.__env.begin(write=True) as txn:
            self.__evict(txn, min_to_delete=len(items))
            for key, value in items:
                key = (
                    pickle.dumps(key, protocol=pickle.HIGHEST_PROTOCOL)
                    if isinstance(key, str)
                    else key.encode()
                )
                pickled_value = (
                    pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
                    if isinstance(value, str)
                    else value.encode()
                )

                key_hash = hashlib.sha256(key).digest()

                txn.put(
                    key_hash,
                    pickled_value,
                    db=self.__key_hash_to_value_db,
                    overwrite=True,
                )

                if self.store_key:
                    txn.put(
                        key_hash, key, db=self.__key_hash_to_key_db, overwrite=False
                    )

    def getmulti(self, keys):
        with self.__env.begin(write=False, buffers=True) as txn:
            results = []
            for key in keys:
                key = (
                    pickle.dumps(key, protocol=pickle.HIGHEST_PROTOCOL)
                    if isinstance(key, str)
                    else key.encode()
                )
                key_hash = hashlib.sha256(key).digest()

                result = txn.get(key_hash, db=self.__key_hash_to_value_db)

                results.append(
                    pickle.loads(result)
                    if result[0] == 128
                    else result.decode()
                    if result is not None
                    else None
                )

            return results
