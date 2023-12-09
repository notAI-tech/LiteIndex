import os
import tempfile


def function_cache(cache_dir=tempfile.mkdtemp(), max_size_on_disk_mb=200):
    env = lmdb.open(cache_dir, map_size=max_size_on_disk_mb * 1024 * 1024)

    def decorator(func):
        def wrapper(*args, **kwargs):
            key = hashlib.sha256(
                pickle.dumps((args, kwargs), protocol=pickle.HIGHEST_PROTOCOL)
            ).digest()
            result = None
            try:
                with env.begin(write=False) as txn:
                    result = txn.get(key)
                    if result is not None:
                        return pickle.loads(result)
                result = func(*args, **kwargs)
                with env.begin(write=True) as txn:
                    txn.put(key, pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL))

            except lmdb.MapFullError:
                with env.begin(write=True) as txn:
                    cursor = txn.cursor()
                    if cursor.first():
                        count_to_delete = int(txn.stat()["entries"] / 10)
                        for _ in range(count_to_delete):
                            cursor.delete()

                with env.begin(write=True) as txn:
                    txn.put(key, pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL))

            return result

        return wrapper

    return decorator


if __name__ == "__main__":

    @function_cache(map_size=100000)
    def test_function(a, b):
        return a + b

    for i in range(1000):
        test_function(i, i)
