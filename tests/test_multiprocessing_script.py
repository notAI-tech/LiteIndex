from number_index import NumberIndex
from multiprocessing import Pool
test_index = NumberIndex("test_number_index", "test_database.sqlite")

from diskcache import Index
test_index_diskcache = Index("test_database_diskcache")

def f(x):
    test_index[f"{x}"] = x

def diskcache_f(x):
    test_index_diskcache[f"{x}"] = x


if __name__ == "__main__":
    pool = Pool(16)

    from time import time
    start_time = time()
    pool.map(f, range(10000))
    end_time = time()
    print(f"Set 10000 items: {end_time - start_time:.2f} seconds")

    start_time = time()
    pool.map(diskcache_f, range(10000))
    end_time = time()
    print(f"DiskCache Set 10000 items: {end_time - start_time:.2f} seconds")