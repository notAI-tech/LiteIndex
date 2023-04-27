import unittest
import os
from random import randint, choice
from string import ascii_lowercase
from time import time
from concurrent.futures import ThreadPoolExecutor
import diskcache
import shutil

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from liteindex import NumberIndex

class TestNumberIndex(unittest.TestCase):
    def setUp(self):
        self.db_path = "test_database.sqlite"
        self.index_name = "test_index"
        self.index = NumberIndex(self.index_name, self.db_path)

    def tearDown(self):
        os.remove(self.db_path)
        os.remove(path=f"{self.db_path}-wal")
        os.remove(path=f"{self.db_path}-shm")



    def test_basic_operations(self):
        self.index["one"] = 1
        self.index["two"] = 2.0
        self.index["three"] = 3

        self.assertEqual(self.index["one"], 1)
        self.assertEqual(self.index["two"], 2.0)
        self.assertEqual(self.index["three"], 3)

        self.index["one"] = 1.1
        self.assertEqual(self.index["one"], 1.1)

        del self.index["one"]
        self.assertIsNone(self.index.get("one"))

        self.index.clear()
        self.assertEqual(len(self.index), 0)

    def generate_random_key(self):
        return "".join(choice(ascii_lowercase) for _ in range(10))

    def test_performance(self):
        num_items = 10000

        # Test batch_set performance
        items = {self.generate_random_key(): randint(1, num_items) for _ in range(num_items)}

        start_time = time()
        self.index.update(items)
        end_time = time()
        print(f"Batch set {num_items} items: {end_time - start_time:.2f} seconds")

        # Test single set performance
        items = {self.generate_random_key(): randint(1, num_items) for _ in range(num_items)}
        start_time = time()
        for key, value in items.items():
            self.index[key] = value
        end_time = time()
        print(f"Set {num_items} items: {end_time - start_time:.2f} seconds")


        # Test retrieval performance
        start_time = time()
        for key in items.keys():
            _ = self.index[key]
        end_time = time()
        print(f"Retrieve {num_items} items: {end_time - start_time:.2f} seconds")

    def concurrent_write(self, key, value):
        index = NumberIndex(self.index_name, self.db_path)
        index[key] = value

    def test_concurrency(self):
        num_items = 1000
        items = {self.generate_random_key(): randint(1, num_items) for _ in range(num_items)}

        with ThreadPoolExecutor() as executor:
            for key, value in items.items():
                executor.submit(self.concurrent_write, key, value)

        for key, value in items.items():
            self.assertEqual(self.index[key], value)

    def test_diskcache_performance(self):
        cache_path = "test_diskcache"
        cache = diskcache.Index(cache_path)
        num_items = 10000

        # Test set performance
        start_time = time()
        items = {self.generate_random_key(): randint(1, num_items) for _ in range(num_items)}
        for key, value in items.items():
            cache[key] = value
        end_time = time()
        print(f"Set {num_items} items in diskcache: {end_time - start_time:.2f} seconds")

        # Test retrieval performance
        start_time = time()
        for key in items.keys():
            _ = cache[key]
        end_time = time()
        print(f"Retrieve {num_items} items from diskcache: {end_time - start_time:.2f} seconds")

        shutil.rmtree(cache_path)

if __name__ == "__main__":
    unittest.main()
