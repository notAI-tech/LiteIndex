import uuid
import time
import random
import string
import unittest
from tabulate import tabulate


def generate_random_dicts(n):
    keys = ["".join(random.choices(string.ascii_letters, k=10)) for i in range(1000)]
    return keys, {
        str(uuid.uuid4()): {
            random.choice(keys): random.choice(
                [
                    None,
                    random.randint(0, 100),
                    random.uniform(0, 1),
                    "".join(random.choices(string.ascii_letters + string.digits, k=10)),
                ]
            )
            for i in range(random.randint(0, 20))
        }
        for j in range(n)
    }


import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from liteindex import AnyIndex
from diskcache import Index


class TestAnyIndexPerformance(unittest.TestCase):
    def test_set_and_update_performance(self):
        all_possible_first_level_keys, random_dicts_dict = generate_random_dicts(100000)

        results = []

        def test_set_speed(module_name, test_name, data_structure, data_dict):
            start_time = time.time()
            for key, value in data_dict.items():
                data_structure[key] = value
            end_time = time.time()
            results.append(
                {
                    "Module Name": module_name,
                    "Test Name": test_name,
                    "Items": len(data_dict),
                    "Time (s)": end_time - start_time,
                }
            )

        def test_update_speed(module_name, test_name, data_structure, data_dict):
            start_time = time.time()
            data_structure.update(data_dict)
            end_time = time.time()
            results.append(
                {
                    "Module Name": module_name,
                    "Test Name": test_name,
                    "Items": len(data_dict),
                    "Time (s)": end_time - start_time,
                }
            )

        def test_random_access_speed(module_name, test_name, data_structure):
            unique_ids = list(random_dicts_dict.keys())
            random.shuffle(unique_ids)
            start_time = time.time()
            for key in unique_ids:
                _ = data_structure[key]
            end_time = time.time()
            results.append(
                {
                    "Module Name": module_name,
                    "Test Name": test_name,
                    "Items": len(unique_ids),
                    "Time (s)": end_time - start_time,
                }
            )

        def test_iteration_speed(module_name, test_name, data_structure):
            start_time = time.time()
            for key, value in data_structure.items():
                pass
            end_time = time.time()
            results.append(
                {
                    "Module Name": module_name,
                    "Test Name": test_name,
                    "Items": len(data_structure),
                    "Time (s)": end_time - start_time,
                }
            )

        index = AnyIndex("test_any_index_in_memory")
        test_set_speed("AnyIndex", "set", index, random_dicts_dict)

        index_2 = AnyIndex("test_any_index_in_memory_2")
        test_update_speed("AnyIndex", "update", index_2, random_dicts_dict)

        index_3 = AnyIndex("test_any_index_in_memory", "test.db")
        test_set_speed("AnyIndex (Disk)", "set", index_3, random_dicts_dict)

        index_4 = AnyIndex("test_any_index_in_memory_2", "test.db")
        test_update_speed("AnyIndex (Disk)", "update", index_4, random_dicts_dict)

        diskcache_index = Index("test_diskcache_index_1")
        test_set_speed("DiskCache", "set", diskcache_index, random_dicts_dict)

        diskcache_index_2 = Index("test_diskcache_index_2")
        test_update_speed("DiskCache", "update", diskcache_index_2, random_dicts_dict)

        d = {}
        test_set_speed("Dict", "set", d, random_dicts_dict)

        d2 = {}
        test_update_speed("Dict", "update", d2, random_dicts_dict)

        test_random_access_speed("AnyIndex", "random access", index)
        test_random_access_speed("AnyIndex", "random access", index_2)
        test_random_access_speed("AnyIndex (Disk)", "random access", index_3)
        test_random_access_speed("AnyIndex (Disk)", "random access", index_4)
        test_random_access_speed("DiskCache", "random access", diskcache_index)
        test_random_access_speed("DiskCache", "random access", diskcache_index_2)
        test_random_access_speed("Dict", "random access", d)
        test_random_access_speed("Dict", "random access", d2)

        test_iteration_speed("AnyIndex", "iteration", index)
        test_iteration_speed("AnyIndex", "iteration", index_2)
        test_iteration_speed("AnyIndex (Disk)", "iteration", index_3)
        test_iteration_speed("AnyIndex (Disk)", "iteration", index_4)
        test_iteration_speed("DiskCache", "iteration", diskcache_index)
        test_iteration_speed("DiskCache", "iteration", diskcache_index_2)
        test_iteration_speed("Dict", "iteration", d)
        test_iteration_speed("Dict", "iteration", d2)

        print(tabulate(results, headers="keys", tablefmt="pretty"))


if __name__ == "__main__":
    unittest.main()
