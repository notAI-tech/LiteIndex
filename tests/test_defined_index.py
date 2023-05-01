import random
import string
from typing import Dict, Union
import time
import unittest

from defined_index import DefinedIndex


def random_string(length: int) -> str:
    return "".join(random.choices(string.ascii_letters, k=length))


def random_value() -> Union[int, str, list]:
    choice = random.choice(["number", "string", "list"])
    if choice == "number":
        return random.randint(1, 100)
    elif choice == "string":
        return random_string(random.randint(3, 10))
    else:
        return [random.randint(1, 100) for _ in range(random.randint(2, 5))]


def generate_random_dict(size: int) -> Dict[str, Union[int, str, list]]:
    return {random_string(5): random_value() for _ in range(size)}


def generate_n_random_dicts(n, schema):
    def generate_random_value(value_type):
        if value_type == "int":
            return random.randint(1, 100)
        elif value_type == "float":
            return random.uniform(1, 100)
        elif value_type == "list":
            return [random.randint(1, 100) for _ in range(3)]
        else:
            return "".join(random.choices(string.ascii_letters, k=5))

    random_dicts = []
    for _ in range(n):
        random_dict = {}
        for key, value_type in schema.items():
            random_dict[key] = generate_random_value(value_type)
        random_dicts.append(random_dict)
    return random_dicts


class TestDefinedIndex(unittest.TestCase):
    def setUp(self):
        self.schema = generate_random_dict(5)
        self.db = DefinedIndex("test_db", self.schema)

    def test_set_and_get_item(self):
        random_dicts = generate_n_random_dicts(10, self.schema)

        for i, d in enumerate(random_dicts):
            self.db[f"key_{i}"] = d
            self.assertEqual(self.db[f"key_{i}"], d)

    def test_update(self):
        random_dicts = {
            f"key_{i}": d
            for i, d in enumerate(generate_n_random_dicts(10, self.schema))
        }

        self.db.update(random_dicts)
        for key, value in random_dicts.items():
            self.assertEqual(self.db[key], value)

    def test_delete_item(self):
        random_dicts = generate_n_random_dicts(10, self.schema)

        for i, d in enumerate(random_dicts):
            self.db[f"key_{i}"] = d
        for i in range(len(random_dicts)):
            key = f"key_{i}"
            del self.db[key]
            self.assertNotIn(key, self.db)

    def test_contains(self):
        random_dicts = generate_n_random_dicts(10, self.schema)

        for i, d in enumerate(random_dicts):
            self.db[f"key_{i}"] = d
        for i in range(len(random_dicts)):
            self.assertIn(f"key_{i}", self.db)

    def test_len(self):
        random_dicts = generate_n_random_dicts(10, self.schema)

        for i, d in enumerate(random_dicts):
            self.db[f"key_{i}"] = d
        self.assertEqual(len(self.db), len(random_dicts))

    def test_iter(self):
        random_dicts = {
            f"key_{i}": d
            for i, d in enumerate(generate_n_random_dicts(10, self.schema))
        }

        self.db.update(random_dicts)
        for key in self.db:
            self.assertIn(key, random_dicts)

    def test_performance(self):
        start = time.time()
        random_dicts = {
            f"key_{i}": d
            for i, d in enumerate(generate_n_random_dicts(1000, self.schema))
        }

        self.db.update(random_dicts)
        for key, value in random_dicts.items():
            self.assertEqual(self.db[key], value)

        end = time.time()
        print(f"Performance test took: {end - start:.2f} seconds")

    def test_custom_list_append(self):
        random_dicts = generate_n_random_dicts(10, self.schema)

        for i, d in enumerate(random_dicts):
            self.db[f"key_{i}"] = d

        for key in self.db:
            for k, v in self.db[key].items():
                if isinstance(v, list):
                    v.append(100)
                    self.assertIn(100, self.db[key][k])

    def test_custom_list_pop(self):
        random_dicts = generate_n_random_dicts(10, self.schema)

        for i, d in enumerate(random_dicts):
            self.db[f"key_{i}"] = d

        for key in self.db:
            for k, v in self.db[key].items():
                if isinstance(v, list):
                    last_value = v.pop()
                    self.assertNotIn(last_value, self.db[key][k])

    def test_custom_list_remove(self):
        random_dicts = generate_n_random_dicts(10, self.schema)

        for i, d in enumerate(random_dicts):
            self.db[f"key_{i}"] = d

        for key in self.db:
            for k, v in self.db[key].items():
                if isinstance(v, list):
                    value_to_remove = v[-1]
                    v.remove(value_to_remove)
                    self.assertNotIn(value_to_remove, self.db[key][k])

    def test_custom_list_extend(self):
        random_dicts = generate_n_random_dicts(10, self.schema)

        for i, d in enumerate(random_dicts):
            self.db[f"key_{i}"] = d

        for key in self.db:
            for k, v in self.db[key].items():
                if isinstance(v, list):
                    list_to_extend = [101, 102, 103]
                    v.extend(list_to_extend)
                    self.assertTrue(all(x in self.db[key][k] for x in list_to_extend))


if __name__ == "__main__":
    unittest.main()
