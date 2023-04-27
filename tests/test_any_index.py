import logging
logging.basicConfig(level=logging.DEBUG)

import random
import string

def random_string(length):
    return ''.join(random.choices(string.ascii_letters, k=length))

def random_key(max_string_length=50):
    return random_string(random.randint(1, max_string_length))

def generate_random_dict_or_list(max_depth=5, max_list_length=10, max_dict_length=10, max_string_length=8, depth=1):
    def random_value(depth):
        if depth > max_depth:
            return random.choice([random.randint(0, 100), random_string(random.randint(1, max_string_length))])

        choice = random.choices(
            population=[0, 1, 2, 3, 4],
            weights=[35, 35, 10 * (max_depth - depth), 10 * (max_depth - depth), 10 * (max_depth - depth)],
            k=1
        )[0]

        if choice == 0:  # Integer
            return random.randint(0, 100)
        elif choice == 1:  # String
            return random_string(random.randint(1, max_string_length))
        elif choice == 2:  # List
            return [random_value(depth + 1) for _ in range(random.randint(1, max_list_length))]
        elif choice == 3:  # Dictionary
            result = {}
            for _ in range(random.randint(1, max_dict_length)):
                key = random_key()
                value = random_value(depth + 1)
                result[key] = value
            return result
        else: # Nested dictionary inside a list
            return generate_random_dict_or_list(max_depth=max_depth, max_list_length=max_list_length, max_dict_length=max_dict_length, max_string_length=max_string_length, depth=depth + 1)

    if random.random() < 0.5: # 50% chance of generating a list
        return [random_value(depth + 1) for _ in range(random.randint(1, max_list_length))]
    else: # 50% chance of generating a dictionary
        result = {}
        for _ in range(random.randint(1, max_dict_length)):
            key = random_key()
            value = random_value(depth + 1)
            result[key] = value
        return result

import unittest
from random import choice
from string import ascii_letters
from time import time
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from liteindex import AnyIndex

class TestAnyIndex(unittest.TestCase):

    def setUp(self):
        self.index_name = 'test_any_index'
        self.db_path = 'test_db'
        self.index = AnyIndex(self.index_name, self.db_path)

    def tearDown(self):
        del self.index
        os.remove(self.db_path)
        
        if os.path.exists(f"{self.db_path}-wal"):
            os.remove(path=f"{self.db_path}-wal")
        
        if os.path.exists(f"{self.db_path}-shm"):
            os.remove(path=f"{self.db_path}-shm")

    def random_key(self, length=10):
        return ''.join(choice(ascii_letters) for _ in range(length))

    def test_set_and_get_item(self):
        test_dict = generate_random_dict_or_list()
        test_key = self.random_key()
        
        self.index[test_key] = test_dict
        self.assertEqual(test_dict, self.index[test_key].get_object())

    def test_update_value(self):
        test_key = self.random_key()
        initial_dict = generate_random_dict_or_list()
        updated_dict = generate_random_dict_or_list()

        self.index[test_key] = initial_dict
        self.index[test_key] = updated_dict

        self.assertEqual(updated_dict, self.index[test_key].get_object())

    def test_del_item(self):
        test_key = self.random_key()
        test_dict = generate_random_dict_or_list()

        self.index[test_key] = test_dict
        del self.index[test_key]

        with self.assertRaises(KeyError):
            _ = self.index[test_key]

    def test_len(self):
        test_keys = [self.random_key() for _ in range(10)]

        for i, key in enumerate(test_keys):
            self.index[key] = generate_random_dict_or_list()
            self.assertEqual(len(self.index), i + 1)

    def test_contains(self):
        test_key = self.random_key()
        test_dict = generate_random_dict_or_list()

        self.index[test_key] = test_dict
        self.assertIn(test_key, self.index)

    def test_iter(self):
        initial_keys = set(self.index.keys())

        test_keys = [self.random_key() for _ in range(10)]
        test_dicts = [generate_random_dict_or_list() for _ in range(10)]

        for key, value in zip(test_keys, test_dicts):
            self.index[key] = value

        iterated_keys = set(self.index.keys())

        # Check if new test keys are present in the iterated keys
        for key in test_keys:
            self.assertIn(key, iterated_keys)

        # Check if initial keys are present in the iterated keys
        for key in initial_keys:
            self.assertIn(key, iterated_keys)

    def test_nested_dict(self):
        test_key = self.random_key()
        logging.debug(f"Test key: {test_key}")

        test_key_2 = self.random_key()
        logging.debug(f"Test key 2: {test_key_2}")

        test_dict = generate_random_dict_or_list()

        test_key_3 = self.random_key(5)
        logging.debug(f"Test key 3: {test_key_3}")

        test_dict[test_key_2] = {test_key_3: "Initial value"}

        logging.debug(f"Test dict: {test_dict}")

        logging.debug(f"trying to set {test_key} to {test_dict}")
        self.index[test_key] = test_dict
        initial_value = self.index[test_key][test_key_2][test_key_3]
        logging.debug(f"Initial value for [{test_key}][{test_key_2}][{test_key_3}]: {initial_value}")

        updated_value = "Updated value"
        self.index[test_key][test_key_2][test_key_3] = updated_value
        logging.debug(f"Updated value for [{test_key}][{test_key_2}][{test_key_3}] to: {updated_value}")

        retrieved_value = self.index[test_key][test_key_2][test_key_3]
        logging.debug(f"Retrieved value for [{test_key}][{test_key_2}][{test_key_3}]: {retrieved_value}")

        self.assertEqual(updated_value, retrieved_value)
        self.assertNotEqual(initial_value, retrieved_value)

    def test_performance(self):
        n = 1000
        test_keys = [self.random_key() for _ in range(n)]
        test_dicts = [generate_random_dict_or_list() for _ in range(n)]

        start_time = time()
        for key, value in zip(test_keys, test_dicts):
            self.index[key] = value

        elapsed_time = time() - start_time
        print(f"Adding {n} items took {elapsed_time:.2f} seconds")
        self.assertLess(elapsed_time, 1, "Adding items took too long")

        start_time = time()
        for key in test_keys:
            _ = self.index[key]

        elapsed_time = time() - start_time
        print(f"Accessing {n} items took {elapsed_time:.2f} seconds")
        self.assertLess(elapsed_time, 1, "Accessing items took too long")

        start_time = time()
        for key in test_keys:
            del self.index[key]

        elapsed_time = time() - start_time
        print(f"Deleting {n} items took {elapsed_time:.2f} seconds")
        self.assertLess(elapsed_time, 1, "Deleting items took too long")



if __name__ == '__main__':
    unittest.main()

