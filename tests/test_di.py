import unittest
import random
import string
from typing import Optional, Dict, List, Any
from defined_index import DefinedIndex

schema = {
    "name": "str",
    "age": 2,
    "height": 2,
    "is_active": True,
    "address": {
        "street": "str",
        "city": "str",
        "state": "str",
        "zip_code": 2,
        "country": "str",
    },
    "skills": [{"name": "str", "level": 2, "tags": ["str"],}],
    "metadata": {
        "created_at": "str",
        "updated_at": "str",
        "notes": {"note1": "str", "note2": "str",},
    },
}


class TestDefinedIndex(unittest.TestCase):
    def random_string(self, length: int = 10):
        return "".join(random.choices(string.ascii_letters, k=length))

    def random_address(self):
        return {
            "street": self.random_string(),
            "city": self.random_string(),
            "state": self.random_string(2),
            "zip_code": random.randint(10000, 99999),
            "country": self.random_string(),
        }

    def random_skill(self):
        return {
            "name": self.random_string(),
            "level": random.randint(1, 10),
            "tags": [self.random_string() for _ in range(3)],
        }

    def random_item(self):
        return {
            "name": self.random_string(),
            "age": random.randint(18, 99),
            "height": random.uniform(4.0, 7.0),
            "is_active": random.choice([True, False]),
            "address": self.random_address(),
            "skills": [self.random_skill() for _ in range(3)],
            "metadata": {
                "created_at": self.random_string(),
                "updated_at": self.random_string(),
                "notes": {
                    "note1": self.random_string(),
                    "note2": self.random_string(),
                },
            },
        }

    def test_set_get(self):
        index = DefinedIndex("test_index", schema=schema)
        item = self.random_item()
        item_id = self.random_string()

        index.set(item_id, item)

        retrieved_item = index.get(item_id)
        self.assertEqual(item, retrieved_item)

    def test_search(self):
        index = DefinedIndex("test_index", schema=schema)

        # Insert multiple items
        for _ in range(10):
            item_id = self.random_string()
            item = self.random_item()
            index.set(item_id, item)

        # Search items based on a query
        query = {"is_active": True}
        results = list(index.search(query))

        # Verify that search results match the query
        for result in results:
            _, item = result
            self.assertTrue(item["is_active"])

    # Add tests for count, sum, and average methods


if __name__ == "__main__":
    unittest.main()
