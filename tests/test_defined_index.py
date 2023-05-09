import unittest
import random
import string
from defined_index import DefinedIndex  # Assuming the class is in the defined_index.py module

# Helper functions for generating random data

def random_string(length=10):
    return ''.join(random.choice(string.ascii_letters) for _ in range(length))

def random_address():
    return {
        "street": random_string(),
        "city": random_string(),
        "state": random_string(2),
        "country": random_string()
    }

def random_skill():
    return {
        "name": random_string(),
        "experience": random.randint(1, 10)
    }

def random_education():
    return {
        "degree": random_string(),
        "major": random_string(),
        "institution": {
            "name": random_string(),
            "location": random_address()
        },
        "year": random.randint(1990, 2023)
    }

def random_person():
    return {
        "name": random_string(),
        "age": random.randint(18, 100),
        "job": {
            "title": random_string(),
            "department": {
                "name": random_string(),
                "location": random_address()
            },
            "skills": [random_skill() for _ in range(random.randint(1, 5))]
        },
        "address": random_address(),
        "education": [random_education() for _ in range(random.randint(1, 4))]
    }


class TestDefinedIndex(unittest.TestCase):

    def setUp(self):
        self.schema = {
            "name": "",
            "age": 0,
            "job": {
                "title": "",
                "department": {
                    "name": "",
                    "location": {
                        "city": "",
                        "country": ""
                    }
                },
                "skills": [
                    {
                        "name": "",
                        "experience": 0
                    }
                ]
            },
            "address": {
                "street": "",
                "city": "",
                "state": "",
                "country": ""
            },
            "education": [
                {
                    "degree": "",
                    "major": "",
                    "institution": {
                        "name": "",
                        "location": {
                            "city": "",
                            "country": ""
                        }
                    },
                    "year": 0
                }
            ]
        }

        self.index = DefinedIndex("test_index", schema=self.schema)

    def test_set_get_data_integrity(self):
        item = random_person()
        item_id = "1"
        self.index.set(item_id, item)
        retrieved_item = self.index.get(item_id)
        del retrieved_item["id"]
        del item["id"]
        self.assertEqual(item, retrieved_item)

    def test_partial_item_update(self):
        item = random_person()
        item_id = "2"
        self.index.set(item_id, item)
        
        partial_update = {"job": {"title": "New Title"}}
        self.index.set(item_id, partial_update)
        retrieved_item = self.index.get(item_id)
        del retrieved_item["id"]

        expected_item = {**item, **partial_update}
        del expected_item["id"]

        self.assertEqual(expected_item, retrieved_item)

    def test_search_empty_query(self):
        item = random_person()
        item_id = "3"
        self.index.set(item_id, item)

        results = list(self.index.search({}))

        del item["id"]
        del results[0][1]["id"]
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][1], item)


    def test_search_exact_value(self):
        item1 = random_person()
        item1["age"] = 25
        self.index.set("4", item1)

        item2 = random_person()
        item2["age"] = 30
        self.index.set("5", item2)

        results = list(self.index.search({"age": 25}))
        self.assertEqual(len(results), 1)
        del item1["id"]
        del results[0][1]["id"]
        self.assertEqual(results[0][1], item1)

    def test_search_nested_fields(self):
        item = random_person()
        item["job"]["department"]["name"] = "Engineering"
        self.index.set("6", item)

        results = list(self.index.search({"job": {"department": {"name": "Engineering"}}}))
        self.assertEqual(len(results), 1)
        del results[0][1]["id"]
        del item["id"]
        self.assertEqual(results[0][1], item)

    # def test_search_multiple_values(self):
    #     item1 = random_person()
    #     item1["age"] = 25
    #     self.index.set("7", item1)

    #     item2 = random_person()
    #     item2["age"] = 30
    #     self.index.set("8", item2)

    #     results = list(self.index.search({"age": [25, 30]}))
    #     self.assertEqual(len(results), 2)

    # def test_search_range_comparison(self):
    #     item1 = random_person()
    #     item1["age"] = 25
    #     self.index.set("9", item1)

    #     item2 = random_person()
    #     item2["age"] = 30
    #     self.index.set("10", item2)

    #     results = list(self.index.search({"age": (">", 25)}))
    #     self.assertEqual(len(results), 1)
    #     self.assertEqual(results[0][1], item2)

    def test_search_empty_input(self):
        results = list(self.index.search(None))
        self.assertEqual(len(results), 0)

if __name__ == '__main__':
    unittest.main()
