import unittest
from defined_index import DefinedIndex

class TestDefinedIndex(unittest.TestCase):
    def setUp(self):
        self.schema = {
            "name": "",
            "age": 0,
            "is_student": False,
            "address": {
                "street": "",
                "city": "",
                "state": "",
                "country": ""
            },
            "courses": [
                {
                    "name": "",
                    "credits": 0,
                    "instructor": {
                        "name": "",
                        "title": ""
                    }
                }
            ]
        }
        self.index = DefinedIndex("test_index", schema=self.schema)
        self.item = {
            "name": "John Doe",
            "age": 30,
            "is_student": True,
            "address": {
                "street": "123 Main St",
                "city": "New York",
                "state": "NY",
                "country": "USA"
            },
            "courses": [
                {
                    "name": "Computer Science",
                    "credits": 3,
                    "instructor": {
                        "name": "Dr. Smith",
                        "title": "Professor"
                    }
                }
            ]
        }
        self.index.set("1", self.item)

    def compare_items(self, item1, item2):
        return {k: v for k, v in item1.items() if k != 'id'} == {k: v for k, v in item2.items() if k != 'id'}

    def test_set_and_get(self):
        retrieved_item = self.index.get("1")
        self.assertTrue(self.compare_items(retrieved_item, self.item))

    def test_search(self):
        query = {"address": {"state": "NY"}}
        results = list(self.index.search(query))
        self.assertEqual(len(results), 1)
        self.assertTrue(self.compare_items(results[0][1], self.item))

    def test_count(self):
        query = {"is_student": True}
        count = self.index.count(query)
        self.assertEqual(count, 1)

    def test_sum_and_average(self):
        query = {"is_student": True}
        total_age = self.index.sum("age", query)
        average_age = self.index.average("age", query)
        self.assertEqual(total_age, 30)
        self.assertEqual(average_age, 30)

if __name__ == '__main__':
    unittest.main()
