#### Example schema

python
schema = {
    "name": "string",
    "age": "number",
    "password": "string",
    "verified": "boolean",
    "birthday": "datetime",
    "address": "json",
    "profile_picture": "blob",
    "description": "compressed_string",
    "tag_line_embedding": "other"
}


- The query language is a subset of MongoDB's query language
- The multiple examples shown below can be combined to create complex queries

#### Example of if comparision, equality, and or queries

| Query                                | Explanation                                                                                                |
|--------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| {}                                               | *                                                                                        |
| {"age": None}                                      | age is NULL                                          |
| {"age": 25}                                      | age == 25                                                                      |
| {"age": 25, "name": "john"}                      | age == 25 and name == john                                                 |
| {"age": {"$gte": 20, "$lte": 30}}                | age >= 20 and age <= 30                                                     |
| {"$and": [{"age": {"$gte": 20, "$lte": 30}}, {"name": "john"}]} | (name == john) and (age >= 20 and age <= 30) |
| {"$or": [{"age": 25}, {"name": "john"}]}         | (name == john) or (age == 25) |
| {"age": {"$in": [25, 30]}, "name": {"$nin": ["john", "jane"]}}   | (age == 25 or age == 30) and (name != "john" and name != "jane") |
| {"age": {"$ne": 25}}                               | age != 25                                            |
| {"age": {"$gt": 20, "$lt": 30}}                    | (age > 20) and (age < 30)                            |
| {"$or": [{"age": {"$gte": 20, "$lte": 30}}, {"name": "john"}]} | ((age >= 20) and (age <= 30)) or (name == "john")    |


#### Example of queries on blob or other

| Query                                | Explanation                                                                                                |
|--------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| {"profile_picture": b"......"}                   | profile_picture == b"......"                                                                               |
| {"profile_picture": {"$gt": b"......"}}         | profile_picture > b"......"                                                                                |


#### Example of regex queries on string





