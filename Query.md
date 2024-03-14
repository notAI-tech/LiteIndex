#### Example schema

```python
schema = {
    "name": "string",
    "age": "number",
    "password": "string",
    "verified": "boolean",
    "birthday": "datetime",
    "address": "json",
    "friend_ids": "list",
    "profile_picture": "blob",
    "description": "string",
    "description_embedding": "normalized_embedding",
}
```


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


#### Example of json queries

| Query                                | Explanation                                                                                                |
|--------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| {"address": {"city": "New York"}}                | address.city == "New York"                                                                                 |
| {"address": {"city": {"$in": ["New York", "San Francisco"]}}} | address.city in ["New York", "San Francisco"] |
| {"address": {"city": {"$ne": "New York"}}}       | address.city != "New York"                                                                                 |
| {"address": {"$contains": ["city", "state"]}}    | address contains "city" and "state"                                                                         |
| {"address": {"city": "New York", "state": "NY"}} | address.city == "New York" and address.state == "NY"                                                      |
| {"address": {"city": {"$or": ["New York", "San Francisco"]}}} | address.city == "New York" or address.city == "San Francisco" |
| {"friend_ids": {"$contains": "123"}}               | friend_ids contains "123"                                                                                     |
| {"friend_ids": {"$contains": ["123", "456"]}}       | friend_ids contains "123" and "456"                                                                           |





