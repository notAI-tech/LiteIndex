#### Example schema

python
schema = {
    "name": "string",
    "age": "number",
    "password": "string",
    "verified": "boolean",
    "nicknames": "flatlist",
    "address_details": "flatdict",
    "profile_picture": "blob",
    "tag_line": "string",
    "tag_line_embedding": "other",
    "birthday": "datetime",
    "metadata": "json",
    "mark_list": "flatlist"
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


<!-- - Example of queries on text (regex is supported)
| Query                                | Explanation                                                                                                |
|--------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| {"name": {"$regex": "doe", "$options": "i"}}     | name contains "doe" (case insensitive)                                                                      | -->



#### Example of queries on flatlist and flatdict


| Query                                | Explanation                                                                                                |
|--------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| {"nicknames": "John"}                             | "John" in nicknames                                                                                       |
| {"nicknames": ["John", "Doe"]}                    | nicknames == ["John", "Doe"]|
| {"nicknames": {"$in": ["John", "Doe"]}}           | "John" in nicknames or "Doe" in nicknames                                                                 |
| {"nicknames": {"$all": ["John", "Doe"]}}          | "John" in nicknames and "Doe" in nicknames                                                                |
| {"nicknames": {"$size": 2}}                       | len(nicknames) == 2                                                                                       |
| {"nicknames.0": "John"}                          | nicknames[0] == "John"        |
| {"nicknames": {"$ne": "John"}}                   | "John" not in nicknames       |


| {"address_details": {"city": "Springfield"}}     | address_details.city == "Springfield"                                                                      |
| {"address_details": {"city": {"$in": ["Springfield", "Chicago"]}}} | address_details.city == "Springfield" or address_details.city == "Chicago" |


#### Example of queries on blob or other

| Query                                | Explanation                                                                                                |
|--------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| {"profile_picture": b"......"}                   | profile_picture == b"......"                                                                               |
| {"profile_picture": {"$gt": b"......"}}         | profile_picture > b"......"                                                                                |


#### Example of regex queries on string





