| Query                                | Explanation                                                                                                |
|--------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| {}                                               | Matches all records                                                                                        |
| {"age": 25}                                      | Matches records where the `age` is 25                                                                      |
| {"age": 25, "name": "john"}                      | Matches records where the `age` is 25 and `name` is 'john'                                                 |
| {"$and": [{"age": {"$gte": 20, "$lte": 30}}, {"name": "john"}]} | Matches records where the age is greater than or equal to 20 and less than or equal to 30, and name is 'john' |
| {"tags_list": ["tag1", "tag2"]}                  | Matches records where `tags_list` contain either 'tag1' or 'tag2'                                          |
| {"$or": [{"age": 25}, {"name": "john"}]}         | Matches records where `age` is 25 or the `name` is 'john'                                                  |
| {"age": {"$in": [25, 30]}, "name": {"$nin": ["john", "jane"]}}   | Matches records where `age` is either 25 or 30 and `name` is neither 'john' nor 'jane'                       |
| {"name": {"$like": "jo%"}}                       | Matches records where `name` starts with 'jo'                                                              |
| {"tag_id_to_name": {"1": "tag1", "2": "tag2"}}   | Matches records where `tag_id_to_name` has key:1-value:'tag1' and key:2-value:'tag2'                        |
| {"age": None}                                    | Matches records where `age` is NULL                                                                         |
| {"age": {"$ne": 25}}                             | Matches records where `age` is not equal to 25                                                              |
| {"age": {"$gt": 20, "$lt": 30}}                  | Matches records where `age` is greater than 20 and less than 30                                            |
| {"$or": [{"age": {"$gte": 20, "$lte": 30}}, {"name": "john"}]} | Matches records where the age is greater than or equal to 20 and less than or equal to 30, or name is 'john' |