# LiteIndex
in-process, easy to use object storage implenetations for Python


```python
pip install --upgrade liteindex
```

## DefinedIndex
```
from liteindex import DefinedIndex

user_details_schema = {
    "name": "",
    "age": 0,
    "password": "",
    "verified": False,
    "nicknames": [],
    "address_details": {},
    "profile_picture": b"",
    "tag_line": "",
}

# db path defaults to memory
# auto_key defaults to False
index = DefinedIndex(
            name="user_details",
            schema=schema,
            db_path="./test.liteindex"
        )
```
- `schema` has toe be specified
