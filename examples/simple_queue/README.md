### Using `DefinedIndex` as queue or queues and performance comparison

- Quick glance at !(https://github.com/notAI-tech/LiteIndex/blob/main/DefinedIndex.md#schema)[Schema]

#### 1. Simple FIFO queue

```python
import uuid
from liteindex import DefinedIndex

# A queue to store any type of data

simple_queue = DefinedIndex(
            "example_queue", 
            schema = {
                "data": "other", # any picklable data
            }
        )

# add one item
simple_queue.update({str(uuid.uuid4()): {"data": ANY PICKLABLE OBJECT}})

# batch add multiple items
simple_queue.update({
    str(uuid.uuid4()): {"data": ANY PICKLABLE OBJECT},
    str(uuid.uuid4()): {"data": ANY PICKLABLE OBJECT},
    str(uuid.uuid4()): {"data": ANY PICKLABLE OBJECT},
})


# default pop() first in first out, 1 item, sorted by insertion time
# n=1, query={}, sort_by="updated_at", reversed_sort=False
# {id: {"data": YOUR OBJECT}} is returned
for _id, _ simple_queue.pop():
    _['data'] # YOUR OBJECT

```



