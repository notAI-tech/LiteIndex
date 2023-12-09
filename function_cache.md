## function_cache

- based on DefinedIndex
- cache function results, especially useful for slow, cpu heavy functions
- cache is stored in a file, so it can be reused between sessions and is multi-process, thread safe

```python
from liteindex import function_cache

@function_cache
def slow_function(a, b):
    return a + b

slow_function(1, 2)
```

