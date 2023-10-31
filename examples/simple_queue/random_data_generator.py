import random
import numpy as np
import string

def generate_data(max_depth=2, max_width=3):
    if max_depth < 0:
        return generate_leaf()

    data_type = random.randint(0, 2)

    if data_type == 0:
        # Generate dict
        return {'key'+str(_): generate_data(max_depth - 1, max_width) for _ in range(random.randint(1, max_width))}

    elif data_type == 1:
        # Generate list
        return [generate_data(max_depth - 1, max_width) for _ in range(random.randint(1, max_width))]

    else:
        return generate_leaf()

def generate_leaf():
    leaf_type = random.randint(0, 4)

    if leaf_type == 0:
        # Generate int
        return random.randint(1, 100000000000000000)
    elif leaf_type == 1:
        # Generate string
        return "abcdefghij klmnop qrstuv wxyz" * random.randint(1, 1000)
    elif leaf_type == 2:
        # Generate numpy array
        return np.random.rand(random.randint(1, 500))
    elif leaf_type == 3:
        # Generate bytes
        return random.randbytes(random.randint(1, 2000))
