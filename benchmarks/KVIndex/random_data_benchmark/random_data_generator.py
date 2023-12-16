import random
import numpy as np
import string

def generate_data(max_depth=1, max_width=1, types=["number", "string", "numpy", "bytes"]):
    if max_depth < 0:
        return generate_leaf(types)

    data_type = random.randint(0, 6)

    if data_type in {0, 1, 2, 3}:
        # Generate dict
        return {'key' + str(_): generate_data(max_depth - 1, max_width) for _ in range(random.randint(1, max_width))}

    elif data_type == 4:
        # Generate list
        return [generate_data(max_depth - 1, max_width) for _ in range(random.randint(1, max_width))]

    else:
        return generate_leaf(types)

def generate_leaf(types):
    leaf_type = random.choice(types)

    if leaf_type == "number":
        # Generate int
        return random.randint(1, 100000000000000000)/ random.randint(1, 100000000000000000)
    elif leaf_type == "string":
        # Generate random string of random size from length 1 to 1000
        return ''.join(random.choice(string.ascii_letters) for i in range(random.randint(1, 2000)))
    elif leaf_type == "numpy":
        # Generate numpy array
        return np.random.rand(random.randint(1, 1000))
    elif leaf_type == "bytes":
        # Generate bytes
        return random.randbytes(random.randint(1, 2000))
