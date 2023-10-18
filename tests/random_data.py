import secrets

def generate_random_bytes(min_size, max_size):
    # Generate a random size between min_size and max_size (inclusive)
    size = secrets.randbelow(max_size - min_size + 1) + min_size

    # Generate random bytes of the generated size
    random_bytes = secrets.token_bytes(size)

    return random_bytes

def gener

