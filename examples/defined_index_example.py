import sys
sys.path.append("..")
from faker import Faker
from liteindex import DefinedIndex
from datetime import datetime
import numpy as np
import timeit
from tqdm import tqdm
import random

# Instantiate Faker library
fake = Faker()

# Define the schema
schema = {
    "name": "string",
    "age": "number",
    "password": "string",
    "verified": "boolean",
    "birthday": "datetime",
    "profile_picture": "blob",
    "nicknames": "json",
    "user_embedding": "other",
    "user_bio": "compressed_string",
}

# Create an index
index = DefinedIndex(
            name="user_details",
            schema=schema,
            db_path="test.db",
            compression_level=None
        )

# Prepare data for N users
N = 100
users = {
    fake.unique.user_name(): {
        "name": fake.name(),
        "age": fake.random_int(20, 60),
        "password": fake.password(),
        "verified": fake.boolean(),
        "birthday": datetime.combine(fake.date_of_birth(), datetime.min.time()),
        "profile_picture": fake.binary(),
        "nicknames": [fake.first_name(), fake.last_name()],
        "user_embedding": np.random.rand(256),
        "user_bio": fake.text()
    } 
    for _ in tqdm(range(N), desc="Generating data")
}

user_id_list = list(users.keys())

# Test insert performance
start = timeit.default_timer()
for k, v in users.items():
    index.update({k: v})
end = timeit.default_timer()
print(f'Insertion time for {N} users: {end - start} seconds')

# Test count performance
start = timeit.default_timer()
print(f'Number of users: {index.count()}')
end = timeit.default_timer()
print(f'Count time: {end - start} seconds')

# Test search performance
start = timeit.default_timer()
print(len(index.search(query={"age": {"$gt": 30}})), "users with age greater than 30")
end = timeit.default_timer()
print(f'Search time for age greater than 30: {end - start} seconds')

# Test complicated search performance
start = timeit.default_timer()
index.search(query={"age": {"$gt": 30}, "verified": True})
end = timeit.default_timer()
print(f'Search time for age greater than 30 and verified: {end - start} seconds')

# Test search on other fields and blob fields
start = timeit.default_timer()
index.search(query={"user_embedding": users[random.choice(user_id_list)]["user_embedding"]})
end = timeit.default_timer()
print(f'Search time for user embedding: {end - start} seconds')

# Test get performance
random_user = random.choice(user_id_list)

start = timeit.default_timer()
print(random_user, index.get(random_user, select_keys=["name", "age"]))
end = timeit.default_timer()
print(f'Get time: {end - start} seconds')

# get and update
start = timeit.default_timer()
print(random_user, index.get(random_user, update={"age": 6788}, select_keys=["name", "age"]))
end = timeit.default_timer()
print(f'Get and update time: {end - start} seconds')
