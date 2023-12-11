# add ../../liteindex/ to path for importing
import sys
sys.path.append("../../")


from liteindex import function_cache, EvictLRU, EvictAny
from diskcache import Cache

# from sentence_transformers import SentenceTransformer

sentences = open("sents.txt").readlines()

import numpy as np
vec = np.random.rand(256)

# model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

@function_cache(dir="./embeddings_cache", eviction_policy=EvictAny)
def get_embeddings(sentence):
    return vec


cache = Cache("./diskcache_embeddings_cache")

@cache.memoize()
def get_embeddings_diskcache(sentence):
    return vec




from tqdm import tqdm

for sentence in tqdm(sentences):
    get_embeddings(sentence)

for sentence in tqdm(sentences):
    get_embeddings_diskcache(sentence)

