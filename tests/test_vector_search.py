from onnx_clip import OnnxClip, softmax, get_similarity_scores
from PIL import Image
from glob import glob
import os
import sys
import numpy as np
from tqdm import tqdm

from liteindex import DefinedIndex, DefinedTypes

index = DefinedIndex(
                    "test_vetors",
                    schema = {
                        "embedding": DefinedTypes.normalized_embedding
                    }
                )



onnx_model = OnnxClip(batch_size=1)

query_vectors = []

for f in tqdm(glob(f"{sys.argv[1]}/*")):
    if os.path.splitext(f)[-1].lower() in {".png", ".jpg", ".jpeg"}:
        embedding = onnx_model.get_image_embeddings([Image.open(f).convert("RGB")])[0]
        embedding = embedding / np.linalg.norm(embedding)
        query_vectors.append(embedding)

        index.update(
            {
                f: {
                    "embedding": embedding,
                }
            }
        )

for query_vector in query_vectors:
    results = index.search(
        {},
        sort_by="embedding",
        reversed_sort=True,
        sort_by_embedding=query_vector,
        select_keys=[],
        return_metadata=True,
        n=1
    )

    print(results)
    print('----->>>>')
