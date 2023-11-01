# ----------- Update ulimit -----------
import resource


def set_ulimit():
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)

    limit = hard - 1

    for _ in range(1000):
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (limit, hard))
            break
        except:
            limit = limit // 2


# ----------- Hash for bytes -----------
"""
handle large bytes as well by usin 2048 bytes chunks
"""

import hashlib


def hash_bytes(data):
    hash_obj = hashlib.sha256(data)
    hex_dig = hash_obj.hexdigest()
    return hex_dig
