# ----------- Update ulimit -----------
try:
    import resource
except:
    resource = None


def set_ulimit():
    if resource is None:
        return
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)

    limit = hard - 1

    for _ in range(1000):
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (limit, hard))
            break
        except:
            limit = limit // 2


# Eviction policies definition


class EvictionCfg:
    EvictAny = "any"
    EvictLRU = "lru"
    EvictLFU = "lfu"
    EvictFIFO = "fifo"
    EvictNone = None

    max_size_in_mb = 0
    max_number_of_items = 0
    invalidate_after_seconds = 0

    def __init__(
        self,
        policy,
        max_size_in_mb=0,
        max_number_of_items=0,
        invalidate_after_seconds=0,
    ):
        self.policy = policy
        self.max_size_in_mb = max_size_in_mb
        self.max_number_of_items = max_number_of_items
        self.invalidate_after_seconds = invalidate_after_seconds

        if self.policy not in [
            EvictionCfg.EvictAny,
            EvictionCfg.EvictFIFO,
            EvictionCfg.EvictLRU,
            EvictionCfg.EvictLFU,
            EvictionCfg.EvictNone,
        ]:
            raise Exception("Invalid eviction policy")

        if self.policy == EvictionCfg.EvictNone:
            if (
                self.max_size_in_mb
                or self.max_number_of_items
                or self.invalidate_after_seconds
            ):
                raise Exception(
                    "EvictNone policy cannot have max_size_in_mb, max_number_of_items or invalidate_after_seconds configured"
                )

        if self.policy in {
            EvictionCfg.EvictLRU,
            EvictionCfg.EvictLFU,
            EvictionCfg.EvictAny,
            EvictionCfg.EvictFIFO,
        }:
            if not self.max_number_of_items and not self.max_size_in_mb:
                raise Exception(
                    "EvictLRU, EvictLFU and EvictAny policies must have either max_size_in_mb or max_number_of_items configured"
                )
