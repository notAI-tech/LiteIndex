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
