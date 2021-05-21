from time import time
import logging

logging.basicConfig()
logger = logging.getLogger('merkl')
LONG = False


def log_if_slow(f, msg, limit=0.25):
    t0 = time()
    ret = f()
    t1 = time()

    duration = t1 - t0
    if duration > limit:
        logger.debug(msg + f': {duration:.1f}s')

    return ret

def short_hash(hash):
    if hash is None or LONG:
        return hash

    return hash[:8]
