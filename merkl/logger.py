from time import time
import logging

logging.basicConfig()
logger = logging.getLogger('merkl')


def log_if_slow(f, msg, limit=0.25):
    t0 = time()
    ret = f()
    t1 = time()

    duration = t1 - t0
    if duration > limit:
        logger.debug(msg + f': {duration:.1f}s')

    return ret
