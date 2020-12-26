from merkl import task, HashMode


@task(hash_mode=HashMode.FUNCTION)
def identical_task(val):
    return val
