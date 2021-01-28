from merkl import task, HashMode

my_global_variable = 4

def _my_fn():
    return 3


@task(hash_mode=HashMode.FIND_DEPS)
def my_task():
    return _my_fn() * my_global_variable
