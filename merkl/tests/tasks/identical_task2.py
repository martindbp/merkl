from merkl import task, HashMode

# Some other code
if 3 > 4:
    print('hello world')


@task(hash_mode=HashMode.FUNCTION)
def identical_task(val):
    return val
