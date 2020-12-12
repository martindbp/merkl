from merkl import node, HashMode

# Some other code
if 3 > 4:
    print('hello world')


@node(hash_mode=HashMode.FUNCTION)
def identical_node(val):
    return val
