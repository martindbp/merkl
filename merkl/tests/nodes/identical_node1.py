from merkl import node, HashMode


@node(hash_mode=HashMode.FUNCTION)
def identical_node(val):
    return val
