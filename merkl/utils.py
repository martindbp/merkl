import hashlib
from functools import wraps


OPERATORS = [
    '__bool__',
    '__not__',
    '__lt__',
    '__le__',
    '__eq__',
    '__ne__',
    '__ge__',
    '__gt__',
    'truth',
    'is_',
    'is_not',
    '__abs__',
    '__add__',
    '__and__',
    '__floordiv__',
    '__index__',
    '__invert__',
    '__lshift__',
    '__mod__',
    '__mul__',
    '__matmul__',
    '__neg__',
    '__or__',
    '__pos__',
    '__pow__',
    '__rshift__',
    '__sub__',
    '__truediv__',
    '__xor__',
    '__concat__',
    '__contains__',
    'countOf',
    '__delitem__',
    '__getitem__',
    'indexOf',
    '__setitem__',
    'length_hint',
    '__len__',
    '__iadd__',
    '__iand__',
    '__iconcat__',
    '__ifloordiv__',
    '__ilshift__',
    '__imod__',
    '__imul__',
    '__imatmul__',
    '__ior__',
    '__irshift__',
    '__ipow__',
    '__isub__',
    '__itruediv__',
    '__ixor__',
    '__hash__',
]


def doublewrap(f):
    """
    a decorator decorator, allowing the decorator to be used as:
    @decorator(with, arguments, and=kwargs)
    or
    @decorator
    """
    @wraps(f)
    def new_dec(*args, **kwargs):
        if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
            # Actual decorated function
            return f(args[0])
        else:
            # Decorator arguments
            return lambda realf: f(realf, *args, **kwargs)

    return new_dec


def nested_map(structure, map_function, convert_tuples_to_lists=False):
    if isinstance(structure, tuple):
        new_tuple = [nested_map(s, map_function) for s in structure]
        if convert_tuples_to_lists:
            return new_tuple
        return tuple(new_tuple)
    elif isinstance(structure, list):
        return [nested_map(s, map_function) for s in structure]
    elif isinstance(structure, dict):
        return {key: nested_map(val, map_function) for key, val in structure.items()}

    return map_function(structure)


def get_hash_memory_optimized(f_path, mode='md5'):
    h = hashlib.new(mode)
    with open(f_path, 'rb') as file:
        block = file.read(512)
        while block:
            h.update(block)
            block = file.read(512)

    return h.hexdigest()
