from functools import wraps

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


def nested_map(structure, map_function):
    if isinstance(structure, tuple):
        new_tuple = []
        for value in structure:
            new_tuple.append(nested_map(value, map_function))
        return tuple(new_tuple)
    elif isinstance(structure, list):
        for i in range(len(structure)):
            structure[i] = nested_map(structure[i], map_function)
    elif isinstance(structure, dict):
        new_dict = {}
        for key, val in structure.items():
            new_dict[key] = nested_map(val, map_function)

    return map_function(structure)


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
