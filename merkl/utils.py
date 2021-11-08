import os
import sys
import ast
import json
import inspect
import hashlib
import textwrap
import signal
from importlib import import_module
from functools import wraps, lru_cache
from inspect import isfunction, ismodule, getmodule
from typing import NamedTuple
from stdlib_list import stdlib_list
from sigtools.specifiers import forwards_to_function, signature
import merkl
from merkl import cache
from merkl.exceptions import TaskOutsError, EvalError
from merkl.logger import logger


DEBUG = False

BUILTIN_MODULES = stdlib_list()

OPERATORS = [
    '__bool__', '__not__', '__lt__', '__le__', '__ne__', '__ge__', 'truth', 'is_', 'is_not',
    '__abs__', '__add__', '__and__', '__floordiv__', '__index__', '__invert__', '__lshift__', '__mod__', '__mul__',
    '__matmul__', '__neg__', '__pos__', '__pow__', '__sub__', '__truediv__', '__xor__',
    '__concat__', '__contains__', 'countOf', '__delitem__', '__getitem__', 'indexOf', '__setitem__', 'length_hint',
    '__len__', '__iadd__', '__iand__', '__iconcat__', '__ifloordiv__', '__ilshift__', '__imod__', '__imul__',
    '__imatmul__', '__ior__', '__irshift__', '__ipow__', '__isub__', '__itruediv__', '__ixor__',
]


def doublewrap(f):
    """
    a decorator decorator, allowing the decorator to be used as:
    @decorator(with, arguments, and=kwargs)
    or
    @decorator
    """
    @forwards_to_function(f)
    def new_dec(*args, **kwargs):
        if len(args) == 1 and len(kwargs) == 0 and callable(args[0]) and not hasattr(args[0], 'type'):
            # Actual decorated function
            return f(args[0])
        else:
            # Decorator arguments
            return lambda realf: f(realf, *args, **kwargs)

    return new_dec


def nested_map(structure, map_function, convert_tuples_to_lists=False, include_level=False, curr_level=0):
    if isinstance(structure, tuple):
        new_tuple = [
            nested_map(s, map_function, convert_tuples_to_lists, include_level, curr_level+1)
            for s in structure
        ]
        if convert_tuples_to_lists:
            return new_tuple
        return tuple(new_tuple)
    elif isinstance(structure, list):
        return [
            nested_map(s, map_function, convert_tuples_to_lists, include_level, curr_level+1)
            for s in structure
        ]
    elif isinstance(structure, dict):
        return {
            key: nested_map(val, map_function, convert_tuples_to_lists, include_level, curr_level+1)
            for key, val in structure.items()
        }

    args = [structure]
    if include_level:
        args.append(curr_level)

    return map_function(*args)


def nested_apply(structure, apply_function, include_level=False, curr_level=0):
    if isinstance(structure, tuple) or isinstance(structure, list):
        for s in structure:
            nested_apply(s, apply_function, include_level, curr_level+1)
    elif isinstance(structure, dict):
        for key, val in structure.items():
            nested_apply(val, apply_function, include_level, curr_level+1)

    args = [structure]
    if include_level:
        args.append(curr_level)

    apply_function(*args)


def nested_collect(structure, collect_function, include_level=False):
    collected = []
    def _collect(*args):
        arg = args if len(args) > 1 else args[0]
        if collect_function(*args):
            collected.append(arg)

        return arg

    nested_apply(structure, _collect, include_level=include_level)
    return collected


class FunctionDep(NamedTuple):
    name: str
    value: object


def find_function_deps(f):
    from merkl.future import Future

    module = getmodule(f)
    try:
        dedented_source = textwrap.dedent(inspect.getsource(f))
    except:
        return []
    name_nodes = [node for node in ast.walk(ast.parse(dedented_source)) if isinstance(node, ast.Name)]
    deps = []
    seen = set()
    for node in name_nodes:
        if node.id in seen:
            continue

        seen.add(node.id)

        if merkl.__dict__.get(node.id):
            continue  # skip references to merkl stuff

        dep = module.__dict__.get(node.id)

        if dep is None:
            continue

        if isinstance(dep, Future):
            deps.append(FunctionDep(node.id, dep))
            continue

        if not isfunction(dep) and not ismodule(dep):
            # Functions and modules are special cases, but anything else
            # that is not serializable to json we skip
            dep = nested_map(dep, map_set_and_dict_to_list)
            try:
                json.dumps(dep)
            except TypeError:
                continue

        # Skip stuff defined in builtin modules
        dep_module = inspect.getmodule(dep)
        dep_module_name = dep_module.__name__ if dep_module is not None else None
        if dep_module_name == '__main__':
            # When module is being run directly, name is not representative, have to use this hack:
            dep_module_name = os.path.splitext(os.path.basename(dep_module.__file__))[0]

        if dep_module_name in BUILTIN_MODULES:
            continue

        deps.append(FunctionDep(node.id, dep))

    return deps


def get_hash_memory_optimized(f_path, mode='md5'):
    h = hashlib.new(mode)
    with open(f_path, 'rb') as file:
        block = file.read(512)
        while block:
            h.update(block)
            block = file.read(512)

    return h.hexdigest()


def get_return_nodes(node, collect):
    if isinstance(node, ast.FunctionDef):
        # Don't collect return nodes in nested functions
        return
    elif isinstance(node, ast.Return):
        collect.append(node)
    elif hasattr(node, 'iter_child_nodes'):
        for child_node in node.iter_child_nodes():
            get_return_nodes(child_node, collect)
    elif hasattr(node, 'body'):
        for child_node in node.body:
            get_return_nodes(child_node, collect)


def get_function_return_info(f):
    try:
        dedented_source = textwrap.dedent(inspect.getsource(f))
    except:
        return set(), set()

    function_ast = ast.parse(dedented_source).body[0]
    return_nodes = []
    for node in ast.iter_child_nodes(function_ast):
        get_return_nodes(node, return_nodes)

    return_types, num_returns = [], []
    for node in return_nodes:
        type_name = type(node.value).__name__
        if type_name == 'Tuple':
            num_returns.append(len(node.value.elts))
        elif type_name == 'Dict':
            try:
                num_returns.append(tuple([key.s for key in node.value.keys]))
            except AttributeError:
                raise TaskOutsError(f'Returned dict literal contains non-string-literal keys')
        else:
            # It it's not tuple, we treat it as a single value
            num_returns.append(1)

        return_types.append(type_name)

    return set(return_types), set(num_returns)


def evaluate_futures(outs, no_cache):
    from merkl.future import Future, map_future_to_value

    orig, cache.NO_CACHE = cache.NO_CACHE, no_cache
    ret = nested_map(outs, map_future_to_value)
    cache.NO_CACHE = orig
    return ret


def collect_dag_futures(future, out_futures, include_parent_pipelines=False):
    out_futures.add(future)
    if include_parent_pipelines and future.parent_pipeline_future is not None:
        out_futures.add(future.parent_pipeline_future)

    for parent_future in future.parent_futures:
        if parent_future in out_futures:
            continue
        collect_dag_futures(parent_future, out_futures)


def _dummy(*args, **kwargs):
    return

default_signature = signature(_dummy)

def signature_with_default(f):
    try:
        return signature(f)
    except ValueError:
        return default_signature


def signature_args_kwargs(f):
    sig = signature_with_default(f)
    args = [v.name for v in sig.parameters.values() if v.default == inspect._empty]
    kwargs = {v.name: v.default for v in sig.parameters.values() if v.default != inspect._empty}
    return args, kwargs


@lru_cache(maxsize=None)
def function_descriptive_name(fn, include_module=True):
    fn_name = fn.__name__ if hasattr(fn, '__name__') else str(fn)
    if not include_module:
        return fn_name

    module = inspect.getmodule(fn)
    module_name = module.__name__ if module is not None else 'unknown'
    return f'{module_name}.{fn_name}'


class DelayedKeyboardInterrupt():
    def __enter__(self):
        self.signal_received = False
        self.old_handler = signal.signal(signal.SIGINT, self.handler)

    def handler(self, sig, frame):
        self.signal_received = (sig, frame)
        logger.debug('SIGINT received. Delaying KeyboardInterrupt.')

    def __exit__(self, type, value, traceback):
        signal.signal(signal.SIGINT, self.old_handler)
        if self.signal_received:
            self.old_handler(*self.signal_received)


eval_immediately = False

class Eval():
    def __init__(self, set_val=True):
        self.set_val = set_val

    def __enter__(self, *args, **kwargs):
        global eval_immediately
        self.prev_val = eval_immediately
        eval_immediately = self.set_val  # normally True

    def __exit__(self, *args, **kwargs):
        global eval_immediately
        eval_immediately = self.prev_val


def import_module_function(module_function_str: str):
    module_name, function_name = module_function_str.rsplit('.', 1)

    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.append(cwd)

    module = import_module(module_name)
    function = getattr(module, function_name)
    return function


def map_set_and_dict_to_list(item):
    if isinstance(item, set):
        return list(sorted(list(item)))
    elif isinstance(item, dict):
        return list(sorted(item.items(), key=lambda x: x[0]))
    return item
