import functools
from importlib import import_module
import clize

from merkl.graph import map_merkl_future_to_value
from merkl.utils import nested_map


def evaluate_futures(f):
    @functools.wraps(f)
    def _wrapper(*args, **kwargs):
        return nested_map(f(*args, **kwargs), map_merkl_future_to_value)
    return _wrapper


class RunAPI:
    def run(self, module_function, **kwargs):
        module_name, function_name = module_function.rsplit('.', 1)

        module = import_module(module_name)
        function = getattr(module, function_name)
        # Function output values may contain MerkLFutures, so wrap the function to evaluate them
        function = evaluate_futures(function)
        clize.run(function, args=['merkl-run', *self.unknown_args])
