import functools
from importlib import import_module
import clize

from merkl.task import map_placeholder_to_value
from merkl.utils import nested_map


def evaluate_placeholders(f):
    @functools.wraps(f)
    def _wrapper(*args, **kwargs):
        return nested_map(f(*args, **kwargs), map_placeholder_to_value)
    return _wrapper


class RunAPI:
    def run(self, module_function):
        module_name, function_name = module_function.rsplit('.', 1)

        module = import_module(module_name)
        function = getattr(module, function_name)
        # Function output values may contain MerkLFutures, so wrap the function to evaluate them
        function = evaluate_placeholders(function)
        clize.run(function, args=['merkl-run', *self.unknown_args])
