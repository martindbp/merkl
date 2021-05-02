import functools
from importlib import import_module
import clize

from merkl.future import Future, map_future_to_value
from merkl.utils import nested_map, nested_collect
from merkl.dot import print_dot_graph
from merkl import cache


def evaluate_futures_wrapper(f, no_cache):
    @functools.wraps(f)
    def _wrapper(*args, **kwargs):
        orig, cache.NO_CACHE = cache.NO_CACHE, no_cache
        ret = nested_map(f(*args, **kwargs), map_future_to_value)
        cache.NO_CACHE = orig
        return ret
    return _wrapper


class RunAPI:
    def run(self, module_function, no_cache):
        module_name, function_name = module_function.rsplit('.', 1)

        module = import_module(module_name)
        function = getattr(module, function_name)
        # Function output values may contain Futures, so wrap the function to evaluate them
        function = evaluate_futures_wrapper(function, no_cache)
        clize.run(function, args=['merkl-run', *self.unknown_args])
