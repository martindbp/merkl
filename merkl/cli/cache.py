from importlib import import_module
import clize
from sigtools.specifiers import forwards_to_function

from merkl.future import Future, map_future_to_value
from merkl.utils import nested_map, nested_collect
from merkl.dot import print_dot_graph
from merkl import cache


def print_graph_wrapper(f, rankdir, transparent_bg, no_cache):
    @forwards_to_function(f)
    def _wrapper(*args, **kwargs):
        futures = nested_collect(f(*args, **kwargs), lambda x: isinstance(x, Future))

    return _wrapper


class CacheAPI:
    def cache(self, module_function=None, keep=False, clear=False):
        module_name, function_name = module_function.rsplit('.', 1)
        module = import_module(module_name)
        function = getattr(module, function_name)
        function = print_graph_wrapper(function, rankdir, transparent_bg, no_cache)
        clize.run(function, args=['merkl-cache', *self.unknown_args])
