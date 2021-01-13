import functools
from importlib import import_module
import clize

from merkl.future import Future, map_future_to_value
from merkl.utils import nested_map, nested_collect
from merkl.dot import print_dot_graph
from merkl.cache import CacheOverride, get_cache_from_arg


def print_graph_wrapper(f, cache, rankdir):
    @functools.wraps(f)
    def _wrapper(*args, **kwargs):
        with CacheOverride(cache):
            futures = nested_collect(f(*args, **kwargs), lambda x: isinstance(x, Future))
            print_dot_graph(futures, rankdir)

    return _wrapper


class DotAPI:
    def dot(self, module_function, cache, rankdir):
        module_name, function_name = module_function.rsplit('.', 1)
        module = import_module(module_name)
        cache = get_cache_from_arg(cache)
        function = getattr(module, function_name)
        function = print_graph_wrapper(function, cache, rankdir)
        clize.run(function, args=['merkl-dot', *self.unknown_args])
