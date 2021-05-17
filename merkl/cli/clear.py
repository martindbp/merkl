from importlib import import_module
import clize
from sigtools.specifiers import forwards_to_function

from merkl.future import Future, map_future_to_value
from merkl.utils import nested_map, nested_collect
from merkl.dot import print_dot_graph
from merkl import cache


def clear_cache_wrapper(f, keep, keep_outs):
    @forwards_to_function(f)
    def _wrapper(*args, **kwargs):
        cache.clear(f(*args, **kwargs), keep, keep_outs)

    return _wrapper


class ClearAPI:
    def clear(self, module_function=None, keep=False, keep_outs=False):
        module_name, function_name = module_function.rsplit('.', 1)
        module = import_module(module_name)
        function = getattr(module, function_name)
        function = clear_cache_wrapper(function, keep, keep_outs)
        clize.run(function, args=['merkl-clear', *self.unknown_args])
