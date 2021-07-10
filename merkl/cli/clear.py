import clize
from sigtools.specifiers import forwards_to_function

from merkl.future import Future, map_future_to_value
from merkl.utils import nested_map, nested_collect, import_module_function
from merkl.dot import print_dot_graph
from merkl import cache


def clear_cache_wrapper(f, keep, keep_outs):
    @forwards_to_function(f)
    def _wrapper(*args, **kwargs):
        cache.clear(f(*args, **kwargs), keep, keep_outs)

    return _wrapper


class ClearAPI:
    def clear(self, module_function=None, keep=False, keep_outs=False):
        function = import_module_function(module_function)
        function = clear_cache_wrapper(function, keep, keep_outs)
        clize.run(function, args=['merkl-clear', *self.unknown_args])
