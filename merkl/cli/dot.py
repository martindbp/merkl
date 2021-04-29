from importlib import import_module
import clize
from sigtools.specifiers import forwards_to_function

from merkl.future import Future, map_future_to_value
from merkl.utils import nested_map, nested_collect
from merkl.dot import print_dot_graph


def print_graph_wrapper(f, rankdir):
    @forwards_to_function(f)
    def _wrapper(*args, **kwargs):
        futures = nested_collect(f(*args, **kwargs), lambda x: isinstance(x, Future))
        print_dot_graph(futures, rankdir)

    return _wrapper


class DotAPI:
    def dot(self, module_function, rankdir):
        module_name, function_name = module_function.rsplit('.', 1)
        module = import_module(module_name)
        function = getattr(module, function_name)
        function = print_graph_wrapper(function, rankdir)
        clize.run(function, args=['merkl-dot', *self.unknown_args])
