import functools
from importlib import import_module
import clize

from merkl.utils import evaluate_futures
from merkl.dot import print_dot_graph


def evaluate_futures_wrapper(f, no_cache, clear):
    @functools.wraps(f)
    def _wrap(*args, **kwargs):
        outs = f(*args, **kwargs)
        if clear:
            cache.clear(outs, keep=True)
        return evaluate_futures(outs, no_cache)

    return _wrap


class RunAPI:
    def run(self, module_function, no_cache, clear):
        module_name, function_name = module_function.rsplit('.', 1)

        module = import_module(module_name)
        function = getattr(module, function_name)
        # Function output values may contain Futures, so wrap the function to evaluate them
        function = evaluate_futures_wrapper(function, no_cache, clear)
        clize.run(function, args=['merkl-run', *self.unknown_args])
