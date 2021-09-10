import functools
import clize

from merkl.utils import evaluate_futures, import_module_function
from merkl.dot import print_dot_graph
from merkl import cache


def evaluate_futures_wrapper(f, no_cache, clear):
    @functools.wraps(f)
    def _wrap(*args, **kwargs):
        outs = f(*args, **kwargs)

        evaluated_outs = evaluate_futures(outs, no_cache)

        # Clear old outs after new have been calculated
        if clear:
            cache.clear(outs, keep=True)

        return evaluated_outs

    return _wrap


class RunAPI:
    def run(self, module_function, no_cache, clear):
        function = import_module_function(module_function)
        # Function output values may contain Futures, so wrap the function to evaluate them
        function = evaluate_futures_wrapper(function, no_cache, clear)
        clize.run(function, args=['merkl-run', *self.unknown_args], exit=False)
