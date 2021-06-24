import functools
from importlib import import_module
import clize

from merkl.io import migrate_output_files
from merkl import cache


def migrate_futures_wrapper(f):
    @functools.wraps(f)
    def _wrap(*args, **kwargs):
        outs = f(*args, **kwargs)
        migrate_output_files(outs)

    return _wrap


class MigrateAPI:
    def migrate(self, module_function):
        module_name, function_name = module_function.rsplit('.', 1)

        module = import_module(module_name)
        function = getattr(module, function_name)
        function = migrate_futures_wrapper(function)
        clize.run(function, args=['merkl-migrate', *self.unknown_args])
