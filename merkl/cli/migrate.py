import functools
import clize

from merkl.io import migrate_output_files
from merkl.utils import import_module_function
from merkl import cache


def migrate_futures_wrapper(f, glob):
    @functools.wraps(f)
    def _wrap(*args, **kwargs):
        outs = f(*args, **kwargs)
        migrate_output_files(outs, glob)

    return _wrap


class MigrateAPI:
    def migrate(self, glob, module_function):
        function = import_module_function(module_function)
        function = migrate_futures_wrapper(function, glob)
        clize.run(function, args=['merkl-migrate', *self.unknown_args])
