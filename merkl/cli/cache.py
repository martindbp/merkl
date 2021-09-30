import clize
from sigtools.specifiers import forwards_to_function

from merkl.future import Future, map_future_to_value
from merkl.utils import nested_map, nested_collect, import_module_function
from merkl.dot import print_dot_graph
from merkl import cache


class CacheAPI:
    def cache(self, module_function, clear=False):
        if module_function is not None:
            count, size = cache.SqliteCache.get_stats(module_function)
            print(f'Num entries: {count}')
            print(f'Total size: {size} bytes ({size / 10e6}M)')
        else:
            for module_function, count, size in sorted(cache.SqliteCache.get_stats(), key=lambda x: x[2]):
                print(f'{size/10e6:.2f}M\t{count}\t{module_function}')
