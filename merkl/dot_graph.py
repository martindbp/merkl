from merkl.future import Future
from merkl.utils import nested_collect
from merkl.exceptions import FutureAccessError


ARG_MAX_LEN = 30


def print_dot_graph_nodes(futures, target_fn=None, printed=set()):
    for future in futures:
        node_id = future.hash[:6]
        node_label = future.hash[:4]
        fn_hash = future.code_args_hash[:6]
        fn_name = future.fn.__name__
        if fn_hash not in printed:
            print(f'\t"fn_{fn_hash}" [shape=diamond, label="{fn_name}"];')
            printed.add(fn_hash)
            args_str = ''
            for key, val in future.bound_args.arguments.items():
                if len(nested_collect(val, lambda x: isinstance(x, Future))) > 0:
                    continue

                args_str += (f'{key}={val}')[:ARG_MAX_LEN] + '\n'

            print(f'\t"fn_{fn_hash}_args" [shape=box, label="{args_str}"];')
            print(f'\t"fn_{fn_hash}_args" -> "fn_{fn_hash}";')

        if node_id not in printed:
            print(f'\t"out_{node_id}" [shape=box, style=dotted, label="{node_label}"];')
            print(f'\t"fn_{fn_hash}" -> "out_{node_id}"')
            printed.add(node_id)
            if target_fn:
                print(f'\t"out_{node_id}" -> "fn_{target_fn}"')

        print_dot_graph_nodes(future.parent_futures(), fn_hash, printed)


def print_dot_graph(futures):
    printed = set()
    print('digraph D {')
    print('\t node [shape=plaintext];')
    print_dot_graph_nodes(futures, printed=printed)
    print('}')
