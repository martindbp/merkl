from merkl.future import Future
from merkl.utils import nested_collect
from merkl.exceptions import FutureAccessError


MAX_LEN = 30
MAX_DEPS = 3


def print_dot_graph_nodes(futures, target_fn=None, printed=set()):
    for future in futures:
        node_id = future.hash[:6]
        node_label = future.hash[:4]
        fn_hash = future.code_args_hash[:6]
        fn_name = future.fn.__name__
        if fn_hash not in printed:
            clamped = len(future.deps) > MAX_DEPS + 1
            deps = future.deps
            if clamped:
                deps = deps[:MAX_DEPS]

            deps = '|'.join(dep[:MAX_LEN] for dep in deps)
            if clamped:
                deps += f'|... +{len(future.deps)-MAX_DEPS}'

            label = fn_name
            if len(future.deps) > 0:
                label = f'{{{label}|{deps} }}'

            print(f'\t"fn_{fn_hash}" [shape=record, label="{label}"];')
            printed.add(fn_hash)
            args_str = ''
            for key, val in future.bound_args.arguments.items():
                if len(nested_collect(val, lambda x: isinstance(x, Future))) > 0:
                    continue

                args_str += (f'{key}={val}')[:MAX_LEN] + '\n'

            print(f'\t"fn_{fn_hash}_args" [shape=box, label="{args_str}"];')
            print(f'\t"fn_{fn_hash}_args" -> "fn_{fn_hash}";')

        if node_id not in printed:
            color = 'green' if future.in_cache() else 'red'
            label = f"< <font color='{color}'>{node_label}</font> >"
            print(f'\t"out_{node_id}" [shape=box, style=dotted, label={label}];')
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
