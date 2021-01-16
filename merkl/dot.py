import merkl.io
from merkl.future import Future
from merkl.utils import nested_collect
from merkl.exceptions import FutureAccessError


MAX_LEN = 30
MAX_DEPS = 3


def print_dot_graph_nodes(futures, target_fn=None, printed=set()):
    # NOTE: This is confusing code, but probably not worth spending time on...
    for future in futures:
        node_id = future.hash[:6]
        node_label = future.hash[:4]
        is_io = future.fn in [merkl.io._get_file_content, merkl.io._get_file_object]
        code_args_hash = future.code_args_hash[:6] if future.code_args_hash else None
        if not is_io and code_args_hash not in printed:
            fn_name = f'{future.fn.__name__}: {future.fn_code_hash[:4]}'
            if future.fn_code_hash not in printed:
                # Only print a function's deps once, in case of multiple invocations (list may be long)
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

                printed.add(future.fn_code_hash)
            else:
                label = fn_name

            print(f'\t"fn_{code_args_hash}" [shape=record, label="{label}"];')
            printed.add(code_args_hash)
            args_str = ''
            if future.bound_args:
                for key, val in future.bound_args.arguments.items():
                    if len(nested_collect(val, lambda x: isinstance(x, Future))) > 0:
                        continue

                    val_str = f"'{val}'" if isinstance(val, str) else str(val)
                    args_str += (f'{key}={val_str}')[:MAX_LEN] + '\n'

                args_str = args_str.strip()

            if args_str:
                print(f'\t"fn_{code_args_hash}_args" [shape=parallelogram, label="{args_str}"];')
                print(f'\t"fn_{code_args_hash}_args" -> "fn_{code_args_hash}";')

        if node_id not in printed:
            color = 'green' if future.in_cache() else 'red'
            if is_io:
                # NOTE: in this case we store the file path in meta. We use the md5_hash input arg as the hash for
                # display, i.e the hash of the file
                node_label = f'{future.meta}<br/>{future.bound_args.args[0][:4]}'
                shape = 'cylinder'
            else:
                shape = 'parallelogram'

            label = f"< <font color='{color}'>{node_label}</font> >"

            print(f'\t"out_{node_id}" [shape={shape}, style=dashed, label={label}];')
            if not is_io:
                print(f'\t"fn_{code_args_hash}" -> "out_{node_id}"')
            printed.add(node_id)

        edge_name = f'{node_id}-{target_fn}'
        if target_fn and edge_name not in printed:
            print(f'\t"out_{node_id}" -> "fn_{target_fn}"')
            printed.add(edge_name)

        print_dot_graph_nodes(future.parent_futures(), code_args_hash, printed)


def print_dot_graph(futures, rankdir=None):
    printed = set()
    print('digraph D {')
    if rankdir is not None:
        print(f'\trankdir="{rankdir}";')
    print('\tnode [shape=plaintext];')
    print_dot_graph_nodes(futures, printed=printed)
    print('}')
