import merkl.io
from merkl.future import Future
from merkl.utils import nested_collect
from merkl.exceptions import FutureAccessError
from merkl import logger


MAX_LEN = 40
MAX_DEPS = 3


def print_dot_graph_nodes(futures, target_fn=None, printed=set()):
    # NOTE: This is very bad code, but probably not worth spending time on...
    for future in futures:
        is_cached_pipeline = future.parent_pipeline_future is not None and len(future.parent_futures) == 0
        node_future = future.parent_pipeline_future if is_cached_pipeline else future
        out_name = future.batch_idx if future.batch_idx is not None else future.out_name

        node_future_hash = node_future.hash
        if not logger.LONG:
            node_future_hash = node_future_hash[:6]
        node_id = f'{out_name}_{node_future_hash}_{node_future.invocation_id}'
        deps_args_hash = None
        if node_future.deps_args_hash:
            deps_args_hash = f'{node_future.deps_args_hash}_{node_future.invocation_id}'
        if not future.is_input and deps_args_hash not in printed:
            fn_name = f'{future.fn_descriptive_name}: {future.fn_code_hash[:4]}'
            if node_future.batch_idx is not None:
                fn_name = f'{future.fn_descriptive_name} (batch): {future.fn_code_hash[:4]}'
            if node_future.fn_code_hash not in printed:
                # Only print a function's deps once, in case of multiple invocations (list may be long)
                clamped = len(future.deps) > MAX_DEPS + 1
                deps = node_future.deps
                if clamped:
                    deps = deps[:MAX_DEPS]

                deps = '|'.join(name[:MAX_LEN] for name, *_ in deps)
                if clamped:
                    deps += f'|... +{len(node_future.deps)-MAX_DEPS}'

                label = fn_name
                if len(node_future.deps) > 0:
                    label = f'{{{label}|{deps} }}'

                printed.add(node_future.fn_code_hash)
            else:
                label = fn_name

            print(f'\t"fn_{deps_args_hash}" [shape=record, label="{label}"];')
            printed.add(deps_args_hash)

        args_str = ''
        if node_future.bound_args:
            for key, val in node_future.bound_args.arguments.items():
                if len(nested_collect(val, lambda x: isinstance(x, Future))) > 0:
                    continue

                if node_future.batch_idx is not None:
                    val = val[node_future.batch_idx]
                    val_str = f"'{val}'" if isinstance(val, str) else str(val)
                    args_str += val_str[:MAX_LEN] + '\n'
                else:
                    val_str = f"'{val}'" if isinstance(val, str) else str(val)
                    args_str += (f'{key}={val_str}')[:MAX_LEN] + '\n'

            args_str = args_str.strip()


        if args_str:
            args_id = deps_args_hash
            args_label = args_str
            if node_future.batch_idx is not None:
                args_id = f'{args_id}_{out_name}'
                args_label = f'{out_name}: {args_label}'
            print(f'\t"fn_{args_id}_args" [shape=plain, style=solid, label="{args_label}"];')
            if f'{args_id}_{deps_args_hash}' not in printed:
                print(f'\t"fn_{args_id}_args" -> "fn_{deps_args_hash}";')
                printed.add(f'{args_id}_{deps_args_hash}')

        if node_id not in printed:
            color = 'green' if future.in_cache() or future.is_input else 'red'
            output_files = future.output_files or []
            if future.is_input or len(output_files) > 0:
                # NOTE: in this case we store the file path in meta
                path = future.meta if future.is_input else '<br/>'.join(path for path, _ in output_files)
                shape = 'cylinder'
                node_label = f'{path}<br/>{future.hash[:4]}'
            else:
                shape = 'parallelogram'
                if out_name is None:
                    node_label = future.hash[:4]
                else:
                    node_label = f'{out_name}: {future.hash[:4]}'

            label = f"< <font color='{color}'>{node_label}</font> >"

            print(f'\t"out_{node_id}" [shape={shape}, style=dashed, label={label}];')
            if not future.is_input:
                print(f'\t"fn_{deps_args_hash}" -> "out_{node_id}"')

            printed.add(node_id)

        edge_name = f'{node_id}-{target_fn}'
        if target_fn and edge_name not in printed:
            print(f'\t"out_{node_id}" -> "fn_{target_fn}"')
            printed.add(edge_name)

        print_dot_graph_nodes(node_future.parent_futures, deps_args_hash, printed)


def print_dot_graph(futures, rankdir=None, transparent_bg=False):
    printed = set()
    print('digraph D {')
    if rankdir is not None:
        print(f'\trankdir="{rankdir}";')
    if transparent_bg:
        print('\tbgcolor="transparent";')
    print('\tnode [shape=plaintext, fillcolor="white", style=filled];')
    print_dot_graph_nodes(futures, printed=printed)
    print('}')
