from merkl.placeholder import Placeholder
from merkl.utils import nested_collect
from merkl.exceptions import PlaceholderAccessError


ARG_MAX_LEN = 30


def print_dot_graph_nodes(placeholders, target_fn=None, printed=set()):
    for placeholder in placeholders:
        node_id = placeholder.hash[:6]
        node_label = placeholder.hash[:4]
        fn_hash = placeholder.code_args_hash[:6]
        fn_name = placeholder.fn.__name__
        if fn_hash not in printed:
            print(f'\t"fn_{fn_hash}" [shape=diamond, label="{fn_name}"];')
            printed.add(fn_hash)
            args_str = ''
            for key, val in placeholder.bound_args.arguments.items():
                if len(nested_collect(val, lambda x: isinstance(x, Placeholder))) > 0:
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

        print_dot_graph_nodes(placeholder.parent_placeholders(), fn_hash, printed)


def print_dot_graph(placeholders):
    printed = set()
    print('digraph D {')
    print('\t node [shape=plaintext];')
    print_dot_graph_nodes(placeholders, printed=printed)
    print('}')
