from merkl.utils import OPERATORS, nested_map, nested_collect
from merkl.exceptions import *


def map_future_to_value(val):
    if isinstance(val, Future):
        return val.get()
    return val


class Future:
    def __init__(
        self,
        fn,
        output_hash,
        num_outs=1,
        code_args_hash=None,
        output_index=None,
        caches=[],
        serializer=None,
        bound_args=None,
        outs_shared_cache=None,
    ):
        self.fn = fn
        self.hash = output_hash
        self.num_outs = num_outs
        self.code_args_hash = code_args_hash
        self.output_index = output_index
        self.caches = caches
        self.serializer = serializer
        self.bound_args = bound_args

        # Cache for the all outputs with the respect to a function and its args
        self.outs_shared_cache = outs_shared_cache or {}

    def parent_futures(self):
        if not self.bound_args:
            return []

        is_future = lambda x: isinstance(x, Future)
        return (
            nested_collect(self.bound_args.args, is_future) +
            nested_collect(self.bound_args.kwargs, is_future)
        )

    def get(self):
        if self.code_args_hash and self.code_args_hash in self.outs_shared_cache:
            output = self.outs_shared_cache.get(self.code_args_hash)
        else:
            evaluated_args = nested_map(self.bound_args.args, map_future_to_value) if self.bound_args else []
            evaluated_kwargs = nested_map(self.bound_args.kwargs, map_future_to_value) if self.bound_args else {}
            output = self.fn(*evaluated_args, **evaluated_kwargs)
            if self.code_args_hash:
                self.outs_shared_cache[self.code_args_hash] = output

        out = output
        if self.output_index is not None:
            out = output[self.output_index]

        if isinstance(output, tuple) and len(output) != self.num_outs:
            raise WrongNumberOfOutsError

        for cache in self.caches:
            cache.put(out, self.hash)

        return out

    def __repr__(self):
        return f'<Future: {self.hash[:8]}>'

    def deny_access(self, *args, **kwargs):
        raise FutureAccessError


# Override all the operators of Future to raise a specific exception when used
for name in OPERATORS:
    setattr(Future, name, Future.deny_access)
