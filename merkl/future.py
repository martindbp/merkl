import json
import hashlib
import merkl.cache
from merkl.io import write_track_file
from merkl.utils import OPERATORS, nested_map, nested_collect
from merkl.exceptions import *


def map_to_hash(val):
    if isinstance(val, Future):
        return {'_hash': val.hash}
    elif not (isinstance(val, str) or isinstance(val, int) or isinstance(val, float)):
        raise SerializationError(f'Value {val} not serializable')
    return val


def map_future_to_value(val):
    if isinstance(val, Future):
        return val.eval()
    return val


class Future:
    __slots__ = [
        'fn', 'fn_code_hash', 'outs', 'out_name', 'deps', '_caches', 'serializer', 'bound_args',
        'outs_shared_cache', '_hash', '_code_args_hash', 'meta', 'is_input', 'output_files',
    ]

    def __init__(
        self,
        fn=None,
        fn_code_hash=None,
        outs=1,
        out_name=None,
        deps=[],
        caches=[],
        serializer=None,
        bound_args=None,
        outs_shared_cache=None,
        hash=None,
        meta=None,
        is_input=False,
        output_files=None,
    ):
        self.fn = fn
        self.fn_code_hash = fn_code_hash
        self.outs = outs
        self.out_name = out_name
        self.deps = deps
        self._caches = caches
        self.serializer = serializer
        self.bound_args = bound_args

        self._hash = hash
        self._code_args_hash = None

        # Cache for the all outputs with the respect to a function and its args
        self.outs_shared_cache = outs_shared_cache or {}
        self.meta = meta
        self.is_input = is_input
        self.output_files = output_files or []
        if len(self.output_files) > 0 and self.serializer is None:
            raise SerializationError(f'No serializer set for future {self}')

    @property
    def caches(self):
        if not self.is_input and merkl.cache.cache_override != 0:
            # Cannot override the cache of input or output futures
            return [merkl.cache.cache_override]
        return self._caches

    @property
    def code_args_hash(self):
        if not self.bound_args:
            return None

        if self._code_args_hash:
            return self._code_args_hash

        # Hash args, kwargs and code together
        hash_data = {
            'args': nested_map(self.bound_args.args, map_to_hash, convert_tuples_to_lists=True),
            'kwargs': nested_map(self.bound_args.kwargs, map_to_hash, convert_tuples_to_lists=True),
            'function_name': self.fn.__name__,
            'function_code_hash': self.fn_code_hash,
            'function_deps': self.deps,
        }
        m = hashlib.sha256()
        m.update(bytes(json.dumps(hash_data, sort_keys=True), 'utf-8'))
        self._code_args_hash = m.hexdigest()
        return self._code_args_hash

    @property
    def hash(self):
        if self._hash:
            return self._hash

        m = hashlib.sha256()
        m.update(bytes(self.code_args_hash, 'utf-8'))
        m.update(bytes(str(self.out_name), 'utf-8'))
        self._hash = m.hexdigest()
        return self._hash

    def parent_futures(self):
        if not self.bound_args:
            return []

        is_future = lambda x: isinstance(x, Future)
        return (
            nested_collect(self.bound_args.args, is_future) +
            nested_collect(self.bound_args.kwargs, is_future)
        )

    def in_cache(self):
        for cache in self.caches:
            if cache.has(self.hash):
                return True

        return False

    def get_cache(self):
        for cache in self.caches:
            if cache.has(self.hash):
                val = cache.get(self.hash)
                if self.is_input:
                    # reading from source file, not serialized
                    return val
                return self.serializer.loads(val)

    def eval(self):
        specific_out = None
        if self.code_args_hash and self.code_args_hash in self.outs_shared_cache:
            outputs = self.outs_shared_cache.get(self.code_args_hash)
        elif self.in_cache():
            outputs = self.get_cache()
        else:
            evaluated_args = nested_map(self.bound_args.args, map_future_to_value) if self.bound_args else []
            evaluated_kwargs = nested_map(self.bound_args.kwargs, map_future_to_value) if self.bound_args else {}
            outputs = self.fn(*evaluated_args, **evaluated_kwargs)
            if self.code_args_hash:
                self.outs_shared_cache[self.code_args_hash] = outputs

        if isinstance(outputs, tuple) and len(outputs) != self.outs:
            raise TaskOutsError('Wrong number of outputs: {len(outputs)}. Expected {self.outs}')
        elif isinstance(outputs, dict) and len(outputs) != len(self.outs):
            raise TaskOutsError('Wrong number of outputs: {len(outputs)}. Expected {len(self.outs)}')

        specific_out = outputs
        if self.out_name is not None:
            specific_out = outputs[self.out_name]

        if not self.is_input:
            # Futures from io should not be cached (but is read from cache)
            if len(self.caches) > 0 or len(self.output_files) > 0:
                specific_out_bytes = self.serializer.dumps(specific_out)
                m = hashlib.sha256()
                m.update(specific_out_bytes)
                content_hash = m.hexdigest()

                for path, track in self.output_files:
                    write_track_file(path, specific_out_bytes, content_hash, track)

                for cache in self.caches:
                    if not cache.has(self.hash):
                        cache.add(content_hash, self.hash, specific_out_bytes)

        return specific_out

    def __repr__(self):
        return f'<Future: {self.hash[:8]}>'

    def deny_access(self, *args, **kwargs):
        raise FutureAccessError


# Override all the operators of Future to raise a specific exception when used
for name in OPERATORS:
    setattr(Future, name, Future.deny_access)
