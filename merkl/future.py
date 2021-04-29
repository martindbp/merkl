import json
import hashlib
from collections import defaultdict
import merkl.cache
from merkl.io import write_track_file, write_future
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


"""
The defer function can be used from inside a task to make sure a function in called after any results have been persisted, e.g:

@task
def my_task():
    f = open('tempfile.txt', 'w')
    f.write('hello world\n')

    def _close_remove():
        f.close()
        os.remove('tempfile.txt')

    defer(_close_remove)
    return f  # contents will be read from `f` and cached

"""
DEFER = defaultdict(list)
def defer(f):
    global DEFER
    from merkl.task import next_invocation_id
    current_invocation_id = next_invocation_id - 1
    DEFER[current_invocation_id].append(f)


FUTURE_STATE_EXCLUDED = ['bound_args', 'fn', 'serializer']

class Future:
    __slots__ = [
        'fn', 'fn_name', 'fn_code_hash', 'outs', 'out_name', 'deps', '_cache', 'serializer', 'bound_args',
        'outs_shared_cache', '_hash', '_code_args_hash', 'meta', 'is_input', 'output_files', 'is_pipeline',
        'parent_pipeline_future', 'invocation_id', 'batch_idx',
    ]

    def __init__(
        self,
        fn=None,
        fn_code_hash=None,
        outs=1,
        out_name=None,
        deps=[],
        cache=None,
        serializer=None,
        bound_args=None,
        outs_shared_cache=None,
        hash=None,
        meta=None,
        is_input=False,
        output_files=None,
        is_pipeline=False,
        invocation_id=-1,
        batch_idx=None,
    ):
        self.fn = fn
        if fn and hasattr(fn, '__name__'):
            self.fn_name = fn.__name__
        self.fn_code_hash = fn_code_hash
        self.outs = outs
        self.out_name = out_name
        self.deps = deps
        self._cache = cache
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

        self.is_pipeline = is_pipeline
        self.parent_pipeline_future = None
        self.invocation_id = invocation_id
        self.batch_idx = batch_idx

    @property
    def caches(self):
        if isinstance(self._cache, list):
            return self._cache

        return [self._cache] if self._cache is not None else []

    @property
    def code_args_hash(self):
        if self._code_args_hash:
            return self._code_args_hash

        if not self.bound_args:
            return None

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
        global DEFER
        assert len(DEFER.get(self.invocation_id, [])) == 0

        specific_out = None
        use_batch_idx = False
        if self.code_args_hash and self.code_args_hash in self.outs_shared_cache:
            outputs = self.outs_shared_cache.get(self.code_args_hash)
            use_batch_idx = self.batch_idx is not None
        elif self.in_cache():
            outputs = self.get_cache()
        else:
            evaluated_args = nested_map(self.bound_args.args, map_future_to_value) if self.bound_args else []
            evaluated_kwargs = nested_map(self.bound_args.kwargs, map_future_to_value) if self.bound_args else {}
            outputs = self.fn(*evaluated_args, **evaluated_kwargs)
            if self.code_args_hash:
                self.outs_shared_cache[self.code_args_hash] = outputs
            use_batch_idx = self.batch_idx is not None

        if use_batch_idx:
            # If result is calculated in batch function, get the right index
            outputs = outputs[self.batch_idx]

        if isinstance(outputs, tuple) and len(outputs) != self.outs and self.outs != 1:
            raise TaskOutsError(f'Wrong number of outputs: {len(outputs)}. Expected {self.outs}')
        elif isinstance(outputs, dict) and len(outputs) != len(self.outs):
            raise TaskOutsError('Wrong number of outputs: {len(outputs)}. Expected {len(self.outs)}')

        if self.is_pipeline:
            # Set the pipeline function on all output futures
            for future in nested_collect(outputs, lambda x: isinstance(x, Future)):
                future.parent_pipeline_future = self

        specific_out = outputs
        if self.out_name is not None:
            specific_out = outputs[self.out_name]

        if not self.is_input:
            # Futures from io should not be cached (but is read from cache)
            if (len(self.caches) > 0 and not self.in_cache()) or len(self.output_files) > 0:
                specific_out_bytes = self.serializer.dumps(specific_out)

                for path in self.output_files:
                    write_track_file(path, specific_out_bytes, self.hash)

                for cache in self.caches:
                    if not cache.has(self.hash):
                        cache.add(self.hash, specific_out_bytes)

        for defer_fn in DEFER[self.invocation_id]:
            defer_fn()

        del DEFER[self.invocation_id]

        return specific_out

    def __repr__(self):
        return f'<Future: {self.hash[:8]}>'

    def __getstate__(self):
        # NOTE: a future is only pickled/serialized when it is returned by a pipeline 

        if len(self.caches) == 0:
            raise SerializationError(f'Serializing {repr(self)} ({self.fn}) but there is no cache set')

        # Trigger calculation of _hash and _code_args_hash that we want to serialize
        self.hash

        # When we pickle a Future (for returning from pipelines), we don't want to pickle the whole graph and
        # potentially large data, so exclude `bound_args` which may contain futures
        # Also, the function may not be pickleable, as well as the serializer
        state = {s: getattr(self, s, None) for s in self.__slots__ if s not in FUTURE_STATE_EXCLUDED}

        return state

    def __setstate__(self, d):
        for key, val in d.items():
            setattr(self, key, val)

        for key in FUTURE_STATE_EXCLUDED:
            setattr(self, key, None)

    def deny_access(self, *args, **kwargs):
        raise FutureAccessError

    def __or__(self, other):
        if not hasattr(other, 'is_merkl'):
            return NotImplemented

        return other(self)

    def __gt__(self, path):
        if not isinstance(path, str):
            return NotImplemented

        return write_future(self, path)


# Override all the operators of Future to raise a specific exception when used
for name in OPERATORS:
    setattr(Future, name, Future.deny_access)
