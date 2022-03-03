import os
import json
import hashlib
from collections import defaultdict
from functools import cached_property, partial

import dill

import merkl.cache
from merkl.exceptions import *
from merkl.cache import get_modified_time, MEMORY_CACHE
from merkl.logger import logger, log_if_slow, short_hash
from merkl.io import write_track_file, write_future, FileRef, DirRef, get_merkl_file_hash
from merkl.utils import OPERATORS, nested_map, nested_collect, function_descriptive_name, DelayedKeyboardInterrupt, Eval

def map_to_hash(val):
    if isinstance(val, Future):
        return {'_hash': val.hash}
    return val


def map_future_to_value(val):
    if isinstance(val, Future):
        return val.eval()
    return val


def to_bytes_maybe(bytes_or_str):
    if isinstance(bytes_or_str, str):
        return bytes(bytes_or_str, 'utf-8')
    return bytes_or_str


def _code_args_serializer_default(obj, fn_name):
    logger.debug(f'Argument of type {type(obj)} for function {fn_name} is not JSON serializable, serializing with dill instead')
    return str(dill.dumps(obj))


FUTURE_STATE_EXCLUDED = ['bound_args', '_fn', 'single_fn', 'outs_shared_futures', '_parent_futures', '_val', 'on_completed']

deps_hash_cache = {}

class Future:
    __slots__ = [
        '_fn', 'single_fn', 'fn_code_hash', 'outs', 'out_name', 'deps', 'cache', 'serializer', 'bound_args',
        'outs_shared_cache', '_hash', '_deps_args_hash', '_deps_hash', '_args_hash', 'meta', 'is_input', 'output_files', 'is_pipeline',
        'parent_pipeline_future', 'invocation_id', 'task_id', 'batch_idx', 'cache_temporarily', 'outs_shared_futures',
        '_parent_futures', 'cache_in_memory', 'ignore_args', 'on_completed', '_val', '_fn_descriptive_name',
    ]

    def __init__(
        self,
        fn=None,
        fn_code_hash=None,
        outs=1,
        out_name=None,
        deps=None,
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
        task_id=-1,
        batch_idx=None,
        cache_temporarily=False,
        cache_in_memory=False,
        ignore_args=None,
        single_fn=None,
    ):
        self._fn = fn
        self.single_fn = single_fn
        self.fn_code_hash = fn_code_hash
        self.outs = outs
        self.out_name = out_name
        self.deps = deps
        self.cache = cache
        self.serializer = serializer
        self.bound_args = bound_args

        self._hash = hash
        self._deps_args_hash = None
        self._args_hash = None
        self._deps_hash = None

        # Cache for the all outputs with the respect to a function and its args
        self.outs_shared_cache = outs_shared_cache if outs_shared_cache is not None else {}
        self.meta = meta
        self.is_input = is_input
        self.output_files = output_files
        if output_files is not None and len(output_files) > 0 and serializer is None:
            raise SerializationError(f'No serializer set for future {self}')

        self.is_pipeline = is_pipeline
        self.parent_pipeline_future = None
        self.invocation_id = invocation_id
        self.task_id = task_id
        self.batch_idx = batch_idx
        self.cache_temporarily = cache_temporarily
        self.cache_in_memory = cache_in_memory
        self.ignore_args = ignore_args
        self.outs_shared_futures = None
        self.on_completed = None
        self._parent_futures = None
        self._val = None
        self._fn_descriptive_name = None

    @property
    def fn(self):
        # If self.single_fn is set, then self._fn is the batch version.
        # Sometimes we want to access the batch one, but by default we use the
        # single function
        return self.single_fn or self._fn

    @property
    def deps_hash(self):
        if self._deps_hash:
            return self._deps_hash

        # We cannot cache the deps with only the function as key, because the function could have been
        # wrapped by @task multiple times
        fn_task_key = f'{self._fn}-{self.task_id}'
        if fn_task_key in deps_hash_cache:
            self._deps_hash = deps_hash_cache[fn_task_key]
            return self._deps_hash

        default = partial(_code_args_serializer_default, fn_name=self.fn_descriptive_name)

        hash_data = {
            'function_deps': self.deps or [],
        }

        logger.info(f'Hashing deps for {self.fn_descriptive_name}')
        m = hashlib.sha256()
        try:
            m.update(bytes(json.dumps(hash_data, sort_keys=True, default=default), 'utf-8'))
        except (TypeError, dill.PicklingError):
            raise SerializationError(f'Value in args {hash_data} not JSON or dill-serializable')
        self._deps_hash = m.hexdigest()
        deps_hash_cache[fn_task_key] = self._deps_hash
        return self._deps_hash

    @property
    def args_hash(self):
        if self._args_hash:
            return self._args_hash

        if not self.bound_args:
            return None

        default = partial(_code_args_serializer_default, fn_name=self.fn_descriptive_name)

        arguments = self.bound_args.arguments
        if self.ignore_args is not None:
            arguments = {name: arg for name, arg in arguments.items() if name not in self.ignore_args}

        hash_data = {
            'arguments': nested_map(arguments, map_to_hash, convert_tuples_to_lists=True),
        }

        m = hashlib.sha256()
        try:
            m.update(bytes(json.dumps(hash_data, sort_keys=True, default=default), 'utf-8'))
        except (TypeError, dill.PicklingError):
            raise SerializationError(f'Value in args {hash_data} not JSON or dill-serializable')
        self._args_hash = m.hexdigest()
        return self._args_hash

    @property
    def deps_args_hash(self):
        if self._deps_args_hash:
            return self._deps_args_hash

        if not self.bound_args:
            return None

        m = hashlib.sha256()
        m.update(bytes(self.deps_hash, 'utf-8'))
        m.update(bytes(self.args_hash, 'utf-8'))
        self._deps_args_hash = m.hexdigest()
        return self._deps_args_hash

    @property
    def hash(self):
        if self._hash:
            return self._hash

        m = hashlib.sha256()
        m.update(bytes(self.deps_args_hash, 'utf-8'))
        m.update(bytes(str(self.out_name), 'utf-8'))
        self._hash = m.hexdigest()
        return self._hash

    @property
    def parent_futures(self):
        if self._parent_futures is not None:
            return self._parent_futures

        if not self.bound_args:
            self._parent_futures = []
            return []

        is_future = lambda x: isinstance(x, Future)
        if self.batch_idx is not None:
            self._parent_futures = nested_collect(self.bound_args.args[0][self.batch_idx], is_future)
        else:
            self._parent_futures = (
                nested_collect(self.bound_args.args, is_future) +
                nested_collect(self.bound_args.kwargs, is_future)
            )
        return self._parent_futures

    def in_cache(self):
        # First check the in-memory cache
        if self.cache_in_memory and self.hash in MEMORY_CACHE:
            return True

        for output_file, write_merkl_file in self.output_files or []:
            if not write_merkl_file:
                continue

            try:
                output_file_merkl_hash = get_merkl_file_hash(output_file)
            except TypeError:
                logger.warning(f'Unable to deserialize .merkl file for {output_file}')
                output_file_merkl_hash = None

            if output_file_merkl_hash == self.hash:
                return True

        if self.cache is None:
            return False

        return self.cache.has(self.hash)

    def get_cache(self):
        # First check the in-memory cache
        if self.cache_in_memory and self.hash in MEMORY_CACHE:
            val = MEMORY_CACHE[self.hash]
            # We don't want to store both unserialized and serialized value in the memory cache, so we need to serialize
            # it if we need the serialized data. We only need this if we have to write it to file
            serialized = to_bytes_maybe(self.serializer.dumps(val)) if self.output_files is not None and len(self.output_files) > 0 else None
            return val, serialized

        if self.cache is None:
            return None, None

        if self.cache.has(self.hash):
            val = self.cache.get(self.hash)
            if self.is_input:
                # reading from source file, not serialized
                return val, val

            deserialized = log_if_slow(lambda: self.serializer.loads(val), f'Deserializing {self.fn_descriptive_name} out {self.hash} slow')
            return deserialized, val

        # Not in regular cache, so check the output files:
        for output_file, write_merkl_file in self.output_files or []:
            if not write_merkl_file:
                continue

            output_file_merkl_hash = get_merkl_file_hash(output_file)
            if output_file_merkl_hash == self.hash:
                with open(output_file, 'rb') as f:
                    val = f.read()

                try:
                    deserialized = log_if_slow(lambda: self.serializer.loads(val), f'Deserializing {self.fn_descriptive_name} out {self.hash} slow')
                except:
                    logger.error(f'Unable to deserialize {output_file}')
                    raise

                return deserialized, val

    def clear_cache(self, delete_output_files=False):
        self._val = None

        if self.cache_in_memory and self.hash in MEMORY_CACHE:
            del MEMORY_CACHE[self.hash]

        if self.cache is None:
            return

        self.cache.clear(self.hash)

        if delete_output_files:
            for output_file, write_merkl_file in self.output_files or []:
                if os.path.exists(output_file):
                    os.remove(output_file)
                merkl_file = output_file + '.merkl'
                if write_merkl_file and os.path.exists(merkl_file):
                    os.remove(merkl_file)

    def write_output_files(self, specific_out, specific_out_bytes):
        for path, write_merkl_file in self.output_files or []:
            # Check if output file is up to date
            modified = get_modified_time(path)
            up_to_date = False
            if self.cache is not None:
                md5_hash, merkl_hash, modified_time = self.cache.get_latest_file(path)
                up_to_date = modified_time == modified and merkl_hash == self.hash

            # If not up to date, serialize and write the new file
            if not up_to_date:
                if specific_out_bytes is None:
                    specific_out_bytes = log_if_slow(lambda: to_bytes_maybe(self.serializer.dumps(specific_out)), f'Serializing {self.fn_descriptive_name} out {self.hash} slow')

                write_track_file(path, specific_out_bytes, self, self.cache, write_merkl_file)

    def eval(self):
        if self._val is not None:
            return self._val

        if self.in_cache():
            if isinstance(self.out_name, int):
                if self.out_name <= 5:
                    logger.debug(f'{self.fn_descriptive_name}:{self.out_name} ({short_hash(self.hash)}) output was cached')
                    if self.out_name == 5:
                        logger.debug(f'And {self.outs - self.out_name} more...')

            specific_out, specific_out_bytes = self.get_cache()
            self.write_output_files(specific_out, specific_out_bytes)
        else:
            specific_out, specific_out_bytes = self._eval()

        if self.cache_in_memory:
            MEMORY_CACHE[self.hash] = specific_out

        self._val = specific_out

        if self.on_completed is not None:
            self.on_completed()

        return specific_out

    def _eval(self):
        specific_out = None
        specific_out_is_ref = False
        outputs = None
        called_function = False
        if self.deps_args_hash and self.deps_args_hash in self.outs_shared_cache:
            outputs = self.outs_shared_cache.get(self.deps_args_hash)
        else:
            evaluated_args = nested_map(self.bound_args.args, map_future_to_value) if self.bound_args else []
            evaluated_kwargs = nested_map(self.bound_args.kwargs, map_future_to_value) if self.bound_args else {}
            logger.debug(f'Calling {self.fn_descriptive_name} (out_name={self.out_name})')
            called_function = True

            # In case an Eval manager was used, we need to reset it so that any calls inside `fn` are not also
            # evaled immediately
            with Eval(False):
                outputs = self._fn(*evaluated_args, **evaluated_kwargs)

            if self.deps_args_hash:
                self.outs_shared_cache[self.deps_args_hash] = outputs

                if self.cache:
                    self.cache.no_commit = True  # for efficiency, commit only after all futures have been cached

                for future in self.outs_shared_futures or []:
                    if future.hash == self.hash:
                        continue
                    future._eval()

                if self.cache:
                    self.cache.no_commit = False
                    self.cache.commit()

        if isinstance(outputs, tuple) and len(outputs) != self.outs and self.outs != 1:
            raise TaskOutsError(f'Wrong number of outputs: {len(outputs)}. Expected {self.outs}')
        elif isinstance(outputs, dict) and self.outs != 1:
            if isinstance(self.outs, int):
                raise TaskOutsError(f'Outs was int: {self.outs}, but not 1, but output is dict')
            elif len(outputs) != len(self.outs):
                raise TaskOutsError(f'Wrong number of outputs: {len(outputs)}. Expected {self.outs}')

        if self.batch_idx is not None:
            outputs = outputs[self.batch_idx]
        if self.out_name is not None:
            outputs = outputs[self.out_name]

        specific_out = outputs
        deep_refs = nested_collect(
            specific_out,
            lambda out, lvl: (isinstance(out, FileRef) or isinstance(out, DirRef)) and lvl >= 1,
            include_level=True,
        )
        if len(deep_refs) > 0:
            raise ValueError('FileRef and DirRef cannot be deeply nested in a task out')

        if self.cache is not None:
            if isinstance(specific_out, FileRef) or isinstance(specific_out, DirRef):
                specific_out = self.cache.transfer_ref(specific_out, self.hash)
                specific_out_is_ref = True

        if self.is_pipeline:
            # Set the pipeline function on all output futures
            for future in nested_collect(outputs, lambda x: isinstance(x, Future)):
                future.parent_pipeline_future = self

        specific_out_bytes = None
        if not self.is_input:  # Futures from io should not be cached (but is read from cache)
            if self.cache is not None:
                specific_out_bytes = to_bytes_maybe(self.serializer.dumps(specific_out))

                if not self.cache.has(self.hash):  # gotta check again
                    with DelayedKeyboardInterrupt():
                        # Cache needs to be fully done, otherwise we might have added data to sqlite but not file
                        ref = (specific_out if specific_out_is_ref else None)
                        logger.debug(f'Caching {self.fn_descriptive_name} {short_hash(self.hash)} ref={ref}, len(content_bytes)={len(specific_out_bytes)}')
                        self.cache.add(
                            self.hash,
                            specific_out_bytes,
                            ref=ref,
                            fn_name=self.fn_descriptive_name,
                        )

                if called_function:  # Make sure we only clear parent futures once for all the output futures
                    for parent_future in self.parent_futures:
                        if parent_future.cache_temporarily:
                            parent_future.clear_cache()

            self.write_output_files(specific_out, specific_out_bytes)

        return specific_out, specific_out_bytes

    @property
    def fn_descriptive_name(self):
        if not hasattr(self, '_fn_descriptive_name') or self._fn_descriptive_name is None:
            self._fn_descriptive_name = function_descriptive_name(self.fn)
        return self._fn_descriptive_name

    def __repr__(self):
        return f'<Future: {self.hash[:8]}>'

    def __getstate__(self):
        # NOTE: a future is only pickled/serialized when it is returned by a pipeline, or when tracked in a file

        if self.cache is None:
            raise SerializationError(f'Serializing {repr(self)} ({self.fn_descriptive_name}: {self.out_name}) but there is no cache set')

        # Trigger calculation of _hash and _code_args_hash that we want to serialize
        self.hash

        # When we pickle a Future (for returning from pipelines), we don't want to pickle the whole graph and
        # potentially large data, so exclude e.g. `bound_args` which may contain futures
        # Also, the function may not be pickleable, and we don't need it when loading a cached Future
        state = {s: getattr(self, s, None) for s in self.__slots__ if s not in FUTURE_STATE_EXCLUDED}

        return state

    def __setstate__(self, d):
        for key, val in d.items():
            setattr(self, key, val)

        for key in FUTURE_STATE_EXCLUDED:
            setattr(self, key, None)

    def __or__(self, other):
        if not hasattr(other, 'is_merkl'):
            return NotImplemented

        return other(self)

    def __gt__(self, path):
        if not isinstance(path, str):
            return NotImplemented

        return write_future(self, path)

    def __rshift__(self, path):
        if not isinstance(path, str):
            return NotImplemented

        return write_future(self, path, write_merkl_file=True)

    def __hash__(self):
        return int(self.hash, 16)

    def __eq__(self, other):
        if not hasattr(other, 'hash'):
            return False

        return self.hash == other.hash

    def deny_access(self, *args, **kwargs):
        raise FutureAccessError

    @classmethod
    def from_file(cls, path, raise_file_not_found=True, replace_output_files=True):
        if not os.path.exists(path + '.merkl'):
            if raise_file_not_found:
                raise FileNotFoundError(path + '.merkl')
            else:
                return None

        with open(path + '.merkl', 'rb') as f:
            future = dill.load(f)
            if replace_output_files:
                future.output_files = [(path, True)]
            return future

    @property
    def v(self):
        # Short hand for getting the value
        return self.eval()


# Override all the operators of Future to raise a specific exception when used
for name in OPERATORS:
    setattr(Future, name, Future.deny_access)
