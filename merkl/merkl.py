import sys
import json
import hashlib
from enum import Enum
from warnings import warn
from inspect import signature, getsource
from functools import wraps
from .serializers import PickleSerializer
from .utils import doublewrap, OPERATORS, nested_map

# Cache of code read from function files
CODE_CACHE = {}

class HashMode(Enum):
    FILE = 1
    FUNCTION = 2

def map_merkl_future_to_value(val):
    if isinstance(val, MerklFuture):
        return val.get()
    return val


def map_merkl_future_to_hash(val):
    if isinstance(val, MerklFuture):
        return {'merkl_hash': val.hash}
    elif not (isinstance(val, str) or isinstance(val, int) or isinstance(val, float)):
        print(f'WARNING: input arg to function: {str(val)} is neither str, int or float', file=sys.stderr)
    return val


class MerklFuture:
    class MerklFutureAccessException(BaseException):
        pass

    def __init__(
        self,
        fn,
        outs_was_none,
        code_args_hash,
        output_index,
        output_hash,
        serializer,
        cache_policy,
        sig,
        bound_args,
        outs_result_cache,
    ):
        self.fn = fn
        self.outs_was_none = outs_was_none
        self.code_args_hash = code_args_hash
        self.output_index = output_index
        self.hash = output_hash
        self.serializer = serializer
        self.cache_policy = cache_policy
        self.sig = sig
        self.bound_args = bound_args

        # Cache for the all outputs with the respect to a function and its args
        self.outs_result_cache = outs_result_cache

    def get(self):
        evaluated_args = nested_map(self.bound_args.args, map_merkl_future_to_value)
        evaluated_kwargs = nested_map(self.bound_args.kwargs, map_merkl_future_to_value)

        if self.code_args_hash in self.outs_result_cache:
            output = self.outs_result_cache.get(self.code_args_hash)
        else:
            output = self.fn(*evaluated_args, **evaluated_kwargs)
            self.outs_result_cache[self.code_args_hash] = output

        if self.output_index is not None:
            return output[self.output_index]

        if isinstance(output, tuple) and self.outs_was_none:
            print(
                (
                    f'WARNING: Output of function `{self.fn.__name__}` is a tuple, but function only has one out by default. '
                    f'To remove warning, set number of outs explicitly for function ({self.fn.__code__.co_filename}). Example:\n'
                    '\t@node(outs=1)\n'
                    f'\tdef {self.fn.__name__}(*args, **kwargs):\n'
                    '\t\tpass'
                ),
                file=sys.stderr
            )

        return output

    def deny_access(self, *args, **kwargs):
        raise self.MerklFutureAccessException

# Override all the operators of MerklFuture to raise a specific exception when used
for name in OPERATORS:
    setattr(MerklFuture, name, MerklFuture.deny_access)


@doublewrap
def node(f, outs=None, hash_mode=HashMode.FILE, out_serializers={}, out_cache_policy={}):
    sig = signature(f)
    if callable(outs):
        outs_sig = signature(outs)
        if outs_sig != sig:
            raise Exception(f'`outs` signature {outs_sig} differs from function signature {sig}')

    f_filename = f.__code__.co_filename
    if hash_mode == HashMode.FILE:
        code = CODE_CACHE.get(f_filename)
        if code is None:
            with open(f_filename, 'r') as code_file:
                code = code_file.read()
    else:
        #TODO: remove identation
        code = getsource(f)

    @wraps(f)
    def wrap(*args, **kwargs):
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()

        # Hash args, kwargs and code together
        hash_data = {
            'args': nested_map(bound_args.args, map_merkl_future_to_hash, convert_tuples_to_lists=True),
            'kwargs': nested_map(bound_args.kwargs, map_merkl_future_to_hash, convert_tuples_to_lists=True),
            'function_name': f.__name__,
            'function_code': code,
        }
        m = hashlib.sha256()
        m.update(bytes(json.dumps(hash_data, sort_keys=True), 'utf-8'))
        code_args_hash = m.hexdigest()

        resolved_outs = outs
        outs_was_none = False
        if callable(outs):
            resolved_outs = outs(*args, **kwargs)

        if isinstance(resolved_outs, int):
            if resolved_outs <= 0:
                raise Exception('Number of outs has to be greater than zero')
        elif resolved_outs == None:
            resolved_outs = 1
            outs_was_none = True
        else:
            raise Exception('`outs` has to be resolved to an integer or None')

        outputs = []
        outs_result_cache = {}
        for i in range(resolved_outs):
            m = hashlib.sha256()
            m.update(bytes(code_args_hash, 'utf-8'))
            if resolved_outs > 1:
                m.update(bytes(str(i), 'utf-8'))
            output_hash = m.hexdigest()
            serializer = out_serializers.get(i, PickleSerializer)
            cache_policy = out_cache_policy.get(i, None)
            output = MerklFuture(
                f,
                outs_was_none,
                code_args_hash,
                i if resolved_outs > 1 else None,
                output_hash,
                serializer,
                cache_policy,
                sig,
                bound_args,
                outs_result_cache,
            )
            outputs.append(output)

        if resolved_outs == 1:
            return outputs[0]
        return outputs

    return wrap
