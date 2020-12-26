import sys
import json
import hashlib
import textwrap
from enum import Enum
from warnings import warn
from functools import wraps, lru_cache
from inspect import signature, getsource, isfunction, ismodule, getmodule
from merkl.serializers import PickleSerializer
from merkl.utils import doublewrap, OPERATORS, nested_map, get_function_return_type_length
from merkl.exceptions import *

getsource_cached = lru_cache()(getsource)

class HashMode(Enum):
    MODULE = 1
    FUNCTION = 2


def map_merkl_future_to_value(val):
    if isinstance(val, MerkLFuture):
        return val.get()
    return val


def map_merkl_future_to_hash(val):
    if isinstance(val, MerkLFuture):
        return {'merkl_hash': val.hash}
    elif not (isinstance(val, str) or isinstance(val, int) or isinstance(val, float)):
        raise NonSerializableArgError
    return val


def validate_outs(outs, sig=None):
    if isinstance(outs, int):
        if outs <= 0:
            raise NonPositiveOutsError
    elif callable(outs) and sig is not None:
        outs_sig = signature(outs)
        if outs_sig != sig:
            raise NonMatchingSignaturesError
    else:
        raise BadOutsValueError


class MerkLFuture:
    def __init__(
        self,
        fn,
        output_hash,
        num_outs=1,
        code_args_hash=None,
        output_index=None,
        serializer=None,
        cache_policy=None,
        bound_args=None,
        outs_result_cache=None,
    ):
        self.fn = fn
        self.hash = output_hash
        self.num_outs = num_outs
        self.code_args_hash = code_args_hash
        self.output_index = output_index
        self.serializer = serializer
        self.cache_policy = cache_policy
        self.bound_args = bound_args

        # Cache for the all outputs with the respect to a function and its args
        self.outs_result_cache = outs_result_cache or {}

    def get(self):
        evaluated_args = nested_map(self.bound_args.args, map_merkl_future_to_value) if self.bound_args else []
        evaluated_kwargs = nested_map(self.bound_args.kwargs, map_merkl_future_to_value) if self.bound_args else {}

        if self.code_args_hash in self.outs_result_cache:
            output = self.outs_result_cache.get(self.code_args_hash)
        else:
            output = self.fn(*evaluated_args, **evaluated_kwargs)
            self.outs_result_cache[self.code_args_hash] = output

        if self.output_index is not None:
            return output[self.output_index]

        if isinstance(output, tuple) and len(output) != self.num_outs:
            raise WrongNumberOfOutsError

        return output

    def deny_access(self, *args, **kwargs):
        raise FutureAccessError


# Override all the operators of MerkLFuture to raise a specific exception when used
for name in OPERATORS:
    setattr(MerkLFuture, name, MerkLFuture.deny_access)


@doublewrap
def task(f, outs=None, hash_mode=HashMode.MODULE, deps=[], out_serializers={}, out_cache_policy={}):
    sig = signature(f)

    if outs is not None:
        validate_outs(outs, sig)
    else:
        # Get num outs from AST if possible
        return_types, num_returns = get_function_return_type_length(f)
        if len(return_types) != 1 and 'Tuple' in return_types:
            raise ReturnTypeMismatchError
        elif len(num_returns) != 1:
            raise NumReturnValuesMismatchError

        outs = num_returns.pop()

    # Validate and resolve deps
    for i in range(len(deps)):
        dep = deps[i]
        if isfunction(dep) or ismodule(dep):
            deps[i] = textwrap.dedent(getsource_cached(dep))
        elif isinstance(dep, bytes):
            deps[i] = dep.decode('utf-8')
        elif not isinstance(dep, str):
            raise NonSerializableFunctionDepError

    code_obj = getmodule(f) if hash_mode == HashMode.MODULE else f
    code = textwrap.dedent(getsource_cached(code_obj))

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
            'function_deps': deps,
        }
        m = hashlib.sha256()
        m.update(bytes(json.dumps(hash_data, sort_keys=True), 'utf-8'))
        code_args_hash = m.hexdigest()

        resolved_outs = outs(*args, **kwargs) if callable(outs) else outs
        validate_outs(resolved_outs)

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
            output = MerkLFuture(
                f,
                output_hash,
                resolved_outs,
                code_args_hash,
                i if resolved_outs > 1 else None,
                serializer,
                cache_policy,
                bound_args,
                outs_result_cache,
            )
            outputs.append(output)

        if resolved_outs == 1:
            return outputs[0]
        return outputs

    return wrap
