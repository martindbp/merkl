import json
import hashlib
import textwrap
from enum import Enum
from functools import wraps, lru_cache
from inspect import signature, getsource, isfunction, ismodule, getmodule
from merkl.serializers import PickleSerializer
from merkl.utils import doublewrap, nested_map, get_function_return_type_length
from merkl.future import Future
from merkl.io import map_to_hash, TrackedPath
from merkl.exceptions import *

getsource_cached = lru_cache()(getsource)

class HashMode(Enum):
    MODULE = 1
    FUNCTION = 2


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


@doublewrap
def task(f, outs=None, hash_mode=HashMode.FUNCTION, deps=[], caches=[], serializer=None):
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
        elif isinstance(dep, TrackedPath):
            deps[i] = dep.hash
        elif not isinstance(dep, str):
            raise NonSerializableFunctionDepError

    if not isinstance(hash_mode, HashMode):
        raise TypeError

    code_obj = getmodule(f) if hash_mode == HashMode.MODULE else f
    code = textwrap.dedent(getsource_cached(code_obj))

    @wraps(f)
    def wrap(*args, **kwargs):
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()

        # Hash args, kwargs and code together
        hash_data = {
            'args': nested_map(bound_args.args, map_to_hash, convert_tuples_to_lists=True),
            'kwargs': nested_map(bound_args.kwargs, map_to_hash, convert_tuples_to_lists=True),
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
        outs_shared_cache = {}
        for i in range(resolved_outs):
            m = hashlib.sha256()
            m.update(bytes(code_args_hash, 'utf-8'))
            if resolved_outs > 1:
                m.update(bytes(str(i), 'utf-8'))
            output_hash = m.hexdigest()

            if serializer is None:
                out_serializer = PickleSerializer
            elif isinstance(serializer, dict):
                out_serializer = serializer[i]

            output = Future(
                f,
                output_hash,
                resolved_outs,
                code_args_hash,
                i if resolved_outs > 1 else None,
                caches,
                out_serializer,
                bound_args,
                outs_shared_cache,
            )
            outputs.append(output)

        if resolved_outs == 1:
            return outputs[0]
        return outputs

    return wrap
