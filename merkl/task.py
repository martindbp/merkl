import hashlib
import textwrap
import pickle
from enum import Enum
from functools import wraps, lru_cache
from inspect import signature, getsource, isfunction, ismodule, getmodule
from merkl.utils import doublewrap, nested_map, get_function_return_info
from merkl.future import Future
from merkl.exceptions import *


class HashMode(Enum):
    MODULE = 1
    FUNCTION = 2


@lru_cache(maxsize=None)
def code_hash(f, is_module=False):
    code_obj = getmodule(f) if is_module else f
    code = textwrap.dedent(getsource(code_obj))
    m = hashlib.sha256()
    m.update(bytes(code, 'utf-8'))
    return m.hexdigest()


def validate_outs(outs, sig=None, return_type=None):
    if isinstance(outs, int):
        if outs <= 0:
            raise NonPositiveOutsError
    elif callable(outs) and sig is not None:
        outs_sig = signature(outs)
        if outs_sig.parameters.keys() != sig.parameters.keys():
            raise NonMatchingSignaturesError
    elif return_type != 'Dict':
        raise BadOutsValueError


@doublewrap
def task(f, outs=None, hash_mode=HashMode.FUNCTION, deps=None, caches=None, serializer=None):
    deps = deps or []
    caches = caches or []
    sig = signature(f)

    return_type = None
    if outs is not None:
        validate_outs(outs, sig)
    else:
        # Get num outs from AST if possible
        return_types, num_returns = get_function_return_info(f)
        if len(return_types) != 1 and 'Tuple' in return_types:
            raise ReturnTypeMismatchError
        elif len(num_returns) != 1:
            raise NumReturnValuesMismatchError

        outs = num_returns.pop()
        return_type = return_types.pop()

    # Validate and resolve deps
    for i in range(len(deps)):
        dep = deps[i]
        if isfunction(dep):
            deps[i] = f'<Function "{dep.__name__}: {code_hash(dep)}>'
        elif ismodule(dep):
            deps[i] = f'<Module "{dep.__name__}: {code_hash(dep, is_module=True)}>'
        elif isinstance(dep, bytes):
            deps[i] = dep.decode('utf-8')
        elif not isinstance(dep, str):
            raise NonSerializableFunctionDepError

    if not isinstance(hash_mode, HashMode):
        raise TypeError

    fn_code_hash = code_hash(f, hash_mode == HashMode.MODULE)

    @wraps(f)
    def wrap(*args, **kwargs):
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()

        resolved_outs = outs(*args, **kwargs) if callable(outs) else outs
        validate_outs(resolved_outs, return_type=return_type)

        enumerated_outs = resolved_outs if isinstance(resolved_outs, tuple) else range(resolved_outs)

        outputs = {}
        outs_shared_cache = {}
        is_single = resolved_outs == 1 and return_type not in ['Tuple', 'Dict']
        for out_name in enumerated_outs:
            if serializer is None:
                out_serializer = pickle
            elif isinstance(serializer, dict):
                out_serializer = serializer[out_name]

            output = Future(
                f,
                fn_code_hash,
                resolved_outs,
                None if is_single else out_name,
                deps,
                caches,
                out_serializer,
                bound_args,
                outs_shared_cache,
            )
            outputs[out_name] = output

        if is_single:
            return outputs[0]
        return outputs if return_type == 'Dict' else list(outputs.values())

    return wrap
