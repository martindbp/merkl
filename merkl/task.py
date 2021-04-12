import json
import hashlib
import textwrap
import pickle
from enum import Enum
from functools import lru_cache
from sigtools.specifiers import forwards_to_function
from inspect import signature, getsource, isfunction, ismodule, getmodule
from merkl.utils import doublewrap, nested_map, get_function_return_info, find_function_deps, FunctionDep
from merkl.future import Future
from merkl.exceptions import *

next_invocation_id = 0

class HashMode(Enum):
    MODULE = 1
    FUNCTION = 2
    FIND_DEPS = 3


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
            raise TaskOutsError(f'Outs: {outs} is not >= 1')
    elif callable(outs) and sig is not None:
        outs_sig = signature(outs)
        if outs_sig.parameters.keys() != sig.parameters.keys():
            raise TaskOutsError(f'Outs function signature does not match task: {outs_sig} vs {sig}')
    elif return_type != 'Dict':
        raise TaskOutsError(f'Bad outs type: {outs}')


def validate_resolve_deps(deps):
    # Collect deps for tasks
    extra_deps = []
    for dep in deps:
        if isfunction(dep) and hasattr(dep, 'is_merkl') and dep.type == 'task':
            extra_deps += dep.deps

    for i in range(len(deps)):
        name = None
        dep = deps[i]
        if isinstance(dep, FunctionDep):
            name = dep.name
            dep = dep.value

        if isfunction(dep):
            if hasattr(dep, 'is_merkl') and dep.type == 'task':
                dep = dep.__wrapped__
            deps[i] = f'<Function {dep.__name__}: {code_hash(dep)}>'
        elif ismodule(dep):
            deps[i] = f'<Module {dep.__name__}: {code_hash(dep, is_module=True)}>'
        elif isinstance(dep, bytes):
            deps[i] = dep.decode('utf-8')
        elif not isinstance(dep, str):
            try:
                deps[i] = json.dumps(dep)
            except TypeError:
                raise SerializationError(f'Task dependency {dep} not serializable to JSON')
        if name is not None:
            deps[i] = (name, deps[i])


@doublewrap
def batch(batch_fn, single_fn=None, hash_mode=HashMode.FIND_DEPS, deps=None, caches=None, serializer=None):
    if single_fn:
        if not hasattr(single_fn, 'is_merkl') or single_fn.type != 'task':
            raise BatchTaskError(f'Function {single_fn} is not decorated as a task')

        if not isinstance(single_fn.outs, int) or single_fn.outs != 1:
            raise BatchTaskError(f'Function {single_fn} must have exactly one out')

    if not isinstance(hash_mode, HashMode):
        raise TypeError(f'Unexpected HashMode value {hash_mode} for function {f}')

    batch_fn_code_hash = code_hash(batch_fn, True)

    @forwards_to_function(batch_fn)
    def wrap(args):
        global next_invocation_id

        if callable(args):
            raise BatchTaskError(f'Function {args} is not decorated as a task')
        elif not isinstance(args, list):
            raise BatchTaskError(f'Batch args {args} is not a list')

        # Validate that args is a list of tuples, or single_fn has single input

        use_single_fn = single_fn
        if single_fn is None:
            # Create a single-out version of the batch function. This will be used to create the future, but
            # `batch_fn` will be used to do the actual calculation
            use_single_fn = task(1, hash_mode, deps, caches, serializer)(batch_fn)

        outs = []
        outs_shared_cache = {}
        invocation_id = next_invocation_id
        for i, args_tuple in enumerate(args):
            if not isinstance(args_tuple, tuple):
                # If single_fn has multiple parameters, then `args_tuple` has to be a tuple
                if single_fn and len(signature(single_fn).parameters.keys()) != 1:
                    raise BatchTaskError(f'Batch arg {args_tuple} is not a tuple')
                else:
                    args_tuple = (args_tuple,)

            out = use_single_fn(*args_tuple)
            out.is_batch = True
            # Set the invocation id to the same for all futures
            out.invocation_id = invocation_id
            # Trigger calculation of the hash, which will be cached
            out.hash
            # Swap out the function for the batch version
            out.fn = batch_fn
            # Swap out code_args_hash to the batch_fn code hash, so
            # that the the value can be taken out of the shared cache
            # NOTE: this doesn't need to have deps and stuff, because it's not used for persistent caching, just to
            # store the output temporarily
            out._code_args_hash = batch_fn_code_hash
            # Need to set the shared cache to be shared across all batch invocations
            out.outs_shared_cache = outs_shared_cache
            # Change the out name to point to the batch index
            out.out_name = i

            outs.append(out)

        next_invocation_id = invocation_id + 1

        return outs

    wrap.is_merkl = True
    wrap.type = 'batch'
    wrap.outs = 1
    wrap.deps = deps
    wrap.__wrapped__ = batch_fn
    return wrap


@doublewrap
def task(f, outs=None, hash_mode=HashMode.FIND_DEPS, deps=None, caches=None, serializer=None):
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
            raise TaskOutsError(f'Mismatch of number of return values in function return statements: {f}')
        elif len(num_returns) != 1:
            raise TaskOutsError(f'Mismatch of number of return values in function return statements: {f}')

        outs = num_returns.pop()
        return_type = return_types.pop()

    if not isinstance(hash_mode, HashMode):
        raise TypeError(f'Unexpected HashMode value {hash_mode} for function {f}')

    fn_code_hash = code_hash(f, hash_mode == HashMode.MODULE)

    if hash_mode == HashMode.FIND_DEPS:
        deps += find_function_deps(f)

    validate_resolve_deps(deps)

    @forwards_to_function(f)
    def wrap(*args, **kwargs):
        global next_invocation_id
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
                invocation_id=next_invocation_id,
            )
            outputs[out_name] = output

        next_invocation_id += 1

        if is_single:
            return outputs[0]
        return outputs if return_type == 'Dict' else tuple(outputs.values())

    wrap.is_merkl = True
    wrap.type = 'task'
    wrap.outs = outs
    wrap.deps = deps
    wrap.__wrapped__ = f
    return wrap


@doublewrap
def pipeline(f, hash_mode=HashMode.FIND_DEPS, deps=None, caches=None):
    deps = deps or []
    caches = caches or []
    sig = signature(f)

    if not isinstance(hash_mode, HashMode):
        raise TypeError(f'Unexpected HashMode value {hash_mode} for function {f}')

    pipeline_code_hash = code_hash(f, hash_mode == HashMode.MODULE)

    if hash_mode == HashMode.FIND_DEPS:
        deps += find_function_deps(f)

    validate_resolve_deps(deps)

    @forwards_to_function(f)
    def wrap(*args, **kwargs):
        global next_invocation_id
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()

        outs = Future(
            f,
            pipeline_code_hash,
            1,
            None,
            deps,
            caches,
            pickle,
            bound_args,
            is_pipeline=True,
            invocation_id=next_invocation_id,
        ).eval()

        next_invocation_id += 1

        return outs

    wrap.is_merkl = True
    wrap.type = 'pipeline'
    wrap.outs = 1
    wrap.deps = deps
    wrap.__wrapped__ = f
    return wrap
