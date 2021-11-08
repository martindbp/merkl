import json
import hashlib
import textwrap
import dill
from pathlib import Path
from enum import Enum
from functools import lru_cache
from inspect import getsource, isfunction, ismodule, getmodule
from sigtools.specifiers import forwards_to_function
import merkl
from merkl.utils import (
    doublewrap,
    nested_map,
    nested_collect,
    get_function_return_info,
    find_function_deps,
    FunctionDep,
    get_hash_memory_optimized,
    signature_with_default,
    signature_args_kwargs,
    function_descriptive_name,
    map_set_and_dict_to_list,
)
from merkl.logger import logger, short_hash
from merkl.future import Future
from merkl.exceptions import *
from merkl.cache import SqliteCache
from merkl.io import DirRef, FileRef
from merkl.utils import Eval

next_invocation_id = 0
next_task_id = 0

class HashMode(Enum):
    MODULE = 1
    FUNCTION = 2
    FIND_DEPS = 3


@lru_cache(maxsize=None)
def code_hash(f, is_module=False, version=None, hash_key=None):
    module = None
    if is_module:
        module = getmodule(f)
    code_obj = module if is_module else f
    if version is not None:
        if not isinstance(version, str):
            raise ValueError(f'Task version was not a string: {version}: {type(version)}')
        if module is None:
            module = getmodule(f)
        return f'{module.__name__}.{f.__name__}-{version}'
    elif hash_key is not None:
        if not isinstance(hash_key, str):
            raise ValueError(f'Task hash_key was not a string: {hash_key}: {type(hash_key)}')
        return hash_key

    try:
        code = textwrap.dedent(getsource(code_obj))
    except:
        code = code_obj.__name__
    m = hashlib.sha256()
    m.update(bytes(code, 'utf-8'))
    return m.hexdigest()


def resolve_serializer(serializer, out_name):
    if serializer is None:
        # NOTE: we use dill instead of pickle as default, because dill can serialize a reference to
        # itself, while pickle can't. This is a problem when we want to serialize a Future(e.g. when
        # caching pipeline results)
        return dill
    elif isinstance(serializer, dict):
        return serializer.get(out_name, dill)  # default to dill if key doesn't exist
    else:
        return serializer


def resolve_outs_and_return_type(outs, args, kwargs, return_type):
    resolved_outs = outs(*args, **kwargs) if callable(outs) else outs
    is_iterable = (
        isinstance(resolved_outs, tuple) or
        isinstance(resolved_outs, set) or
        isinstance(resolved_outs, list)
    )
    if is_iterable:
        return_type = 'Dict'

    validate_outs(resolved_outs, return_type=return_type)

    enumerated_outs = None
    if is_iterable:
        enumerated_outs = resolved_outs
    elif isinstance(resolved_outs, int):
        enumerated_outs = range(resolved_outs)
    else:
        raise ValueError

    return resolved_outs, enumerated_outs, return_type


def validate_outs(outs, sig=None, return_type=None):
    if isinstance(outs, int):
        if outs <= 0:
            raise TaskOutsError(f'Outs: {outs} is not >= 1')
    elif isinstance(outs, list) or isinstance(outs, tuple) or isinstance(outs, set):
        for key in outs:
            if not isinstance(key, str):
                raise TaskOutsError(f'Out key is not a string: {key}')
        if return_type != 'Dict' and not sig:
            raise TaskOutsError(f'Out is list/tuple/set of keys but return type is not dict: {return_type}')
    elif callable(outs) and sig is not None:
        outs_sig = signature_with_default(outs)
        if outs_sig.parameters.keys() != sig.parameters.keys():
            raise TaskOutsError(f'Outs function signature does not match task: {outs_sig} vs {sig}')
    elif return_type != 'Dict':
        raise TaskOutsError(f'Bad outs type: {outs}')


def validate_resolve_deps(deps):
    # Collect deps for tasks
    extra_deps = []
    for dep in deps:
        if (
            isinstance(dep, FunctionDep) and
            isfunction(dep.value) and
            hasattr(dep.value, 'is_merkl') and
            dep.value.type in ['task', 'batch']
        ):
            extra_deps += validate_resolve_deps(dep.value.deps)

    resolved_deps = []
    for i in range(len(deps)):
        name = None
        dep = deps[i]
        if isinstance(dep, FunctionDep):
            name = dep.name
            dep = dep.value

        if isfunction(dep):
            if hasattr(dep, 'is_merkl'):
                dep = dep.orig_fn
            try:
                dep = f'<Function {dep.__name__}: {code_hash(dep)}>'
            except OSError:  # source code not available
                logger.debug(f'No source code found for dep: {dep}')
                continue
        elif ismodule(dep):
            try:
                dep = f'<Module {dep.__name__}: {code_hash(dep, is_module=True)}>'
            except OSError:  # source code not available
                logger.debug(f'No source code found for dep: {dep}')
                continue
        elif isinstance(dep, bytes):
            dep = dep.decode('utf-8')
        elif isinstance(dep, Future):
            dep =  f'<Future {dep.fn_descriptive_name} {dep.hash}>'
        elif isinstance(dep, FileRef) or isinstance(dep, DirRef):
            dep = dep.hash_repr()
        elif not isinstance(dep, str):
            dep = nested_map(dep, map_set_and_dict_to_list)
            try:
                dep = json.dumps(dep)
            except TypeError:
                raise SerializationError(f'Task dependency {dep} not serializable to JSON')

        if name is not None:
            resolved_deps.append((name, dep))
        else:
            resolved_deps.append(dep)

    return resolved_deps + extra_deps


def eval_futures(obj):
    if isinstance(obj, Future):
        return obj.eval()
    return obj


@doublewrap
def batch(
    batch_fn,
    single_fn=None,
    hash_mode=HashMode.FIND_DEPS,
    deps=None,
    cache=SqliteCache,
    serializer=None,
    version=None,
    hash_key=None,
    cache_in_memory=None,
    ignore_args=None,
):
    deps = deps or []
    if single_fn is None:
        raise BatchTaskError(f"'single_fn' has to be supplied")

    batch_fn_name = function_descriptive_name(batch_fn)
    single_fn_name = function_descriptive_name(single_fn)

    if hasattr(single_fn, 'has_batch_fn'):
        raise BatchTaskError(f"Trying to create batch task for single function that has already been used")

    if not hasattr(single_fn, 'is_merkl') or single_fn.type != 'task':
        raise BatchTaskError(f'Function {single_fn_name} is not decorated as a task')

    if not isinstance(hash_mode, HashMode):
        raise TypeError(f'Unexpected HashMode value {hash_mode} for function {batch_fn_name}')

    batch_fn_sig = signature_with_default(batch_fn)
    batch_args, batch_kwargs = signature_args_kwargs(batch_fn)
    single_args, single_kwargs = signature_args_kwargs(single_fn)
    if len(batch_args) != 1:
        raise BatchTaskError(f'Batch function {batch_fn_name} must have exactly one input arg (but possibly more kwargs)')

    for kwarg, default in batch_kwargs.items():
        if kwarg not in single_kwargs:
            raise BatchTaskError(f'kwarg {kwarg} passed to batch function {batch_fn_name} that does not exist in single function\'s kwargs')
        single_default = single_kwargs[kwarg]
        if single_default != default:
            raise BatchTaskError(f'{batch_fn_name} kwarg {kwarg} default value ({default}) differs from single function\'s default value ({single_default})')


    # We create/resolve all deps and add them to `single_fn`, such that results
    # that come out of the single_fn and batch task have a hash that depends on
    # the implementation of both
    if hash_mode == HashMode.FIND_DEPS:
        deps += find_function_deps(batch_fn)
    batch_fn_code_hash = code_hash(batch_fn, hash_mode == HashMode.MODULE, version, hash_key)
    deps.append(('batch_function_code_hash', batch_fn_code_hash))
    deps.append(('batch_function_name', function_descriptive_name(batch_fn, include_module=False)))
    deps = validate_resolve_deps(deps)
    for dep in deps:
        single_fn.deps.append(dep)

    single_fn.has_batch_fn = True
    single_fn.orig_fn.has_batch_fn = True

    @forwards_to_function(batch_fn)
    def wrap(args, **kwargs):
        global next_invocation_id

        if callable(args):
            raise BatchTaskError(f'Function {args} is not decorated as a task')
        elif not isinstance(args, list):
            raise BatchTaskError(f'Batch args {args} is not a list')

        outs = []
        outs_shared_cache = {}
        futures = []
        non_cached_outs_args = []
        invocation_id = next_invocation_id

        deps_hash = None

        for i, args_tuple in enumerate(args):
            orig_args_tuple = args_tuple
            # Validate that args is a list of tuples, or single_fn has single input
            if not isinstance(args_tuple, tuple):
                # If single_fn has multiple parameters, then `args_tuple` has to be a tuple
                args_tuple = (args_tuple,)

            # In case `eval_immediately` is set (i.e. we're inside an Eval
            # context), we need to reset this temporarily here, since we don't
            # want to call `single_fn` yet.
            with Eval(False):
                out = single_fn(*args_tuple, **kwargs)

            any_out_not_cached = False
            cached_futures = []
            for future in nested_collect(out, lambda x: isinstance(x, Future)):
                if ignore_args is not None:
                    future.ignore_args = ignore_args  # must set this before calculating hash

                # Set the invocation id to the same for all futures
                future.invocation_id = invocation_id
                # deps_hash is the same for all output Futures, so cache it
                if deps_hash is not None:
                    future._deps_hash = deps_hash
                else:
                    deps_hash = future.deps_hash

                # Trigger calculation of the hash, which will be cached
                future.hash
                # Swap out the function for the batch version
                future.single_fn = future.fn
                future._fn = batch_fn
                # Swap out deps_args_hash to the batch_fn code hash, so
                # that the the value can be taken out of the shared cache
                # NOTE: this doesn't need to have deps and stuff, because it's not used for persistent caching, just to
                # Store the output temporarily
                future._deps_args_hash = batch_fn_code_hash

                if cache_in_memory is not None:
                    future.cache_in_memory = cache_in_memory

                # Override optional parameters
                if cache:
                    future.cache = cache if not merkl.cache.NO_CACHE else None
                if serializer:
                    future.serializer = resolve_serializer(serializer, future.out_name)

                if not future.in_cache():
                    any_out_not_cached = True
                else:
                    cached_futures.append(future)

            if any_out_not_cached:
                for future in cached_futures:
                    future.clear_cache()  # clear the previous cached value

                for future in nested_collect(out, lambda x: isinstance(x, Future)):
                    # Need to set the shared cache to be shared across all batch invocations
                    # NOTE: important only share this cache for outs that are not already in cache
                    future.outs_shared_cache = outs_shared_cache
                    futures.append(future)

                non_cached_outs_args.append((out, orig_args_tuple))

            outs.append(out)

        for future in futures:
            future.outs_shared_futures = futures

        # Swap out the args to the final list of batch args with non-cached results
        batch_bound_args = batch_fn_sig.bind([args for _, args in non_cached_outs_args], **kwargs)
        batch_bound_args.apply_defaults()
        for i, (out, _) in enumerate(non_cached_outs_args):
            for future in nested_collect(out, lambda x: isinstance(x, Future)):
                future.bound_args = batch_bound_args
                # Store the batch index from where the out should pick its results
                future.batch_idx = i

        next_invocation_id = invocation_id + 1

        if len(outs) > 1000:
            logger.debug(f'Batch task {batch_fn_name} has many outs ({len(outs)}), beware that hashing this many outs as args to another task may be slow')

        if merkl.utils.eval_immediately:
            outs = nested_map(outs, eval_futures)

        return outs

    wrap.is_merkl = True
    wrap.type = 'batch'
    wrap.outs = 1
    wrap.orig_fn = batch_fn
    wrap.deps = single_fn.deps
    return wrap


@doublewrap
def task(
    f,
    outs=None,
    hash_mode=HashMode.FIND_DEPS,
    deps=None,
    cache=SqliteCache,
    serializer=None,
    sig=None,
    version=None,
    hash_key=None,
    cache_in_memory=False,
    ignore_args=None,
):
    global next_task_id
    deps = deps or []
    ignore_args = ignore_args or []
    sig = sig if sig else signature_with_default(f)

    return_type = None
    if outs is not None:
        validate_outs(outs, sig)
    else:
        # Get num outs from AST if possible
        return_types, num_returns = get_function_return_info(f)
        if len(return_types) != 1 and 'Tuple' in return_types:
            raise TaskOutsError(f'Mismatch of number of return values in function return statements: {f}')
        elif len(num_returns) != 1:
            if len(num_returns) == 0:
                raise TaskOutsError(f'{f} has no return statement, cannot deduce number of outs')
            else:
                raise TaskOutsError(f'Mismatch of number of return values in function return statements: {f}')

        outs = num_returns.pop()
        return_type = return_types.pop()

    if not isinstance(hash_mode, HashMode):
        raise TypeError(f'Unexpected HashMode value {hash_mode} for function {f}')

    fn_code_hash = code_hash(f, hash_mode == HashMode.MODULE, version, hash_key)

    if hash_mode == HashMode.FIND_DEPS:
        deps += find_function_deps(f)

    deps = validate_resolve_deps(deps)

    deps.append(('function_code_hash', fn_code_hash))
    deps.append(('function_name', function_descriptive_name(f, include_module=False)))
    task_id = next_task_id

    @forwards_to_function(f)
    def wrap(*args, **kwargs):
        global next_invocation_id
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()

        resolved_outs, enumerated_outs, resolved_return_type = resolve_outs_and_return_type(
            outs, args, kwargs, return_type
        )

        outputs = {}
        outs_shared_cache = {}
        is_single = resolved_outs == 1 and resolved_return_type not in ['Tuple', 'Dict']
        futures = []
        deps_hash = None
        for out_name in enumerated_outs:
            out_serializer = resolve_serializer(serializer, out_name)

            future = Future(
                f,
                fn_code_hash,
                resolved_outs,
                None if is_single else out_name,
                deps,
                cache if not merkl.cache.NO_CACHE else None,
                out_serializer,
                bound_args,
                outs_shared_cache,
                invocation_id=next_invocation_id,
                task_id=task_id,
                cache_in_memory=cache_in_memory,
                ignore_args=ignore_args,
            )
            # `deps_hash` triggers an expensive calculation, but it's the
            # same for all output futures, so we cache it and set manually
            if not hasattr(f, 'has_batch_fn'):
                # NOTE: if this is a single_fn for a batch task, we don't want to trigger this calculation, as
                # it would do it for all outputs from the batch task
                if deps_hash is not None:
                    future._deps_hash = deps_hash
                else:
                    deps_hash = future.deps_hash
            outputs[out_name] = future
            futures.append(future)

        for future in futures:
            future.outs_shared_futures = futures

        next_invocation_id += 1

        if merkl.utils.eval_immediately:
            outputs = nested_map(outputs, eval_futures)

        if is_single:
            return outputs[0]

        outputs = outputs if resolved_return_type == 'Dict' else tuple(outputs.values())

        if len(outputs) > 1000:
            logger.debug(f'Task {f} has many outs ({len(outputs)}), beware that hashing this many outs as args to another task may be slow')

        return outputs

    wrap.is_merkl = True
    wrap.type = 'task'
    wrap.outs = outs
    wrap.deps = deps
    wrap.orig_fn = f
    next_task_id += 1
    return wrap


@doublewrap
def pipeline(f, hash_mode=HashMode.FIND_DEPS, deps=None, cache=SqliteCache, cache_in_memory=False, ignore_args=None):
    deps = deps or []
    ignore_args = ignore_args or []
    sig = signature_with_default(f)

    if not isinstance(hash_mode, HashMode):
        raise TypeError(f'Unexpected HashMode value {hash_mode} for function {f}')

    pipeline_code_hash = code_hash(f, hash_mode == HashMode.MODULE)

    if hash_mode == HashMode.FIND_DEPS:
        deps += find_function_deps(f)

    deps = validate_resolve_deps(deps)

    @forwards_to_function(f)
    def wrap(*args, **kwargs):
        global next_invocation_id
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()

        pipeline_future = Future(
            f,
            pipeline_code_hash,
            1,
            None,
            deps,
            cache if not merkl.cache.NO_CACHE else None,
            dill,
            bound_args,
            is_pipeline=True,
            invocation_id=next_invocation_id,
            cache_in_memory=cache_in_memory,
            ignore_args=ignore_args,
        )

        if pipeline_future.in_cache():
            # If a pipeline output was cached but the futures never evaluated
            # (e.g. due to a crash), then when we unserialize and try to use
            # them, merkl will crash. To prevent this, we check that all
            # futures are in cache, otherwise we remove this pipeline result
            # from the cache and re-evaluate
            outs = pipeline_future.eval()

            out_futures = nested_collect(outs, lambda x: isinstance(x, Future))
            out_future_not_cached = False
            for future in out_futures:
                if not future.in_cache():
                    out_future_not_cached = True
                    break

            if out_future_not_cached:
                logger.debug(f'Pipeline output for {function_descriptive_name(f)} ({short_hash(pipeline_future.hash)}) was cached, but output futures were not, so re-evaluating')
                pipeline_future.clear_cache()
                outs = pipeline_future.eval()

            num_futures = len(out_futures)
        else:
            outs = pipeline_future.eval()
            out_futures = nested_collect(outs, lambda x: isinstance(x, Future))

        next_invocation_id += 1

        if len(out_futures) > 1000:
            logger.debug(f'Pipeline {f} has many output futures ({len(out_futures)}), beware that hashing this many futures as args to another task may be slow')

        if merkl.utils.eval_immediately:
            outs = nested_map(outs, eval_futures)

        return outs

    wrap.is_merkl = True
    wrap.type = 'pipeline'
    wrap.outs = 1
    wrap.deps = deps
    wrap.orig_fn = f
    return wrap
