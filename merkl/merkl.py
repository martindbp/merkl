import sys
import json
import hashlib
from warnings import warn
from inspect import signature
from functools import wraps
from .serializers import PickleSerializer
from .utils import doublewrap, OPERATORS

# Cache for the all outputs with the respect to a function and its' args
CODE_ARGS_CACHE = {}

# Cache of code read from function files
CODE_CACHE = {}

# Set true to print hashing bytes in sequence as they are applied
PRINT_HASHING_SEQUENCE = False


class MerkleJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, MerklFuture):
            return {'merkl_hash': obj.hash}
        return json.JSONEncoder.default(self, obj)


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
        bound_args
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

    def get(self):
        for arg_name, val in sorted(self.sig.parameters.items()):
            arg = self.bound_args.arguments.get(arg_name)
            if val.kind == val.VAR_POSITIONAL:
                # It's an *args type parameter
                new_args = []
                for i, a in enumerate(arg):
                    if isinstance(a, MerklFuture):
                        a = a.get()
                    new_args.append(a)
                self.bound_args.arguments[arg_name] = tuple(new_args)
            else:
                if isinstance(arg, MerklFuture):
                    self.bound_args.arguments[arg_name] = arg.get()

        if self.code_args_hash in CODE_ARGS_CACHE:
            output = CODE_ARGS_CACHE.get(self.code_args_hash)
        else:
            output = self.fn(*self.bound_args.args, **self.bound_args.kwargs)
            CODE_ARGS_CACHE[self.code_args_hash] = output

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


def update(m, b):
    # Convenience function to print the bytes used to hash in order
    if PRINT_HASHING_SEQUENCE:
        print(b)
    m.update(b)


def hash_argument(m, arg):
    if isinstance(arg, MerklFuture):
        update(m, bytes(arg.hash, 'utf-8'))
    else:
        if not (isinstance(arg, str) or isinstance(arg, int) or isinstance(arg, float)):
            print(f'WARNING: input arg to function: {str(arg)} is neither str, int or float')
        update(m, bytes(str(arg), 'utf-8'))


@doublewrap
def node(f, outs=None, out_serializers={}, out_cache_policy={}):
    sig = signature(f)
    if callable(outs):
        outs_sig = signature(outs)
        if outs_sig != sig:
            raise Exception(f'`outs` signature {outs_sig} differs from function signature {sig}')

    @wraps(f)
    def wrap(*args, **kwargs):
        # Calculate hash for code and args together
        m = hashlib.sha256()
        # Need to use name, since multiple function can be present in single file
        m.update(bytes(f.__name__, 'utf-8'))

        fn_filename = f.__code__.co_filename
        code = CODE_CACHE.get(fn_filename)
        if code is None:
            with open(fn_filename, 'rb') as code_file:
                code = code_file.read()
        update(m, code)

        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()
        for arg_name, val in sorted(sig.parameters.items()):
            arg = bound_args.arguments.get(arg_name)
            update(m, bytes(arg_name, 'utf-8'))
            if val.kind == val.VAR_POSITIONAL:
                # It's an *args type parameter
                for a in arg:
                    hash_argument(m, a)
            else:
                hash_argument(m, arg)

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
        for i in range(resolved_outs):
            m = hashlib.sha256()
            update(m, bytes(code_args_hash, 'utf-8'))
            if resolved_outs > 1:
                update(m, bytes(str(i), 'utf-8'))
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
            )
            outputs.append(output)

        if resolved_outs == 1:
            return outputs[0]
        return outputs

    return wrap
