Combine args, code and function name into one JSON ouput and hash that -> better for debugging
Add code_hash_mode = ['file', 'function']
Add code_deps = [module, function]
Read/write tracked files
    read_tracked_file(path) returns a MerklFuture which evaluates to a file object. The hash of the MerklFuture object is the hash stored in the <file>.merkl file. The MerklFuture object has no `fn` or `sig` set, and doesn't calculate any value on .get()
    write_tracked_file(path)
Add nested_map(struct, f) function which takes a nested structure and calls `f` on all non-list/tuple/dict values
Use nested_map to replace MerklFuture objects with values in input args
Use a more local cache for getting the output value for different output indices
Override MerklFuture __hash__ and __eq__ and other operators to raise an error if used (so that they can't be added to
sets, or used in control flow)
    import operator
    class Foo(object):

        a=0 

        def __init__(self, a):
            self.a=a            

        def operate(self, other, op):
            #common logic here
            return Foo(op(self.a, other.a))

Add `outs` as a function with same signature as original function (raise error if not)
