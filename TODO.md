Combine args, code and function name into one JSON ouput and hash that -> better for debugging
Add code_hash_mode = ['file', 'function']
Add code_deps = [module, function]
Read/write tracked files
    read_tracked_file(path) returns a MerklFuture which evaluates to a file object. The hash of the MerklFuture object is the hash stored in the <file>.merkl file. The MerklFuture object has no `fn` or `sig` set, and doesn't calculate any value on .get()
    write_tracked_file(path)
Add nested_map(struct, f) function which takes a nested structure and calls `f` on all non-list/tuple/dict values
Use nested_map to replace MerklFuture objects with values in input args
Use a more local cache for getting the output value for different output indices
