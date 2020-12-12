Read/write tracked files
    read_tracked_file(path) returns a MerklFuture which evaluates to a file object. The hash of the MerklFuture object is the hash stored in the <file>.merkl file. The MerklFuture object has no `fn` or `sig` set, and doesn't calculate any value on .get()
    write_tracked_file(path)
Visualize graph using graphviz
    Update graph for different events such as: function called, future evaluated, value retrieved from cache etc
    Show code and highlight line next to graph
Allow nested calling of nodes, for e.g. an outer hyper-parameter optimization function, which calls a training/eval function in the inner body
    Add execute_inner_graph option to decorator, which runs the function at graph-building time without substituting
    MerklFutures. If a Future is accessed, we stop. Need to go through return values and substitute Futures with actual values
    for this to work.
Unindent code before hashing, to decrease duplication
Build CLI for `merkl run <module>.<function>`
