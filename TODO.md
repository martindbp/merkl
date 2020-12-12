Read/write tracked files
    read_tracked_file(path) returns a MerklFuture which evaluates to a file object. The hash of the MerklFuture object is the hash stored in the <file>.merkl file. The MerklFuture object has no `fn` or `sig` set, and doesn't calculate any value on .get()
    write_tracked_file(path)
Visualize graph using graphviz
    Update graph for different events such as: function called, future evaluated, value retrieved from cache etc
    Show code and highlight line next to graph
