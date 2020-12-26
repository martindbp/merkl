Read/write tracked files
    read_tracked_file(path) returns a MerkLFuture which evaluates to a file object. The hash of the MerkLFuture object is the hash stored in the <file>.merkl file. The MerkLFuture object has no `fn` or `sig` set, and doesn't calculate any value on .get()
    write_tracked_file(path)
Visualize graph using graphviz
    Update graph for different events such as: function called, future evaluated, value retrieved from cache etc
    Show code and highlight line next to graph
    Instead of having graphviz as a dependency, allow exporting .dot files, which can be piped into graphviz?
    Option to output all steps of `merkl run` to a folder
Allow nested calling of nodes, for e.g. an outer hyper-parameter optimization function, which calls a training/eval function in the inner body
    Add execute_inner_graph option to decorator, which runs the function at graph-building time without substituting
    MerkLFutures. If a Future is accessed, we stop. Need to go through return values and substitute Futures with actual values
    for this to work.
Build CLI for `merkl run <module>.<function>`
    Add --dry option
Add type hints / mypy
Make sure functions are executed in the same order as the code
Provide a `merkl.collect(*futures)` function that returns a list of independent nodes or branches that can be executed
Mark a node as a batch version of another node -> hashes of outputs use the code of the single-use node
Have a cache backend that dedupes using content hash. Store links: merkle hash -> content hash -> content
    Two types of backends: just merkle hashes, or merkle + content hashes
    When passing along fileobjects, to a streaming hash of the content while writing to file. Can stream bytes into hashlib
Rename node -> task
Rename graph.py -> pipelines.py
Allow outs to be a dict -> each value becomes its own out
Determine number of outs using AST
    All return statements need to have same signature
        Need to filter out return statements inside nested functions
    Multiple outs only if return statement is tuple or dict literal
