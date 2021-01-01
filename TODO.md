Have a cache backend that dedupes using content hash.
    Cache backend gets content, merkle hash and content hash and can decide itself how to store/recover an item
    Default backend: stores merkle hash -> content hash in SQLite, as well as small contents. Large contents are stored
    in file
    Also provide an S3ContentBackend that gets content from content hash only. Need so be combined with merkle ->
    content hash backend.

    Two types of backends: just merkle hashes, or merkle + content hashes
    When passing along fileobjects, to a streaming hash of the content while writing to file. Can stream bytes into hashlib
    Implement SQLite backend, good for small values and links
Provide a `merkl.collect(*placeholders)` function that returns a list of independent tasks or branches that can be executed
Make sure functions are executed in the same order as the code
write_tracked_file(path)
Have a `pipeline` version of the task decorator, which executes the function instead of deferring, and caches the pipeline
    pipeline can be a alias of task, but with "pipeline" set to True
    Need to ensure that there are not deeply nested placeholders in the return values, they need to be top level
    Need a serializer/deserializer for Placeholder, or is pickle fine? Need to restore the correct cache backend for the
    placeholder too. When pickled, we may need to include the merkl version number in case the merkl implemetation
    changed
Mark a task as a batch version of another task -> hashes of outputs use the code of the single-use task
Visualize graph using graphviz
    Update graph for different events such as: function called, future evaluated, value retrieved from cache etc
    Show code and highlight line next to graph
    Option to output all steps of `merkl run` to a folder
Provide task wrapper for executing shell commands
    Provide file content and fileobject variants
    Provide paths to named pipes instead of output files for the shell command?
Add type hints / mypy
Build CLI for `merkl run <module>.<function>`
    Add --dry option
Lazily do work in Placeholders. Do it from the back so we hit later cached values first and avoid unecessary work in the beginning of the pipeline
Rename Placeholder back to Future: a task can be eager or lazy. If eager, we execute right away and put in cache unless
already in cache. We can also execute it in another process (hence future)
Use setuptools for CLI
