Implement HashMode.RECURSIVE by adding functions defined in same module as deps

Refactor exceptions

Add capability to store result metadata in the cache
    Cache backend gets meta data as well, decides if and how to store it

Add ability to serialize/deserialize straight to fileobject
    Cache needs to have a get_fileobject and add_fileobject

To return execution order, yield it in pipeline function while returning the final value, e.g. This way we could yield
execution order multiple times, if we have .eval() calls

Use execution order when calling `merkl run`. Provide `--target [N/<name>]` parameter, where N is number of cores if on
local machine, and <name> could be AWS or something.

Add a FileObjectWriteFuture (and rename FileObjectFuture -> FileObjectReadFuture?) which is returned when doing:
    @task
    def task1():
        with merkl_open(flags='b') as f:
            f.write(...)

        return f

Implement --pull option for run command, that doesn't run, just pulls the tracked files and cached futures
    For tracked files, need to modify the timestamp to match the one in the .merkl file

Have a cache backend that dedupes using content hash.
    Cache backend gets content, merkle hash and content hash and can decide itself how to store/recover an item
    Default backend: stores merkle hash -> content hash in SQLite, as well as small contents. Large contents are stored
    in file
    Also provide an S3ContentBackend that gets content from content hash only. Need so be combined with merkle ->
    content hash backend.

    Two types of backends: just merkle hashes, or merkle + content hashes
    When passing along fileobjects, to a streaming hash of the content while writing to file. Can stream bytes into hashlib
    Implement SQLite backend, good for small values and links

Make sure functions are executed in the same order as the code

Have a `pipeline` version of the task decorator, which executes the function instead of deferring, and caches the pipeline
    pipeline can be a alias of task, but with "pipeline" set to True
    Need to ensure that there are not deeply nested futures in the return values, they need to be top level
    Need a serializer/deserializer for Future, or is pickle fine? Need to restore the correct cache backend for the
    future too. When pickled, we may need to include the merkl version number in case the merkl implemetation
    changed

Provide task wrapper for executing shell commands
    Provide file content and fileobject variants
    Provide paths to named pipes instead of output files for the shell command?

Add type hints / mypy

Idea: A task can be eager or lazy. If eager, we execute right away and put in cache unless
already in cache. We can also execute it in another process (hence future)
