When calling batch task, don't evaluate cached results

Ability to make functions out of bash commands e.g:
```
resize_image = bash_task('imagmagick resize {file_arg('input_file', '.png')} {out_file_arg('output_file', '.png')} --size {arg('input_size')}')
```

Keep cache/results in a hidden sqlite database. Don't use the equivalent of .dvc files as they clutters the file system,
instead keep a single ".merklist" file where each line is a file, its merkl hash (and content hash?). Timestamps for
diffing stored in database
    Efficient stats of all files with os.scandir
    Or, put them in the .gitignore so we don't have yet another file, and all the paths need to be there anyway


Implement find_pip_version() / pip_version_of('numpy')

Make sure batch can take single arguments without putting them in tuples

Use git library to read git hashes as an alternative to DVC

Have pipeline be able to yield execution order

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

Provide task wrapper for executing shell commands
    Provide file content and fileobject variants
    Provide paths to named pipes instead of output files for the shell command?

Add type hints / mypy
    Possible to use mypy as a library to enforce types at runtime when we build the graph?

Idea: A task can be eager or lazy. If eager, we execute right away and put in cache unless
already in cache. We can also execute it in another process (hence future)
