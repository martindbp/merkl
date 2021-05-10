Make some kind of BaseCache that can be inherited from

Store large contents as files instead of in sqlite

When adding FileRef/DirRef, need to make a record in the cache that there's a file

Add verbose command and logging

Allow files to be seperately tracked if a function returns a single DirOut, use out keys as filenames

clize parameters doesn't work for pipelines

Add a "code_version" parameter to task which supersedes any code hash, but not deps

Add type hints / mypy
    Attach a type hint to Future, check for equality when passing in to other tasks
    Use typeguard to check constant inputs and return values

Ability to make functions out of bash commands e.g:
```
resize_image = bash_task('imagmagick resize {file_arg('input_file', '.png')} {out_file_arg('output_file', '.png')} --size {arg('input_size')}')
```

Implement `merkl status` that goes through all files tracked in the sqlite db, does stat on them in the filesystem
and lists if they have been changed


Implement find_pip_version() / pip_version_of('numpy')

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

Idea: A task can be eager or lazy. If eager, we execute right away and put in cache unless
already in cache. We can also execute it in another process (hence future)
