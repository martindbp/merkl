Add command to print debug info about a task/pipeline, e.g. printing its deps, fn_code_hash. If we supply a task/pipeline with args and a hash to print, we can print the inputs, outputs and other info

Catching KeyboardInterrupts doesn't work. How can be interrupt a loop and still continue?

Serializers for args?

Make some kind of BaseCache that can be inherited from

Allow files to be seperately tracked if a function returns a single DirOut, use out keys as filenames

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

To return execution order, yield it in pipeline function while returning the final value, e.g. This way we could yield
execution order multiple times, if we have .eval() calls

Use execution order when calling `merkl run`. Provide `--target [N/<name>]` parameter, where N is number of cores if on
local machine, and <name> could be AWS or something.

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
