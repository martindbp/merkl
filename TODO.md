Need a way to load a Future from a tracked file.

Implement cache_metadata and cache_args for tasks. For each cache entry, we now store merkl_hash, args_hash and deps_hash. Args and deps are also stored in the same table. Convert deps from tuple list to dictionary before storing in database, so we can use JSON extension to query.

Investigate if there's some way to persist batch results to a single file. Perhaps we can do:
>> for res in batch_result:
>>      res > 'output_file.json'
And keep track of all Futures that write to a specific file. 


Rename dot command -> dag, options for outputing json or dot

Document ignore_args readme

Can we create Futures on the fly from a generator? MerklList, MerklDict

When a task has a lot of outputs, like a corpus (list of sentences), then it would be much better to cache outputs together.

Strip comments and whitespace from code before hashing? Maybe not, you might want to add some whitespace to get a different hash

Seems like some hash file is being removed on "clear" but not the database record
    It removes stuff that's not part of the pipeline

Make more useful/friendly error messages, e.g. for:
Traceback (most recent call last):
  File "/home/marpett/envs/algernon/bin/merkl", line 33, in <module>
    sys.exit(load_entry_point('merkl==0.1', 'console_scripts', 'merkl')())
  File "/home/marpett/envs/algernon/lib/python3.9/site-packages/merkl-0.1-py3.9.egg/merkl/cli/cli.py", line 112, in main
    route(**kwargs)
  File "/home/marpett/envs/algernon/lib/python3.9/site-packages/merkl-0.1-py3.9.egg/merkl/cli/run.py", line 30, in run
    clize.run(function, args=['merkl-run', *self.unknown_args], exit=False)
  File "/home/marpett/envs/algernon/lib/python3.9/site-packages/sigtools/modifiers.py", line 158, in __call__
    return self.func(*args, **kwargs)
  File "/home/marpett/envs/algernon/lib/python3.9/site-packages/clize-4.1.1-py3.9.egg/clize/runner.py", line 363, in run
    ret = cli(*args)
  File "/home/marpett/envs/algernon/lib/python3.9/site-packages/clize-4.1.1-py3.9.egg/clize/runner.py", line 220, in __call__
    return func(*posargs, **kwargs)
  File "/home/marpett/envs/algernon/lib/python3.9/site-packages/merkl-0.1-py3.9.egg/merkl/cli/run.py", line 13, in _wrap
    outs = f(*args, **kwargs)
  File "/home/marpett/envs/algernon/lib/python3.9/site-packages/merkl-0.1-py3.9.egg/merkl/task.py", line 443, in wrap
    outs = pipeline_future.eval()
  File "/home/marpett/envs/algernon/lib/python3.9/site-packages/merkl-0.1-py3.9.egg/merkl/future.py", line 211, in eval
    specific_out, specific_out_bytes = self._eval()
  File "/home/marpett/envs/algernon/lib/python3.9/site-packages/merkl-0.1-py3.9.egg/merkl/future.py", line 230, in _eval
    outputs = self.fn(*evaluated_args, **evaluated_kwargs)
  File "/home/marpett/Projects/algernon.ai/train_predict.py", line 16, in train_pipeline
    images_dir, masks_dir = generate_dataset.pipeline(20000, 512, 80, seed=42)
  File "/home/marpett/envs/algernon/lib/python3.9/site-packages/merkl-0.1-py3.9.egg/merkl/task.py", line 443, in wrap
    outs = pipeline_future.eval()
  File "/home/marpett/envs/algernon/lib/python3.9/site-packages/merkl-0.1-py3.9.egg/merkl/future.py", line 211, in eval
    specific_out, specific_out_bytes = self._eval()
  File "/home/marpett/envs/algernon/lib/python3.9/site-packages/merkl-0.1-py3.9.egg/merkl/future.py", line 230, in _eval
    outputs = self.fn(*evaluated_args, **evaluated_kwargs)
  File "/home/marpett/Projects/algernon.ai/generate_dataset.py", line 197, in pipeline
    text_image_paths = generate_text_images(text_image_parameters)
  File "/home/marpett/envs/algernon/lib/python3.9/site-packages/merkl-0.1-py3.9.egg/merkl/task.py", line 215, in wrap
    out = single_fn(*args_tuple)
  File "/home/marpett/envs/algernon/lib/python3.9/site-packages/merkl-0.1-py3.9.egg/merkl/task.py", line 332, in wrap
    bound_args = sig.bind(*args, **kwargs)
  File "/usr/lib/python3.9/inspect.py", line 3062, in bind
    return self._bind(args, kwargs)
  File "/usr/lib/python3.9/inspect.py", line 2983, in _bind
    raise TypeError('too many positional arguments') from None
TypeError: too many positional arguments

Catch stdout/err as outputs of the function

Add command to print debug info about a task/pipeline, e.g. printing its deps, fn_code_hash. If we supply a task/pipeline with args and a hash to print, we can print the inputs, outputs and other info

Catching KeyboardInterrupts doesn't work. How can be interrupt a loop and still continue?

Serializers for args?

Make some kind of BaseCache that can be inherited from

Allow files to be seperately tracked if a function returns a single DirOut, use out keys as filenames

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
