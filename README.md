# MerkL - track ML data, models and create cached pipelines

MerkL is a tool for tracking large data (data sets, models) in git, and creating ML pipelines in pure Python code with
cachable intermediate and final results, suitable both for development and production.

## What problems does MerkL solve?

There are a few problems when developing, deploying and productionizing ML models and pipelines:

* Large data such as training data sets and binary models are too large to version in git
* We want to version code and data together so that you can track experiments, deploy with confidence and know how a
  result was calcualted (data provenance)
* We want to be able to use the same tools (Github, CI, etc) for ML as for the rest of the product
* In production (and development), we want to cache calculations in ML pipelines that do not need to be recalculated
* We want to be able to create a pipeline during development, and easily deploy it without having to reimplement it
* Pipeline results from production should be easily reproducible in development

MerkL tries to solve these issues by:

1. Providing simple tools to offload the storage of large files to e.g. S3 and only keep references in the git repository
2. Providing a way to chain together Python functions into pipelines, and fetch cached results without first having to executing the functions
3. Allow pipelines to be run as scripts or deployed directly in a web server without loss of efficiency

## Technical details

MerkL is inspired by [DVC](http://dvc.org) and provides much of the same functionality, but differs in a few ways:

1. MerkL pipelines and functions are defined in pure Python and can be run from the command line as well as from a script.
   Pipelines are thus suitable to be served from a web server without incurring the overhead of script start-up
   time or loading of dependencies for each invocation.
2. MerkL defines a _MerkLe DAG_ (Directed Asyclic Graph), also known as _block-chain_, consisting of the initial data,
   code and output hashes. The output hash are are the combined hashes of the inputs, the code, and the output index.
   All intermediate and final output hashes can therefore be calculated without having to execute any function body
   code. MerkL can then easily check if these values exist in the cache without the need for external logs of pipeline results.
3. Since pipelines are defined in code, one can create dynamic pipelines, e.g functions with
   variable number of inputs and outputs. This is possible since all normal control structures (if, for, while) are
   available when defining the pipeline in code.
4. Outputs can be saved to file, but since data is passed in memory between chained function
   calls (and cached by default), it is not mandatory to write the output to file.
5. Breakpoints and debugging works as usual since the pipeline and nodes run in a single Python script.

Besides this core functionality, MerkL provides some simple commands for tracking files and pushing/pulling them from
remote storage.

## Commands

### `merkl init`
Creates a MerkL repository in the current directory. This creates the hidden `.merkl` folder which contains the local
cache and the `config` file. The `.merkl` directory is also added to the .gitignore if the file exists.

### `merkl track <file>`
MerkL provides this command to move large files such as ML data sets or models to a cache folder outside of git, and track the file using its content hash instead. The steps that the command executes are:

1. Hash the file
2. Create a new file `<file>.merkl` containing the file hash and timestamp
3. Move the file to `.merkl/cache` and symlink it back to the git repo
4. Add `<file>` to `.gitignore`

### `merkl run <module>.<function> [args] [--kwargs] [-0 <file>] [-1 <file>] ... [-N <file>]`
Run a pipeline or node function. Command line arguments are passed on as function arguments. The pipeline function is turned into a CLI using the [clize](https://clize.readthedocs.io/en/stable/#) library.
Paths for the outputs to be written to can be supplied with the `-n <file>` option, where n takes the value of the output index.

### `merkl dry-run [--pull] [--fill-missing] <module>.<function> [args] [--kwargs]`
Like `merkl run` but doesn't calculate any values.
If --pull is supplied, any cached values are pulled
If --fill-missing is supplied, any missing function arguments are set to placeholder objects.

### `merkl push [<file>]`
Pushes tracked file to remote storage, e.g. Redis, S3, a relational database or a combination of destinations. If no
files are specified, then all tracked files in the project are pushed.

### `merkl pull [<file>]`
Pulls tracked file from remote to local storage. If no files are specified, then all tracked files in the project are pushed.

## Nodes

A Python function can be turned into a pipline node by the use of the `merkl.node` decorator:

```
from merkl import node

@node
def my_function(input_value):
    return 2 * input_value
```

Node functions have these restrictions:

1. The function must be deterministic, i.e. all randomness needs to be seeded either by a constant in the function or
   by an input argument
2. The number of outputs need to be determinable without running the body of the function. By default the decorator
   assumes there to be a _single_ output, but it can be set to another constant value in the decorator:
   ```
   @node(outs=2)
   def my_function(input_value)
   ```

   If the number of outputs is variable and depends on the input arguments, `outs` can be set to a callable function with the
   same signature as the original function that returns the number of outputs:
   ```
   @node(outs=lambda input_value, k: k)
   def my_clustering_function(input_value, k=3)
   ```
   Note that the input arguments may contain futures for upstream calcalations that cannot be accessed.

The `node` decorator also takes these optional arguments:

* `serializers`: a map between out index and a MerkLSerializer class. The default serializer is JsonSerializer
* `cache_policy`: either a CachePolicy object or a map between output index and CachePolicy objects
* `code_hash_mode`: CodeHashMode.[FUNCTION,MODULE], defaults to MODULE. This means the code in the whole file the function
  resides in is used to compute the function hash
* `code_deps`: a list of modules functions, strings or bytes this function depends on for code hashing

## Pipelines

Pipelines can be defined anywhere, but can usefully be defined in a self-contained function, which can then be run with the `merkl
run <module>.<function>` command, or reused in a script.

When calling a MerkL node function, it returns one or several `MerkLFuture` _placeholder_ objects (depending on number of
outs), which represents an value that hasn't been computed or fetched yet. In order to access the actual value, call
the `.get()` method on the future. This will perform the computation of the output and all needed preceding values
recursively, using cached values when available.

To persist a computed output to disk, call the `write_file(output, track=[True/False])` function. If `track=False`, the
file is written as-is. If True, the file is tracked the same way as when calling `merkl track <file>`.

[EXAMPLE HERE]

## Deploying

Deploying a pipeline is as easy as checking out the git repository and running the `merkl dry-run --pull --fill-missing <module>.<function> --model my_large_model.pickle`.
This does a dry-run of the pipeline without running any calculations, and pulls any referenced files that are cached (the model file).

## Serving

Serving a pipeline is done by simply running the pipeline function and returning the result. Any cached values will be
read locally or fetched remotely if available.

If the requester wants to save time by sending along the cached values, it can do a dry-run to get the cached values,
send them in the request, and on the server side those values can be added to the local cache.
