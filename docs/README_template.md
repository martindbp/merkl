# MerkL - create flexible ML pipelines with deep and fine-grained caching

MerkL is a tool for creating ML pipelines in pure Python that are useful for development and experimentation, but also
easy to deploy to production without modifications. Results are cached using Merkle hashes of the code and puts as keys. 
MerkL can be used in conjunction with tools like git-lfs or [DVC](http://dvc.org) to store and track large files such 
as training data and models in version control.

## Tour and examples

In MerkL, pipelines are built using functions decorated with the `task` decorator. When a task is called, the function
body is not exectuted immediately, but instead `Future` objects are returned in place of real outputs. These can then be
passed on to other tasks:

${TABLE1}

No function body is executed before `.eval()` is called on a Future, or it is returned by a pipeline executed by the `run` command.
Instead a graph is built and each Future is assigned a hash that uniquely identifies its future value. If the code or input values change, then the
output Future hashes also change. These hashes are then used to find cached results.

As seen in the table above, one can use the `merkl run` command to run a pipline, and the `merkl dot` command to create
a dot file for visualizing the computational graph. The graph can be rendered and displayed by piping the dot file
through the `dot` and `visualize` programs (note that these require that graphviz and imagemagick are installed).

Arguments can be passed to the pipeline through the `run` command. See [clize](https://clize.readthedocs.io/en/stable/)
for more information on how to pass parameters from the command line.

To set a default cache for all Future values, the `--cache <name>` option can be supplied (here we use the DVC cache). If we run the pipeline twice with this parameter, the values will now be cached, which is indicated by green hashes in the graph. Changing the code in `task2`, for example by changing the `2` to a `3`, we can see that the output value is not cached anymore:

<table>
<tr>
<th>Second run</th>
<th>Modify `task2`</th>
</tr>

<tr>
<td>

![](docs/pipeline1_2.png)

</td>
<td>

![](docs/pipeline1_3.png)

</td>
</tr>

</table>

Here's a slightly more realistic pipeline consisting of a model training and evaluation task:

${TABLE2}

This pipeline assumes that `{train,test}.csv` are large files that we don't want to track in git. We can use
[DVC](http://dvc.org) to track them:

`$ dvc add train.csv test.csv`

This moves a copy to the `.dvc/cache` folder, creates `{train,test}.csv.dvc` files containing the md5 hash. When a DVC
file is read in MerkL, only the md5 content hash is used when building the graph, so there is no need to read the large file contents.

## Details

That concludes the basic features, here are some of the features explained in more detail.

# Multiple outs

The number of individual outputs that you want to track separately has to be determinable at DAG "build" time,
therefore it cannot be dependent on some value calculated inside a task.

By default, MerkL tries to guess the number of outs by looking at the return statements in the AST (Abstract Syntax Tree) using Python's
inspection capabilities. If the return statement is a Tuple or Dict literal, the values are split into separate Futures:

<table>
<tr>
<th>Multiple tuple args</th>
<th>Single return args</th>
<th>Multiple dict args</th>
</tr>
<tr>
<td valign="top">

```python
@task
def my_task():
    return 1, 2, 3

print(my_task())
```

</td>
<td valign="top">

```python
@task
def my_task():
    values = 1, 2, 3
    return values

print(my_task())
```

</td>
<td valign="top">

```python
@task
def my_task():
    return {
        'key1': 1,
        'key2': 2,
        'key3': 3,
    }

print(my_task())
```

</td>
</tr>


<tr>
<td valign="top">

```
(
    <Future: b7a152bd>,
    <Future: 3612309d>,
    <Future: 541053ee>,
)
```

</td>
<td valign="top">

```
<Future: bc930b90>
```

</td>
<td valign="top">

```
{
    'key1': <Future: 7d7f02e7>,
    'key2': <Future: 09f5f892>,
    'key3': <Future: aaba8931>,
}
```

</td>
</tr>
</table>

In some cases we want the number of outs to be dynamic and depend on the input to the function. In this case you can
supply a callable to the `outs` task parameter with the same signature as the task function, and return the number of
outs:

```python
@task(outs=lambda data, k: k)
def kmeans(data, k):
    ...
    return clusters
```

If all else fails, you can set `outs` to any positive integer.

# Pipelines

Pipeline functions can optionally be decorated with the `pipeline` decorator. There are two benefits of doing this. First, the
construction of the the pipeline graph itself is cached, and secondly, the function can yield a dynamic execution plan.

What would be the benefit of caching the construction of a pipeline if all the task results are already individually cached? Well, there are
cases where the graph itself would be very big and take some time to construct, such as when you have a pre-processing
pipeline that loops through thousands of images, preprocesses them and perhaps creates multiple augmentations per image.
The later inference pipeline depends on the preprocessing and training pipelines, and it would be quite inefficient if
the whole graph was built for each invocation of the inference function.

```python

@task
def preprocess(image):
    ...

@task(outs=lambda image, k: k)
def augment(image, k):
    ...

@task
def train(

```

Cached pipelines

Let's say you have a training pipeline

# Batch tasks

Some functions have a batch version that has a more efficient implementation than the single item version. In this case, we
might want to track the outputs from the batch task _as if_ they were produced by the single item version, such that
outputs between them will be cached. In this case you can use the `batch` decorator:

```python
@task
def embed_sentence(sentence):
    ...
    return embedded_sentence

@batch(embed_sentence)
def embed_sentences(sentences):
    ...
    return embedded_sentences
```

In some cases, you might only have a batch implementation, but you want each output to be treated individually. In that
case you can also use the `batch` decorator but without an argument:

```python

@batch
def embed_sentences(sentences):
    ...
    return embedded_sentences

```
The difference here is that identical inputs will have the same output Future hash, which is not true for a regular
task:

```python
outs = embed_sentences_batch(['my sentence', 'my sentence'])
assert outs[0].hash == outs[1].hash

outs = embed_sentences_task(['my sentence', 'my sentence'])
assert outs[0].hash != outs[1].hash
```

# HashMode and dependencies

When the hash of a task function is determined, there are three different `HashMode`s: `FUNCTION`, `MODULE` and
`FIND_DEPS`. The default `HashMode` for both `task`, `batch` and `pipeline` is `FIND_DEPS`.

* `FUNCTION`: Uses the code of the task function for hashing
* `MODULE`: Uses the whole module file
* `FIND_DEPS`: Finds dependencies such as other functions or nonlocal variables used within the function that are
  defined in the same module. If a dep is a `task` then its dependencies are added in turn.

Deps can be added manually and could be a module, function, task, bytes, string or any JSON-serializable object:

```python
@task(deps=['my string'])
def my_task():
    pass
```

As a convenience, you can use the `find_pip_version()` function to easily add a library and its version as a hash key:

```python
@task(deps=[find_pip_version('numpy')])
def my_task():
    pass
```
