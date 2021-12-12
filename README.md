# MerkL - ML pipelines in pure Python with great caching

MerkL is a tool for creating ML pipelines in pure Python that are useful for
development and experimentation, but also easy to deploy to production without
modifications. Results are cached using Merkle hashes of the code and inputs as
keys, greatly simplifying caching and reducing the need for a feature store.

NOTE: this project is not production ready and maybe never will be. At this
point I'm the only user. If you're interested in talking about it I can be
reached at me@martindbp.com.

## Tour and examples

In MerkL, pipelines are built using functions decorated with the `task` decorator. When a task is called, the function
body is not exectuted immediately, but instead `Future` objects are returned in place of real outputs. These can then be
passed on to other tasks:

<table>
<tr>
<th>pipeline1.py</th>
<th>merkl dot pipeline1.my_pipeline 42 | dot -Tpng | display</th>
</tr>
<tr>
<td valign="top">

```python
from merkl import task

@task
def task1(input_value):
    return 2 * input_value

@task
def task2(input_value):
    return input_value ** 2

def my_pipeline(input_value: int):
    val = task1(input_value)
    final_val = task2(val)
    return final_val
```

</td>
<td align="center" valign="top">

![](docs/pipeline1.png)

</td>

</tr>

<tr>
<th colspan="2">merkl run pipeline1.my_pipeline 42</th>
</tr>
<tr>
<td colspan="2" valign="top">

```
7056
```

</td>
<tr>
</table>

No function body is executed before `.eval()` is called on a Future, or it is returned by a pipeline executed by the `run` command.
Instead a graph is built and each Future is assigned a hash that uniquely identifies its future value. If the code or input values change, then the
output Future hashes also change. These hashes are then used to find cached results.

As seen in the table above, one can use the `merkl run` command to run a pipline, and the `merkl dot` command to create
a dot file for visualizing the computational graph. The graph can be rendered and displayed by piping the dot file
through the `dot` and `visualize` programs (note that these require that graphviz and imagemagick are installed).

Arguments can be passed to the pipeline through the `run` command. See [clize](https://clize.readthedocs.io/en/stable/)
for more information on how to pass parameters from the command line.

By default, all computed outputs are cached, unless the `--no-cache` option is supplied. If we run the pipeline a second time, we can see that the hashes turn green, which indicates that the values are already in the cache. Changing the code in `task2`, for example by changing the `2` to a `3`, we can see that the output value is
not cached anymore:

<table>
<tr>
<th>First run</th>
<th>Second run</th>
<th>Modify task2</th>
</tr>

<tr>
<td>

![](docs/pipeline1.png)

</td>
<td>

![](docs/pipeline1_2.png)

</td>
<td>

![](docs/pipeline1_3.png)

</td>
</tr>

</table>

Here's a slightly more realistic pipeline consisting of a model training and evaluation task:

<table>
<tr>
<th>pipeline2.py</th>
<th>merkl dot pipeline2.train_eval  | dot -Tpng | display</th>
</tr>
<tr>
<td valign="top">

```python
from merkl import task, read_future

@task
def train(data, iterations):
    return 'trained model'

@task
def evaluate(model, data):
    return 99.3

def train_eval():
    train_data = read_future('train.csv')
    test_data = read_future('test.csv')
    model = train(train_data, iterations=100)
    model > 'model.bin'
    score = evaluate(model, test_data)
    return score, model
```

</td>
<td align="center" valign="top">

![](docs/pipeline2.png)

</td>

</tr>

<tr>
<th colspan="2">merkl run pipeline2.train_eval </th>
</tr>
<tr>
<td colspan="2" valign="top">

```
(99.3, 'trained model')
```

</td>
<tr>
</table>

### Hashing

In order to compute the recursive Merkle hashes, the content (md5) hash of the
input files are needed. MerkL hashes these on demand as they are needed, but
stores these hashes in the `.merkl/cache.sqlite3` database. If the timestamp of
the file ever changes, the file is hashed again.

Files created by MerkL tasks or pipelines are also tracked in the database so that MerkL knows if a file needs to be updated or not.

### Multiple outs

The number of outputs that you want to track separately has to be determinable at DAG "build" time,
therefore it cannot be dependent on some value computed inside a task.

By default, MerkL tries to guess the number of outs by looking at the return statements in the AST (Abstract Syntax
Tree) using Python's inspection capabilities. If the return statement is a Tuple or Dict literal, the values are split
into separate Futures:

<table>
<tr>
<th>Tuple multiple outs</th>
<th>Tuple single out</th>
<th>Dict multiple outs</th>
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
<th colspan="3">STDOUT</th>
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

<table>
<tr>
<th>outs_lambda.py</th>
<th>merkl dot outs_lambda.pipeline  | dot -Tpng | display</th>
</tr>
<tr>
<td valign="top">

```python
from merkl import task

@task(outs=lambda data, k: k)
def split(data, k):
    ksize = len(data) // k
    return [data[i*ksize:(i+1)*ksize]
            for i in range(k)]

def pipeline():
    return split([1, 2, 3, 4, 5, 6], k=2)
```

</td>
<td align="center" valign="top">

![](docs/pipeline3.png)

</td>

</tr>

<tr>
<th colspan="2">merkl run outs_lambda.pipeline </th>
</tr>
<tr>
<td colspan="2" valign="top">

```
([1, 2, 3], [4, 5, 6])
```

</td>
<tr>
</table>

If all else fails, you can set `outs` to any positive integer or an iterable of output dictionary keys.

### Pipe syntax

For convenience, you can optionally chain tasks using the `|` operator, if the tasks have a single input and output. A
future can also be "piped" directly to a file path using the `>` operator:

```python
# Original
write_future(train(preprocess(read_future('train.csv'))), 'model.bin')

# With pipe syntax
read_future('train.csv') | preprocess | train > 'model.bin'
```

### Pipelines

Pipeline functions can optionally be decorated with the `pipeline` decorator. The decorator provides caching of the
graph construction (as opposed to only the evaluation). This is useful when for example there is an inference stage
which depends on a training stage which preprocesses thousands of images. Even though the _result_ of the training is
cached, the _construction_ of the graph may become slow.

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

### Batch tasks

Some functions have a batch version that has a more efficient implementation than the single item version. In this case, we
might want to track the outputs from the batch task _as if_ they were produced by the single item version, such that
outputs between them will be cached. In this case you can use the `batch` decorator:

```python
@batch(embed_word)
def embed_words(words):
    ...
    return embedded_words
```

In some cases, you might only have a batch implementation, but you want each output to be treated individually. In that
case you can also use the `batch` decorator but without an argument:

```python

@batch
def embed_words(words):
    ...
    return embedded_words

```
The difference here is that identical inputs will have the same output Future hash, which is not true for a regular
task. You can tell the difference in these two graphs:

<table>
<tr>
<th>1</th>
<th>2</th>
</tr>
<tr>
<td valign="top">

![](docs/pipeline4_1.png)

</td>
<td valign="top">

![](docs/pipeline4_2.png)

</td>
</tr>

</table>


```python
outs = embed_sentences_batch(['my sentence', 'my sentence'])
assert outs[0].hash == outs[1].hash

outs = embed_sentences_task(['my sentence', 'my sentence'])
assert outs[0].hash != outs[1].hash
```

### HashMode and dependencies

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

As a convenience, you can use the `pip_version()` function to easily add a library's version string as a hash key:

```python
@task(deps=[pip_version('numpy')])
def my_task():
    pass
```

Files and directories can be added as dependencies using the `FileRef` and `DirRef` classes:

```python
from merkl import task, FileRef
@task(deps=[FileRef('my_file.txt')])
def my_task():
    pass
```

Refs added as dependencies will contribute the file content hash to the task.

### Filesystem IO

TBD

### Task versions

TBD

### The Cache

TBD

### Wrapping shell commands

TBD
