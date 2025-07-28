# Arrow backends

Conecta currently supports three arrow backends:

[pyarrow], [arro3] and [nanoarrow]

The default one is `pyarrow`, you can change it with `return_backend`:
```python
from conecta import read_sql

table = read_sql(
    "postgres://user:password@localhost:5432",
    queries=['select l_orderkey from lineitem'],
    partition_on='l_orderkey',
    partition_num=4,
    return_backend='arro3' # <--------- change here
)
```
Every return backend has a different python package that you need to install in your
environment, otherwise an exception will be raised.

## Why different arrow backends?

These libraries are implementations of the arrow specification, which is not tied to a programming
language, each have different properties and advantages that you can take advantage of. If you
come across an advantage that is not documented here, please contribute it!

## `arro3`
The package size is significantly smaller (2.2-2.9MB) than `pyarrow`'s (26-45MB), depending on the 
system.

It is less feature-complete but is perfectly fine if you are just loading data in e.g. Polars. Creating
a polars dataframe from an `arro3` table can be measurably faster than `pyarrow`.

Releases are typically faster and paired with the latest arrow version.

```python
import timeit
from conecta import read_sql

times = 10
result = timeit.timeit(
    """
t = read_sql(
    "postgres://postgres:postgres@192.168.88.251:5400",
    queries=['select l_orderkey from lineitem10x'],
    partition_on='l_orderkey',
    partition_num=4,
    return_backend='arro3' # pyarrow
)

df = polars.from_arrow(t)
    """, globals=globals(), number=times,
)

print(result / times)
```

**0.4460104392997891s** for `pyarrow`

**0.33837970568998571s** for `arro3`

In this benchmark arro3 is ~24% faster than pyarrow.

## `pyarrow`

Is the most prominent one, feature-full and generally available in many environments, it is
the default backend for compatability reasons.

## `nanoarrow` 
Returns an `ArrayStream` which your application might benefit from.



[pyarrow]: https://pypi.org/project/pyarrow/
[arro3]: https://pypi.org/project/arro3-core/
[nanoarrow]: https://pypi.org/project/nanoarrow/
