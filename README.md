# Conecta - SQL to Arrow made easy.

» [Documentation]
| [Releases]
| [Issues]
| [Repository]
| [License]


Conecta is a library designed to load data from SQL databases into [Arrow] with maximum 
speed and memory efficiency by leveraging zero-copy and true concurrency in Python.


```python
from conecta import read_sql

table = read_sql(
    "postgres://user:password@localhost:5432/postgres",
    query="select * from lineitem",
    partition_number=4
)
```

This snippet will create a pool, launch 4 threads, send four different queries and
stream the results back to arrow. The core is written in Rust.

Conecta integrates natively to the arrow ecosystem, we support
several arrow libraries: [pyarrow], [arro3] and [nanoarrow].
Additionally, you can create dataframes like Polars or Pandas.

```python
from conecta import read_sql
import polars as pl
import pandas as pd

table = read_sql(
    "postgres://user:password@localhost:5432/postgres",
    query="select * from lineitem",
    partition_number=4,
    
    # By default pyarrow is the arrow backend.
    results_backend='pyarrow'
)

# -- Polars --
# You could use results_backend='arro3' for a smaller
# installation setup.
df = pl.from_arrow(table)

# -- Pandas --
df = table.to_pandas()
```

## How to install.
You can install it with `todo`

## When should you use `conecta`.

## How does conecta work.

[Arrow]: https://arrow.apache.org/

[pyarrow]: https://pypi.org/project/pyarrow/
[arro3]: https://pypi.org/project/arro3-core/
[nanoarrow]: https://pypi.org/project/nanoarrow/

[Documentation]: https://conecta.surister.dev/
[Releases]: https://github.com/surister/conecta/releases
[Issues]: https://github.com/surister/conecta/issues
[Repository]: https://github.com/surister/conecta/
[License]: https://github.com/surister/conecta/blob/master/LICENSE.md