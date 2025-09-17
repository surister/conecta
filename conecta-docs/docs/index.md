# Conecta - Overview.

This is the documentation for [Conecta](https://github.com/surister/conecta),
a python library designed to load data from SQL databases to [`arrow`](https://arrow.apache.org/) with maximum speed
and memory efficiency by leveraging zero-copy and true concurrency.


## Getting started

The fastest way to get started is to run:

```shell
pip install conecta
```

And start loading data:

```python
from conecta import read_sql

table = read_sql(
    "postgres://user:password@localhost:5400/database",
    queries=["select * from lineitem"],
    partition_on="l_orderkey",
    partition_num=4
)
```

## Documentation overview:

This documentation follows [Diátaxis](https://diataxis.fr/)

I recommend that you start on the how-to guide [Load data](/how-to/read_sql)

## Features
* Connection pooling
* Real parallel multithreading
* Zero-copy mindset, data is only copied **once**
* Rich datatypes
