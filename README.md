# Conecta - SQL to Arrow made easy.

» [Documentation]
| [Releases]
| [Issues]
| [Repository]
| [License]

![PyPI - Version](https://img.shields.io/pypi/v/conecta)
![PyPI - Status](https://img.shields.io/pypi/status/conecta)
![Python Version from PEP 621 TOML](https://img.shields.io/python/required-version-toml?tomlFilePath=https%3A%2F%2Fraw.githubusercontent.com%2Fsurister%2Fconecta%2Frefs%2Fheads%2Fmaster%2Fconecta-python%2Fpyproject.toml)

[![docs](https://github.com/surister/conecta/actions/workflows/docs.yml/badge.svg)](https://github.com/surister/conecta/actions/workflows/docs.yml)
[![Full build all targets](https://github.com/surister/conecta/actions/workflows/build_python.yml/badge.svg)](https://github.com/surister/conecta/actions/workflows/build_python.yml)
[![release](https://github.com/surister/conecta/actions/workflows/release.yml/badge.svg)](https://github.com/surister/conecta/actions/workflows/release.yml)

[![Test core](https://github.com/surister/conecta/actions/workflows/test_core.yml/badge.svg)](https://github.com/surister/conecta/actions/workflows/test_core.yml)
[![Test python](https://github.com/surister/conecta/actions/workflows/test_python.yml/badge.svg)](https://github.com/surister/conecta/actions/workflows/test_python.yml)



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

## Features:

* Connection pooling
* Real multithreading
* Client-based query partition
* Utilities like: sql bind parameters

Supported sources:
* Postgres: [postgres_docs]

## How to install.
You can get started by running `pip install conecta`

## When should you use `conecta`.



## How does conecta work.


## Supported architectures

Conecta is compiled in almost all popular architectures.

Supported architectures:

* linux: x86_64, x86, aarch64, armv7 and ppc64le
* musllinux: x86_64, x86, aarch64, armv7
* windows: x64 and x86
* macos: x86_64 and aarch64


Unsupported architectures:
* linux: IBM s390x


[Arrow]: https://arrow.apache.org/

[pyarrow]: https://pypi.org/project/pyarrow/
[arro3]: https://pypi.org/project/arro3-core/
[nanoarrow]: https://pypi.org/project/nanoarrow/

[Documentation]: https://conecta.surister.dev/
[Releases]: https://github.com/surister/conecta/releases
[Issues]: https://github.com/surister/conecta/issues
[Repository]: https://github.com/surister/conecta/
[License]: https://github.com/surister/conecta/blob/master/LICENSE.md

[postgres_docs]: https://conecta.surister.dev/databases/postgres/