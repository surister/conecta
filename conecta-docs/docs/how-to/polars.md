Conecta fits very well as a optimizable substitute of `connectox` and can
easily be used in Polars. 

# Using conecta in polars.

You have several ways of using `conecta` in Polars:

1. Using `conecta.read_sql` directly and creating a Polars dataframe.

```python
import polars as pl
import conecta as cn

table = cn.read_sql(
    conn="postgres://postgres:postgres@localhost:5432",
    queries=['select * from lineitem'],
    partition_on='l_orderkey',
    partition_num=4,
)

df = pl.DataFrame(table)

# or

from polars import from_arrow
df = from_arrow(table)
```

2. Patching your current polars code.

`conecta.ext.patch_polars` monkey patches `polars.read_database_uri` to use `conecta` instead
of `connectorx`, this action is reversible with `conecta.ext.unpatch_polars`.

```python
import polars as pl
from conecta.ext import patch_polars

# This will run polars as usual, and will fail because connectorx
# does not implement `array_uuid`
df = pl.read_database_uri(
    'select uuid_array from datatypes',
    uri='postgres://postgres:postgres@192.168.88.251:5400')
# pyo3_runtime.PanicException: not implemented: _uuid

patch_polars() # Everything after this line will use `conecta.read_sql` instead.

# This will work!
df = pl.read_database_uri(
    'select uuid_array from datatypes',
    uri='postgres://postgres:postgres@192.168.88.251:5400')
# ┌─────────────────────────────────┐
# │ uuid_array                      │
# │ ---                             │
# │ list[binary]                    │
# ╞═════════════════════════════════╡
# │ [b"\x9b\xd0\xa0\xcf)\xbeK\xea\… │
# └─────────────────────────────────┘
```

If at some point you want to un-do the patching, you can call `unpatch_polars`

```python
from conecta.ext import unpatch_polars

unpatch_polars()
```

If you only want some methods to be patched, you can use it as a decorator:

```python
import polars as pl
from conecta.ext import patch_polars

@patch_polars
def will_succeed():
    df = pl.read_database_uri(
    'select uuid_array from datatypes',
    uri='postgres://postgres:postgres@192.168.88.251:5400')

# After will_succeed() exists, polars is unpatched.
    
def will_fail():
    df = pl.read_database_uri(
    'select uuid_array from datatypes',
    uri='postgres://postgres:postgres@192.168.88.251:5400')

will_succeed()
will_fail()
```

# Recommendations

The general recommendation is to use conecta whenever you can and optimize it for
your use case, you can achieve this by just calling `patch_polars` at the beginning of
your code. If in some parts of your code you need to load from a database not yet supported
by conecta, you can either use `unpatch_polars` before it or use `patch_polars` decorator 
where you **want** to use conecta.