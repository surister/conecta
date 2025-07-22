# Loading data from a sql database to Arrow.

Comparison of performance (duration and max memory usage) of different Polars methods to load
data from a database to a DataFrame (arrow)

Database to test will be Postgres v16 and data will be tphc's 'lineitem' and 'liteitem10x' generated
by https://docs.rs/tpchgen/latest/tpchgen/

Every case is run twice, both results are logged.

## lineitem - polars.read_database - bare
t0: 29.089s - 2.4GiB
t1: 28.512 - 2.43GiB

```python
polars.read_database("select * from lineitem", connection=engine.connect())
```

## lineitem - polars.read_database - iter_batches/batchsize=20_000
t0: 2.344s - 538MiB
t1: 2.301s - 499MiB

```python
polars.read_database(
    "select * from lineitem",
    connection=engine.connect(),
    iter_batches=True,
    batch_size=20_000
)
```

## lineitem - polars.read_database - iter_batches/batchsize=100_000
t0: 2.365s - 646MiB
t1: 2.232s - 586MiB

```python
polars.read_database(
    "select * from lineitem",
    connection=engine.connect(),
    iter_batches=True,
    batch_size=100_000
)
```

## lineitem10x - polars.read_database - bare
t0: 4min - OOM (after 25GiB)
t1: 4min - OOM (after 25GiB)

```python
polars.read_database("select * from lineitem10x", connection=engine.connect())
```

## lineitem10x - polars.read_database - iter_batches/batchsize=20_000
t0: 109.220s - 23.7GiB
t1: 111.922s - 23.4MiB

```python
polars.read_database(
    "select * from lineitem10x",
    connection=engine.connect(),
    iter_batches=True,
    batch_size=20_000
)
```

## lineitem - polars.read_database - iter_batches/batchsize=100_000
t0: 98.622s - 23.46GiB
t1: 103.704s - 23.7GiB

```python
polars.read_database(
    "select * from lineitem10x",
    connection=engine.connect(),
    iter_batches=True,
    batch_size=100_000
)
```
