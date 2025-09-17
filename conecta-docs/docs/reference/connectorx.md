# The difference between `connectorx` and `conecta`

As you might notice, conecta has the same purpose as connectorx, so why write a new library?

I have been a user of Polars for a long time and I work in the database space, therefore
I am very interested in learning how libraries like this work internally.

When I dove into connectorx's source code and [paper], I came up with several ideas that I want
to try to make things more efficient and flexible. It also motivated me that the development
of connectorx has been slow for the last few years.

## What is the goal of conecta?

I have several goals for conecta:

1. I want conecta to be **better documented**, in both code and documentation. I believe that an
open source project needs to have **proper documentation**and advanced **tutorials**
like [optimizing further](using_the_library/optimizing_further.md).

2. I want conecta to be more flexible, that is to offer the user **more features** and **options**, 
especially when it comes to performance, there is not much that you can do in connectorx to further
optimize things. Better integrations like [having several arrow backends](arrow_backends.md) is
also a goal.

3. I want conecta to have **better performance**, to be honest after trying and investigating a lot,
there are not many things that will dramatically increase performance, connectorx is very efficient
as it is. Nonetheless, there are several things that can be done, like improving memory locality,
pre-allocation in arrow and letting the user tweak several internals to optimize for use case.

4. I want conecta to **focus** in `arrow`, I think that it is hard to fully optimize for everything,
therefore I chose to optimize for one destination only. That does not mean that you cannot use
this to load data to pandas, `pyarrow` for instance supports `df.to_pandas()`

5. I want conecta to have full **feature parity** in all sources when possible.

## When should I use conecta instead of connectorx?

I am obviously biased, but being honest, as I'm writing this 2025-07-31 you should use connectorx,
conecta is still not mature enough.

Now, once it's mature enough, I would recommend to benchmark performance, to check maintainment
status and see if you need the extra features that conecta offers.

## Difference between conecta and connectorx.

There are several differences between `conecta` and `connectorx`: 

* conecta has a different way of moving record batches (data) to python.
While internally it might be similar, connectorx gets the pointers of all record batches using the
arrow's C data interface, later in python, record batches are reconstructed, and a pyarrow 
table is made from those batches. In conecta we completely offload that work to [pyo3-arrow](https://crates.io/crates/pyo3-arrow),
simplifying our code base and allowing us to also support other arrow backends.
* Conecta does pre-allocation in arrow, leading to smaller memory usage, conectorx only pre-allocates
when the result is pandas.
* Conecta directly appends deserialized values to pre-allocated arrow arrays, 
connectorx appends values to a small internal buffer.
* Conecta outputs `partition_number` record batches, in other words, one record batch per thread.
Connectorx creates record batches of max length 65536. 
This leads to several issues: on small tables extra allocations will be triggered
when the total count is not divisible by 65536, making the program unnecessarily use extra memory,
on big tables a lot of record batches will be created (sometimes 100s), making [memory locality]
poor. You can read more about this in [(todo, write article )indepth connectorx vs conecta memory allocation].

## Connectorx is awesome.

The team that developed connectorx is awesome and I thank them for their job, there are 1.8k
commits, not an easy task.

Their work was a huge inspiration for this library.

[memory locality]: https://en.wikipedia.org/wiki/Locality_of_reference
[paper]: https://www.vldb.org/pvldb/vol15/p2994-wang.pdf