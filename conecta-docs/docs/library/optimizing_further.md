# Optimizing further

Conecta already has great performance and low memory usage, nontheless there are things that
can be done on the source database to speed up data loading. It is recommended that you familiarize
yourself with conecta [internals](/project/internals).

The gist of it is that queries that contain aggregations and filters over the `partition_on` column are used in several
metadata queries and the data loading queries. Depending on the database and data volume the metadata queries alone
can take up 5-40% of the total time, therefore anything that speeds those queries will improve performance.

The following techniques might not be applicable to all queries or sources, understanding the basics of the database
you are using and these techniques is needed to evaluate what can be optimized.

Before optimizing further, consider whether the current performance is acceptable or not, optimizing for the sake
of it can be *fun* but *pointless*.

## Using indexes

You can use indexes to speed filtering and aggregations on the indexed column. If the query you are trying to load
the data from contains joins on several tables, it might be needed to add several indexes.

> Indexing depends on the database source: indexing policies and inddx maintenance like VACUUM operations
> might be needed for consistency. Read the source database manual carefully.

For example: **Postgres**, reading TPHC lineitem 10x.

### Without index

* Time: **152.24s**
* Memory: **8040.894Mb RAM**

### With index

```sql
CREATE INDEX CONCURRENTLY idx_lineitem10x_orderkey ON lineitem10x (l_orderkey);
```

* Time: **91.75s**
* Memory: **804.0894Mb RAM**

It reduced the metadata queries down to a few ms cutting down 40% of the total load time.