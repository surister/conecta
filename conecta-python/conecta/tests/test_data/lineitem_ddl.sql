create table lineitem_small (
  l_orderkey integer not null,
  l_partkey integer,
  l_suppkey integer,
  l_linenumber integer,
  l_quantity integer,
  l_extendedprice double precision,
  l_discount double precision,
  l_tax double precision,
  l_returnflag character(1),
  l_linestatus character(1),
  l_shipdate date,
  l_commitdate date,
  l_receiptdate date,
  l_shipinstruct text,
  l_shipmode text,
  l_comment text
);

