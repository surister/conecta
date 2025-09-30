import arro3
import nanoarrow
import pyarrow
import pytest

import conecta


def test_read_sql_simple(pg_conn):
    query = "select * from lineitem_small"

    table: pyarrow.lib.Table = conecta.read_sql(
        conn=pg_conn,
        query=query,
    )
    assert isinstance(table, pyarrow.lib.Table)
    assert table.num_rows == 10_000
    assert table.num_columns == 16


def test_read_sql_arro3(pg_conn):
    query = f"select * from lineitem_small"
    table: arro3.core.Table = conecta.read_sql(
        conn=pg_conn,
        query=query,
        return_backend='arro3'
    )

    assert isinstance(table, arro3.core.Table)
    assert table.num_rows == 10_000
    assert table.num_columns == 16


def test_read_sql_nanoarrow(pg_conn):
    query = f"select * from lineitem_small"

    table: nanoarrow.array_stream.ArrayStream = conecta.read_sql(
        conn=pg_conn,
        query=query,
        return_backend='nanoarrow'
    )
    schema = table.schema
    assert isinstance(table, nanoarrow.array_stream.ArrayStream)
    assert len(table.to_pylist()) == 10_000
    assert len(schema.fields) == 16

def test_read_sql_unknown_backend():
    query = f"select * from lineitem_small"

    with pytest.raises(ValueError):
        conecta.read_sql(
            conn='',
            query=query,
            return_backend='whot'
        )


def test_read_sql_limit(pg_conn):
    query = f"select * from lineitem_small limit 10"

    table = conecta.read_sql(
        conn=pg_conn,
        query=query,
    )
    assert table.num_rows == 10
    assert table.num_columns == 16


def test_read_sql_queries(pg_conn):
    queries = [
        f'''select *
            from lineitem_small
            where l_orderkey >= 1_108_353
              and l_orderkey < 1_113_255
        ''',
        f'''select *
            from lineitem_small
            where l_orderkey >= 1_113_255
              and l_orderkey <= 1_197_255
        '''
    ]
    table = conecta.read_sql(pg_conn, query=queries)
    assert table.num_rows == 10_000
    assert table.num_columns == 16
