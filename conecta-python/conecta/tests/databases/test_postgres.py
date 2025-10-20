import conecta
import pyarrow

def test_types(pg_conn, pg_types_query):
    table: pyarrow.lib.Table = conecta.read_sql(pg_conn, pg_types_query)
    assert table.num_rows == 1
    assert table.num_columns == 29  # Columns that the pg_types_query has.
