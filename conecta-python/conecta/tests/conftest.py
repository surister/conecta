import pathlib
from typing import Any, Generator

import pytest
import sqlalchemy
from sqlalchemy import text
from testcontainers.postgres import PostgresContainer

def test_data_dir() -> pathlib.Path:
    """Returns the path of test_data direction"""
    cwd = pathlib.Path().cwd()

    if 'tests' not in cwd.parts:
        # path is not inside conecta-python
        if 'conecta-python' not in cwd.parts:
            cwd = cwd / 'conecta-python'
        cwd = cwd / 'conecta/tests'

    test_dir = cwd / 'test_data'
    return test_dir

@pytest.fixture(scope='session')
def pg_conn() -> Generator[str, Any, None]:
    """
    Starts a new postgres container and loads the following tables

    - lineitem_small - a lineitem table with 10_000 rows, used to .
    - postgres_datatypes - a table containing many postgres datatypes, used to test types.

    :return: The connection string for the database.
    """
    test_dir = test_data_dir()

    LINEITEM_DDL = open(test_dir / 'lineitem_ddl.sql').read()
    LINEITEM_DATA = open(test_dir / 'lineitem_small.sql').read()

    PG_DATATAYPES_DDL = open(test_dir / 'pg_datatypes_ddl.sql').read()
    PG_DATATAYPES_DATA = open(test_dir / 'pg_datatypes_data.sql').read()

    with PostgresContainer("postgres:18") as postgres:
        postgres.with_command('psql -c "create table t (a text)"')
        engine = sqlalchemy.create_engine(postgres.get_connection_url())
        conn = engine.connect()
        conn.execute(text(LINEITEM_DDL))
        conn.execute(text(LINEITEM_DATA))
        conn.execute(text(PG_DATATAYPES_DDL))
        conn.execute(text(PG_DATATAYPES_DATA))
        conn.commit()

        res = conn.execute(text('select count(*) from lineitem_small')).fetchall()
        # Load data was successful
        assert res[0][0] == 10_000

        conn.close()
        del LINEITEM_DATA
        del PG_DATATAYPES_DATA

        conn_url = postgres.get_connection_url().replace('+psycopg2', '')
        yield conn_url

@pytest.fixture()
def pg_types_query() -> str:
    """
    :return: The select query for pg_datatypes_select
    """
    return open(test_data_dir() / 'pg_datatypes_select.sql').read()