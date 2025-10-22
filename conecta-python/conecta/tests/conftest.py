import pathlib

from typing import Any, Generator

import pytest
import sqlalchemy
from sqlalchemy import text
from testcontainers.postgres import PostgresContainer

def get_test_data_path() -> pathlib.Path:
    """Returns the path of test_data directory"""
    cwd = pathlib.Path().cwd()

    if 'tests' not in cwd.parts:
        # path is not inside conecta-python
        if 'conecta-python' not in cwd.parts:
            cwd = cwd / 'conecta-python'
        cwd = cwd / 'conecta/tests'

    test_dir = cwd / 'test_data'
    return test_dir

@pytest.fixture(scope='session')
def postgres_postgis_conn() -> Generator[str, Any, None]:
    test_dir = get_test_data_path()

    PGIS_DATATYPE_DDL = open(test_dir / 'pg_postgis_datatypes_ddl.sql').read()
    PGIS_DATATYPE_DATA = open(test_dir / 'pg_postgis_datatypes_data.sql').read()
    with PostgresContainer(
            "postgis/postgis:17-3.5",
            username='postgres',
            password='postgres',

    ) as postgres:
        engine = sqlalchemy.create_engine(postgres.get_connection_url())
        conn = engine.connect()

        for query in [PGIS_DATATYPE_DDL, PGIS_DATATYPE_DATA]:
            conn.execute(text(query))
        conn.commit()
        conn.close()

        yield postgres.get_connection_url().replace('+psycopg2', '')


@pytest.fixture(scope='session')
def pg_conn() -> Generator[str, Any, None]:
    """
    Starts a new postgres container and loads the following tables

    - lineitem_small - a lineitem table with 10_000 rows, used to .
    - pg_datatypes - a table containing many postgres datatypes, used to test types.
    - pg
    :return: The connection string for the database.
    """
    test_dir = get_test_data_path()

    LINEITEM_DDL = open(test_dir / 'lineitem_ddl.sql').read()
    LINEITEM_DATA = open(test_dir / 'lineitem_small.sql').read()

    PG_DATATAYPES_DDL = open(test_dir / 'pg_datatypes_ddl.sql').read()
    PG_DATATAYPES_DATA = open(test_dir / 'pg_datatypes_data.sql').read()

    with PostgresContainer("postgres:18", driver=None) as postgres:
        engine = sqlalchemy.create_engine(postgres.get_connection_url())
        conn = engine.connect()

        for query in [LINEITEM_DDL, LINEITEM_DATA, PG_DATATAYPES_DDL, PG_DATATAYPES_DATA]:
            conn.execute(text(query))
        conn.commit()
        res = conn.execute(text('select count(*) from lineitem_small')).fetchall()

        # Load data was successful
        assert res[0][0] == 10_000

        conn.close()
        del LINEITEM_DATA
        del PG_DATATAYPES_DATA

        yield postgres.get_connection_url().replace('+psycopg2', '')

@pytest.fixture()
def pg_types_query() -> str:
    """
    :return: The select query for pg_datatypes_select
    """
    return open(get_test_data_path() / 'pg_datatypes_select.sql').read()

@pytest.fixture()
def postgres_postgis_types_query() -> str:
    return open(get_test_data_path() / 'pg_postgis_datatypes_select.sql').read()
