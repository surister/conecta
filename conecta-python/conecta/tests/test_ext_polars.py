import pyarrow

from conecta.ext import patch_polars, unpatch_polars
from unittest import mock

from polars import read_database_uri

def test_patch_polars():
    """Tests that when we patch polars, `polars.read_database_uri` actually calls read_sql"""

    patch_polars()

    with mock.patch('conecta.read_sql') as f:
        f.return_value = pyarrow.array([])

        read_database_uri("", "")

        f.assert_called_once()

def test_unpatch_polars():
    """Tests that after unpatching polars, it calls connectorx."""
    from polars import read_database_uri
    patch_polars()
    unpatch_polars()

    with mock.patch('connectorx.read_sql') as f:
        f.return_value = pyarrow.array([])

        read_database_uri("", "")

        f.assert_called_once()

def test_patch_polars_decorator():
    """Same as test_patch_polars but using it as decorator"""

    @patch_polars
    def t():
        from polars import read_database_uri
        with mock.patch('conecta.read_sql') as f:
            f.return_value = pyarrow.array([])
            read_database_uri("", "")
            f.assert_called_once()
    t()

    # Now test that is back to normal
    with mock.patch('connectorx.read_sql') as f:
        f.return_value = pyarrow.array([])

        read_database_uri("", "")

        f.assert_called_once()
