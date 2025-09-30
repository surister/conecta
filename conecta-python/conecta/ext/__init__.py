import functools
import re

import polars
from polars.dependencies import import_optional
from polars._typing import SchemaDict
from polars import DataFrame, from_arrow
from polars.io.database._utils import _read_sql_connectorx as _saved


def _read_sql_conecta(
        query: str | list[str],
        connection_uri: str,
        partition_on: str | None = None,
        partition_range: tuple[int, int] | None = None,
        partition_num: int | None = None,
        protocol: str | None = None,
        schema_overrides: SchemaDict | None = None,
) -> DataFrame:
    """
    Thin wrapper around `conecta.read_sql` to adapt the API to what Polars uses.

    You would normally not use this directly, instead use `patch_polars` and then
    use polars as usual.

    :param query:
    :param connection_uri:
    :param partition_on:
    :param partition_range:
    :param partition_num:
    :param protocol:
    :param schema_overrides:
    :return:
    """
    conecta = import_optional("conecta")
    try:
        if isinstance(query, str):
            query = [query, ]

        tbl = conecta.read_sql(
            conn=connection_uri,
            query=query,
            return_backend='pyarrow',
            partition_on=partition_on,
            partition_range=partition_range,
            partition_num=partition_num,
            # protocol=protocol, Not yet implemented.
        )
    except BaseException as err:
        # basic sanitization of /user:pass/ credentials exposed in connectorx errs
        errmsg = re.sub("://[^:]+:[^:]+@", "://***:***@", str(err))
        raise type(err)(errmsg) from err
    return from_arrow(tbl, schema_overrides=schema_overrides)


def _patch_polars():
    """Monkey patches polars `_read_sql_connectorx` with conecta's `_read_sql_conecta`"""
    polars.io.database._utils._read_sql_connectorx = _read_sql_conecta


def unpatch_polars():
    """Repatches polars `_read_sql_connectorx` with the old function"""
    polars.io.database._utils._read_sql_connectorx = _saved


def patch_polars(arg=None):
    """
    Patches polars if used as a function. Use `conecta.ext.unpatch_polars` to undo the
    monkey patching.

    Example:
        >>> patch_polars()
        >>> polars.read_database_uri(...) # Will use `conecta.read_sql`

    If used as a decorator, if will unpatch polars after the execution, this is intended
    for being used if you only want to patch

    Example

        >>> @patch_polars
        ... def polars_work(): # This will use conecta._readsql
        ...     polars.read_database_uri(...)
        >>> polars_work() # The decorator on exist unpatches polars
                          # so it will use connectorx instead.
    """
    if not callable(arg):
        _patch_polars()
        return None

    func = arg

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        _patch_polars()
        f = func(*args, **kwargs)
        unpatch_polars()
        return f
    return wrapper
