# IDEs like Pycharm will not detect 'sum_as_string', additionally, we wrap it around
# a dummy `sum_as_string` to be able to add docstring and typehints.
import dataclasses
import json
import re
from typing import Literal, Optional

from .conecta import create_partition_plan as _create_partition_plan
from .conecta import read_sql as _read_sql


def set_debug_log(mode: Literal['perf', 'lib', 'all'] = 'lib') -> None:
    """
     Sets the debugging log configuration of conecta. It configures the `RUST_LOG` environment
     variable accordingly.

    Args:
        mode: The mode that will be used.

    Modes:

    * perf: Logs only show the conecta::perf_logger logger, helpful to debug
     ram usage and timings of data load.
    * lib: Logs everything that happens in conecta_core and conecta_python,
     includes `conecta_performance`, helpful to debug the library, includes `partition plan`.
    * all: Logs everything, other crates like sql parsing, http...

    Returns:

    """
    import os

    match mode:
        case "perf":
            rust_log = 'conecta_core::perf_logger=debug'
        case "lib":
            rust_log = 'conecta=debug,conecta_core=debug'
        case "all":
            rust_log = 'debug'
        case _:
            raise ValueError(f"mode {mode} does not exist")

    os.environ['RUST_LOG'] = rust_log


def sql_bind(sql: str,
             parameters: dict,
             char_delimiter: str = ':',
             quote_ident_with: str = '"') -> str:
    """Replaces parameters in an SQL statement with values from a dictionary.

    Supports integers, strings, and null values while ensuring proper escaping.

    Supports special function 'IDENT', to quote identifiers.

    Args:
        sql: The SQL statement with :parameter placeholders.
        parameters: Dictionary containing parameter names and values.
        char_delimiter: Character prefix for placeholders (default: ':').
        quote_ident_with: Character for quoting IDENT values (default: '"').

    Raises:
        ValueError: If an unsupported data type is encountered.

    Returns:
        The SQL statement with placeholders replaced by values.

    Examples:
        >>> safe_sql_replace('select IDENT(:col) FROM tbl1 t WHERE t.value =
        ... :var1 and t.name = :var2', {'col': 'col1', 'var1': 1, 'var2': 'somename'})
        select "col1" FROM tbl1 t WHERE t.value = 1 and t.name = 'somecol'
    """

    def format_value(value):
        """Formats the value inside the string depending on type."""
        if isinstance(value, str):
            # Escape single quotes.
            value = value.replace("'", "''")

            # Wrap the value in single quotes
            replacement = f"'{value}'"
            return replacement

        if isinstance(value, (int, float)):
            return str(value)

        if value is None:
            return 'NULL'

        raise ValueError(f'unsupported parameter type: {type(value)}')

    for key, value in parameters.items():
        replacement = format_value(value)
        to_replace = char_delimiter + key

        # Replace FIELD(:value) with quoted value
        sql = re.sub(rf'IDENT\({to_replace}\)',
                     replacement.replace("'", quote_ident_with),
                     sql)

        # Replace :value with actual value
        sql = re.sub(rf'(?<!\w){to_replace}(?!\w)',
                     replacement,
                     sql)

    return sql


@dataclasses.dataclass
class PartitionConfig:
    """The partition plan that ``conecta`` will use to perform the data load.

    Attributes:
        queries: The SQL queries provided by the user. If there is more than one,
         it is considered that the user performs the partitions.
        partition_on: The column that will be used to partition, it has to be a sortable data type,
         like integers. Ideally, the column is indexed and values are uniformly distributed.
        partition_num: The number of partitions, the ideal number
         is typically close to CPU core count.
        partition_range: The min and max value of `partition_num`.
        needed_metadata_from_source: The metadata that needs to be fetched from the ``Source``. There
         are two options: [Count,CountAndMinMax]
        query_partition_mode: The mode that was inferred from the user parameters, there are
         three options: [OneUnpartitionedQuery, OnePartitionedQuery, PartitionedQueries] See more at
         https://github.com/surister/conecta/blob/70af291199b1a794e00455816f01d925f5a8c040/conecta-core/src/metadata.rs#L17
        ...
    """
    query: list[str]
    partition_on: Optional[str]
    partition_num: Optional[int]
    partition_range: tuple[int]
    needed_metadata_from_source: str
    query_partition_mode: str
    preallocation: bool


@dataclasses.dataclass
class PartitionPlan:
    """The partition plan that ``conecta`` will use to
    perform the data load.

    Attributes:
        min_value: The min value of the given column `partition_on`.
        max_value: The max value of the given column `partition_on`.
        count: The nº of rows in the table.
        metadata_query: The query used to get the metadata: [min, max and count], depending on the user's parameters, it might not contain min/max (if partition_range is present).
        query_data: The list of queries that will be used to fetch the data.
        partition_config: The configuration given by the user, it is validated and should be considered
         valid.
    """
    min_value: int
    max_value: int
    counts: list
    metadata_query: str
    data_queries: list[str]
    partition_config: PartitionConfig

    @classmethod
    def from_dict(cls, d: dict):
        """
        Create a ``PartitionPlan`` from a dictionary. It is expected that all keys
        match ``PartitionPlan``'s attributes.
        """
        partition_config = d.pop('partition_config')
        return cls(**d, partition_config=PartitionConfig(**partition_config))


def create_partition_plan(
        conn: str,
        query: list[str] | str,
        partition_on: Optional[str] = None,
        partition_range: tuple = None,
        partition_num: int = None,
        **config
) -> PartitionPlan:
    if isinstance(query, str):
        query = list(query)

    pool_size = config.get('max_pool_size')
    preallocation = config.get('preallocation')
    plan = json.loads(
        _create_partition_plan(
            conn,
            query,
            partition_on,
            partition_range,
            partition_num,
            pool_size if pool_size is not None else 1,
            preallocation if preallocation is not None else True
        )
    )
    return PartitionPlan.from_dict(plan)


def read_sql(
        conn: str,
        query: list[str] | str,
        partition_on: Optional[str] = None,
        partition_range: Optional[tuple] = None,
        partition_num: Optional[int] = None,
        return_backend: Literal['pyarrow', 'arro3', 'nanoarrow'] = 'pyarrow',
        **extra_conf
):
    if isinstance(query, str):
        query = [query]

    extra_conf_options = {"max_pool_size", "preallocation"}

    default_conf = {
        'max_pool_size': None,
        'preallocation': False
    }

    if extra_conf is None:
        # Default values for extra_conf, otherwise we err when calling the rust
        # generated method.
        extra_conf = {}

    extra_conf = default_conf | extra_conf

    if extra_conf:
        # if extra_conf parameters are not defined in extra_conf_options, strip them.
        extra_conf = {k: v for k, v in extra_conf.items() if k in extra_conf_options}

    match return_backend:
        case 'pyarrow' as p:
            try:
                import pyarrow as arrow
            except ImportError as e:
                raise ImportError(
                    f'Return backend {p!r} needs the package \'pyarrow\','
                    f' you can fix this with `pip install pyarrow`') from e
        case 'arro3' as p:
            try:
                import arro3 as arrow
            except ImportError as e:
                raise ImportError(
                    f'Return backend {p!r} needs the package \'arro3-core\','
                    f' you can fix this with `pip install arro3-core`') from e
        case 'nanoarrow' as p:
            try:
                import nanoarrow as arrow
            except ImportError as e:
                raise ImportError(
                    f'Return backend {p!r} needs the package \'nanoarrow\','
                    f' you can fix this with `pip install nanoarrow`') from e
        case _:
            # This is reachable.
            raise ValueError(f'Backend {return_backend!r} is not supported.')

    return _read_sql(
        conn,
        query=query,
        partition_on=partition_on,
        partition_range=partition_range,
        partition_num=partition_num,
        return_backend=return_backend,
        **extra_conf
    )
