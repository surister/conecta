# IDEs like Pycharm will not detect 'sum_as_string', additionally, we wrap it around
# a dummy `sum_as_string` to be able to add docstring and typehints.
import dataclasses
import json
from typing import Literal, Optional, LiteralString

from .conecta import sum_as_string as _sum_as_string
from .conecta import create_partition_plan as _create_partition_plan
from .conecta import read_sql as _read_sql


def set_debug_log(mode=Literal['performance', 'lib', 'all']) -> None:
    """
     Sets the debugging log configuration of conecta. It configures the `RUST_LOG` environment
     variable accordingly.

    Args:
        mode: The mode that will be used.

    Modes:

    * performance: Logs only show the conecta::perf_logger logger, helpful to debug
     ram usage and timings of data load.
    * lib: Logs everything that happens in conecta_core and conecta_python,
     includes `conecta_performance`, helpful to debug the library, includes `partition plan`.
    * all: Logs everything, other crates like sql parsing, http...

    Returns:

    """
    import os

    rust_log = ""

    match mode:
        case "performance":
            rust_log = 'conecta_core::perf_logger=debug'
        case "lib":
            rust_log = 'conecta=debug,conecta_core=debug'
        case "all":
            rust_log = 'debug'
        case _:
            raise ValueError(f"mode {mode} does not exist")

    os.environ['RUST_LOG'] = rust_log


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
         [todo add link of rust ENUM]
        ...
    """
    queries: list[str]
    partition_on: Optional[str]
    partition_num: Optional[int]
    partition_range: tuple[int]
    needed_metadata_from_source: str
    query_partition_mode: str


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
    count: int
    metadata_query: str
    query_data: list[str]
    partition_config: PartitionConfig

    @classmethod
    def from_dict(cls, d: dict):
        """
        Create a ``PartitionPlan`` from a dictionary. It is expected that all keys
        match ``PartitionPlan``'s attributes.
        """
        partition_config = d.pop('partition_config')
        return cls(**d, partition_config=PartitionConfig(**partition_config))


def sum_as_string(a: int, b: int) -> str:
    """
    Python docstring go here.

    :param a:
    :param b:
    :return:
    """
    return _sum_as_string(a, b)


def create_partition_plan(
        conn: str,
        queries: list[str],
        partition_on: Optional[str] = None,
        partition_range: tuple = None,
        partition_num: int = None
):
    plan = json.loads(
        _create_partition_plan(conn, queries, partition_on, partition_range, partition_num)
    )
    return PartitionPlan.from_dict(plan)


def read_sql(
        conn: str,
        queries: list[str],
        partition_on: Optional[str] = None,
        partition_range: tuple = None,
        partition_num: int = None,
        return_backend: Literal['pyarrow', 'arro3', 'nanoarrow'] = 'pyarrow',
        extra_conf: dict = None
):
    extra_conf_options = {"max_pool_size", "disable_preallocation"}

    default_conf = {
        'max_pool_size': None,
        'disable_preallocation': False
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
                    f' you can fix this with `pip install pyarrow`') from e
        case 'nanoarrow' as p:
            try:
                import pyarrow as arrow
            except ImportError as e:
                raise ImportError(
                    f'Return backend {p!r} needs the package \'nanoarrow\','
                    f' you can fix this with `pip install pyarrow`') from e
        case _:
            raise ValueError(f'Return backend not supported.')

    return _read_sql(conn,
                     queries=queries,
                     partition_on=partition_on,
                     partition_range=partition_range,
                     partition_num=partition_num,
                     return_backend=return_backend,
                     **extra_conf
                     )

# Todo: Add support for detecting bad arguments like 'return_type'
# (which is connectorx API), and recommend the new name.
# def read_sql(
#         conn: str,
#         query: str,
#         *,
#         pre_query: Optional["str"] = None,
#         post_query: Optional["str"] = None,
#         df_return_type: Literal["arrow", "pandas", "polars"] = "arrow",
#         protocol: str = "default",  # options depends on source.
#         # Todo: Can we give the user the option to pass a method, and we run that method to calculate the partition number?
#         # Default is 'do nothing automatically', let the user input.
#         partition_strategy: Literal["default", "high", "low"] = "default",
#         partition_column,
#         partition_range: Optional[tuple[int, int]],
#         partition_n: Optional[int],
# ):
#     pass
