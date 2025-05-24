# IDEs like Pycharm will not detect 'sum_as_string', additionally, we wrapp it around
# a dummy `sum_as_string` to be able to add docstring and typehints.
from typing import Literal, Optional

from .conecta import sum_as_string as _sum_as_string


def sum_as_string(a: int, b: int) -> str:
    """
    Python docstring go here.

    :param a:
    :param b:
    :return:
    """
    return _sum_as_string(a, b)


# Todo: Add support for detecting bad arguments like 'return_type'
# (which is connectorx API), and recommend the new name.
def read_sql(
    conn: str,
    query: str,
    *,
    pre_query: Optional["str"] = None,
    post_query: Optional["str"] = None,
    df_return_type: Literal["arrow", "pandas", "polars"] = "arrow",
    protocol: str = "default",  # options depends on source.
    # Todo: Can we give the user the option to pass a method, and we run that method to calculate the partition number?
    # Default is 'do nothing automatically', let the user input.
    partition_strategy: Literal["default", "high", "low"] = "default",
    partition_column,
    partition_range: Optional[tuple[int, int]],
    partition_n: Optional[int],
):
    pass
