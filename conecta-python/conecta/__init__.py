# IDEs like Pycharm will not detect 'sum_as_string', aditionally we wrapp it around
# a dummy `sum_as_string` to be able to add docstring and typehints.
from .conecta import sum_as_string as _sum_as_string

def sum_as_string(a: int, b: int) -> str:
    """
    Python docstring go here.

    :param a:
    :param b:
    :return:
    """
    return _sum_as_string(a, b)

