from conecta import sql_bind


def test_sql_replace_values():
    result = sql_bind('SELECT * FROM products WHERE id = :val',
                      {'val': '10; DROP members--'})
    assert result == "SELECT * FROM products WHERE id = '10; DROP members--'"

    result = sql_bind(':val, :val1, :val2',
                      {'val': 'val', 'val1': 1, 'val2': None})
    assert result == "'val', 1, NULL"


def test_sql_replace_ident():
    """Test that sql method properly replaces IDENT special function"""
    query = "select IDENT(:one), IDENT(:two) + :val from t"
    result = sql_bind(query,
                      {'one': 'first_column',
                       'two': 'second_column',
                       'val': 'value'})

    assert result == "select \"first_column\", \"second_column\" + 'value' from t"
