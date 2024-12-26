import duckdb
if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    conn = duckdb.connect("data/iowa_liquor.duckdb")
    
    result = conn.execute("""
    SELECT COUNT(invoice_line_no) FROM iowa_liquor_sales
    """).fetchall()
    
    rows_in_db = result[0][0]

    print(result[0][0])

    return rows_in_db


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
