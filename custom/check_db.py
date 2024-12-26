import os
import duckdb
if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

db_path = 'data/iowa_liquor.duckdb'

@custom
def check_last_record(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    if os.path.exists(db_path):
        conn = duckdb.connect("data/iowa_liquor.duckdb")
    
        result = conn.execute("""
        SELECT COUNT(invoice_line_no) FROM iowa_liquor_sales
        """).fetchall()
        
        rows_in_db = result[0][0]
    else:
        rows_in_db = 0

    return rows_in_db


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, int), 'The output is not int'
