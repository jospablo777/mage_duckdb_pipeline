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
    Rethrieves the number of records in our local DuckDB database.

    Returns:
        rows_in_db (int): the number of records in our local database.
    """
    conn = duckdb.connect("data/iowa_liquor.duckdb")
    
    try:
        result = conn.execute("""
        SELECT COUNT(invoice_line_no) FROM iowa_liquor_sales
        """).fetchall()
        
        rows_in_db = result[0][0]

    except duckdb.CatalogException as e:
        print("The table doesn't exist; assigning offset=0")

        rows_in_db = 0

    # Close connection    
    conn.close()

    return rows_in_db

 
@test
def test_output(output, *args) -> None:
    """
    Test if there is an output and verifies its type.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, int), 'The output is not int'
