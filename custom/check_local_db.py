import os
import duckdb
if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

db_path = 'data/iowa_liquor.duckdb'

@custom
def check_last_year(*args, **kwargs):
    """
    Rethrieves the latest year in our local DuckDB database. If the table does not exist, it returns 0.

    Returns:
        last_year (int): year of the latest record in our local database.
    """
    conn = duckdb.connect("data/iowa_liquor.duckdb")
    
    try:
        result = conn.execute("""
        SELECT EXTRACT(YEAR FROM MAX(date)) FROM iowa_liquor_sales
        """).fetchall()
        last_year = result[0][0]

    except duckdb.CatalogException as e:
        print("The table doesn't exist; assigning rows_in_db=0")
        last_year = 0

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        last_year = 0

    # Close DB connections
    conn.close()

    return last_year


 
@test
def test_output(output, *args) -> None:
    """
    Test if there is an output and verifies its type.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, int), 'The output is not int'
