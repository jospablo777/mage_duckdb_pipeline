import duckdb
import pandas as pd
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_duckdb(*args, **kwargs):
    """
    Read DuckDB data into a pandas data frame.
    """
    db_path = "data/iowa_liquor.duckdb"
    query = """
    SELECT 
        invoice_line_no, 
        date, 
        pack, 
        bottle_volume_ml, 
        state_bottle_cost, 
        state_bottle_retail, 
        sale_bottles, 
        sale_dollars 
    FROM iowa_liquor_sales
    """
    # Connect to DuckDB and fetch the data
    with duckdb.connect(db_path) as conn:
        df = conn.execute(query).fetch_df()
        conn.close()

    return df


@test
def test_output(output, *args) -> None:
    """
    Check if the pandas dataframe exist and that it has content.
    """
    assert output is not None, 'The output is undefined'
    assert len(output.index) > 0, 'The data frame is empty'
