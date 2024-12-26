# Check: https://duckdb.org/docs/api/python/overview.html#:~:text=Persistent%20Storage,and%20from%20other%20DuckDB%20clients.
# https://pro.mage.ai/blog/duckdb-with-mage-intro

import duckdb

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    # Create a connection to a file called 'data/iowa_liquor.duckdb'
    # If the file doesn't exist it should create it
    con = duckdb.connect("data/iowa_liquor.duckdb")
    # Create a table and load data into it
    con.sql("CREATE TABLE IF NOT EXISTS iowa_liquor_sales AS SELECT * FROM data")
    #con.sql("INSERT INTO test VALUES (42)")
    # query the table
    con.sql("SELECT date, liquor_type, is_premium, sale_bottles, sale_dollars FROM iowa_liquor_sales LIMIT 20").show()
    # explicitly close the connection
    con.close()


