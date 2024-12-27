import duckdb
import polars as pl
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def insert_data_in_table(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    # Connect to DuckDB database
    conn = duckdb.connect("data/iowa_liquor.duckdb")

    # Try to insert the data into the table
    try:
        conn.register("data", data)
        conn.execute("INSERT INTO iowa_liquor_sales SELECT * FROM data")
    except duckdb.ConstraintException as e:
        print(e)

        # Retrieve existing keys only when needed
        existing_keys_df = conn.execute("SELECT invoice_line_no FROM iowa_liquor_sales").fetchdf()
        existing_keys_series = pl.DataFrame(existing_keys_df)["invoice_line_no"]

        ## Use Polars' vectorized filtering to exclude duplicates
        filtered_data = data.filter(~data["invoice_line_no"].is_in(existing_keys_series))

        # Insert only non-duplicate records into DuckDB
        if filtered_data.height > 0:
            conn.register("filtered_data", filtered_data)  # Register the filtered DataFrame
            conn.execute("INSERT INTO iowa_liquor_sales SELECT * FROM filtered_data")
        else:
            print("No new records to insert.")

    # Explicitly close the connection
    conn.close()
