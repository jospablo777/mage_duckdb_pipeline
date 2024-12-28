import os
import duckdb

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

create_table_query = """
    CREATE TABLE IF NOT EXISTS iowa_liquor_sales (
        invoice_line_no TEXT PRIMARY KEY NOT NULL,
        date TIMESTAMP NOT NULL,
        store TEXT,
        name TEXT,
        address TEXT,
        city TEXT,
        zipcode TEXT,
        store_location TEXT,
        county_number TEXT,
        county TEXT,
        category TEXT,
        category_name TEXT,
        vendor_no TEXT,
        vendor_name TEXT,
        itemno TEXT,
        im_desc TEXT,
        pack REAL,
        bottle_volume_ml REAL,
        state_bottle_cost REAL,
        state_bottle_retail REAL,
        sale_bottles REAL,
        sale_dollars REAL,
        sale_liters REAL,
        sale_gallons REAL,
        liquor_type TEXT,
        is_premium BOOLEAN,
        bottle_size TEXT,
        gov_profit_margin REAL,
        gov_retail_markup_percentage REAL,
        price_per_liter REAL,
        price_per_gallon REAL,
        total_volume_ordered_L REAL,
        volume_to_revenue_ratio REAL,
        week_day INTEGER,
        is_weekend BOOLEAN,
        quarter INTEGER
);
"""


@data_exporter
def export_data(data, *args, **kwargs):
    """
    Creates a DuckDB database if it doesnt exists. It also create the table iowa_liquor_sales if it doesn't exist.

    Args:
        data (pl.DataFrame): The output from the upstream parent block

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    # Create a connection to a file called 'data/iowa_liquor.duckdb'
    # If the file doesn't exist it should create it
    conn = duckdb.connect("data/iowa_liquor.duckdb")
    # Create a table and load data into it
    conn.sql(create_table_query)
    # Explicitly close the connection
    conn.close()
    
    # Pass the unmodified data
    return data


@test
def test_output(output, *args) -> None:
    """
    Test the output exists.
    """
    assert output is not None, 'The output is undefined'

@test
def db_exist(*args) -> None:
    """
    Test there is a .duckdb file.
    """
    assert os.path.exists("data/iowa_liquor.duckdb"), "The database file doesnt exist"