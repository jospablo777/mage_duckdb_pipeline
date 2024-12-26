# Check: https://duckdb.org/docs/api/python/overview.html#:~:text=Persistent%20Storage,and%20from%20other%20DuckDB%20clients.
# https://pro.mage.ai/blog/duckdb-with-mage-intro

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
def export_data(data, schema, *args, **kwargs):
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
    conn = duckdb.connect("data/iowa_liquor.duckdb")
    # Create a table and load data into it
    conn.sql(create_table_query)

    # Insert Polars DataFrame into DuckDB
    conn.execute("INSERT INTO iowa_liquor_sales SELECT * FROM data")

    #con.sql("INSERT INTO test VALUES (42)")
    # query the table
    conn.sql("SELECT date, liquor_type, is_premium, sale_bottles, sale_dollars FROM iowa_liquor_sales LIMIT 20").show()
    # explicitly close the connection
    conn.close()



