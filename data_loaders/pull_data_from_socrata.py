import io
import polars as pl
import requests
from math import ceil
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(schema, total_n_rows, last_record_in_db, *args, **kwargs):
    """
    Pulls the data from the data set endpoint and reads it into a Polars data frame.
    
    Returns:
        polars.DataFrame: a data frame with the content pulled from the Socrata API.    
    
    Args: 
        schema (dict): a dictionary that specifies the type (values) of the columns (keys).
    """
    #schema = dictionary  # Access the get_schema_from_metadata output
    print(kwargs.get('DOMAIN'))
    
    DOMAIN = 'data.iowa.gov' #kwargs.get('DOMAIN'),
    DATASET_ID = 'm3tr-qhgy' #kwargs.get('DATASET_ID'),
    BATCH_SIZE = 1000
    OFFSET = last_record_in_db

    reccords_left = total_n_rows - last_record_in_db
    n_iterations = ceil(reccords_left/BATCH_SIZE)
    

    # Example: Using schema to read data
    data_url =  f"https://{DOMAIN}/resource/{DATASET_ID}.csv?$limit={BATCH_SIZE}&$offset={OFFSET}&$order=invoice_line_no"
    response = requests.get(data_url)
    data = pl.read_csv(io.StringIO(response.text), schema=schema)

    return data  # You can pass this DataFrame to subsequent blocks


@test
def test_output(output, *args) -> None:
    """
    Validates the output of data pulling block.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, pl.DataFrame), "The output is not a a Polars data frame"
    assert len(output) > 0, "The data frame is empty"