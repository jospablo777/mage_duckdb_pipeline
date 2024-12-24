import io
import polars as pl
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(dictionary, *args, **kwargs):
    """
    Template for loading data from API
    """
    schema = dictionary  # Access the get_schema_from_metadata output

    # Example: Using schema to read data
    data_url = "https://data.iowa.gov/resource/m3tr-qhgy.csv?$limit=1000"
    response = requests.get(data_url)
    data = pl.read_csv(io.StringIO(response.text), schema=schema)

    return data  # You can pass this DataFrame to subsequent blocks


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'