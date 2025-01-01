import io
import polars as pl
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# Map API data types to polars types
SODA_TO_POLARS = {
    "text": pl.Utf8,
    "number": pl.Float64,  # Use Float64 to handle both integers and floats
    "calendar_date": pl.Datetime("us"),
    "floating_timestamp": pl.Datetime("us"),  # Microsecond precision
}


# Loads the schema (i.e., types) of our data set
@data_loader
def load_data_schema_from_api(*args, **kwargs):
    """
    Load the schema of a dataset from the Socrata API.

    This function retrieves the schema of a specified dataset by making a request
    to the Socrata API's metadata endpoint. It processes the metadata to create 
    a dictionary mapping column names to their corresponding Polars-compatible data types.
    Columns that start with the pattern ":@computed_" are excluded from the schema.

    Returns:
        schema (dict): a dictionary where keys are column names (str) and values are 
                       Polars data types (e.g., pl.Utf8, pl.Float64, pl.Datetime).

    Notes:
        - The `SODA_TO_POLARS` dictionary maps Socrata types to Polars-compatible types.
        - Columns with names starting with ":@computed_" are excluded to filter out 
          metadata or non-data columns.
    """
    url = 'https://{DOMAIN}/api/views/{DATASET_ID}'.format(**kwargs) # Data URL, DOMAIN and DATASET_ID are global variables in kwargs
    response = requests.get(url)

    metadata = response.json()
    columns = metadata.get("columns", [])
    schema = {
        col["fieldName"]: SODA_TO_POLARS.get(col["dataTypeName"], pl.Utf8)
        for col in columns
        if not col["fieldName"].startswith(":@computed_") # Filters out columns that are not ins the data
        }
    
    return schema


@test
def test_output(dictionary, *args) -> None:
    """
    Validate the output of the get_schema block.
    """
    assert dictionary is not None, "The output is undefined"
    assert isinstance(dictionary, dict), "The output is not a dictionary"
    assert len(dictionary) > 0, "The dictionary is empty"
