import io
import polars as pl
import requests
if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


SOCROTA_TO_POLARS = {
    "text": pl.Utf8,
    "number": pl.Float64,  # Use Float64 to handle both integers and floats
    "calendar_date": pl.Datetime("us"),
    "floating_timestamp": pl.Datetime("us"),  # Microsecond precision
}


# Metadata url
domain = "data.iowa.gov" # Iowa Gov
dataset_id = "m3tr-qhgy" # Liquor data set
data_url = f"https://{domain}/api/views/{dataset_id}"

# Loads the schema of the
@custom
def load_data_schema_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url = data_url
    response = requests.get(url)

    metadata = response.json()
    columns = metadata.get("columns", [])
    schema = {
        col["fieldName"]: SOCROTA_TO_POLARS.get(col["dataTypeName"], pl.Utf8)
        for col in columns
        }


    return schema

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, "The output is undefined"
    assert isinstance(output, dict), "The output is not a dictionary"
    assert len(output) > 0, "The dictionary is empty"
