import io
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def get_total_of_rows(*args, **kwargs):
    """
    Gets the total of rows of the data set in the SODA data set.

    Returns:
        total_rows (int): total of rows in the data set.
    """
    url = 'https://{DOMAIN}/api/views/{DATASET_ID}'.format(**kwargs) # Data URL, DOMAIN and DATASET_ID are global variables in kwargs
    response = requests.get(url)
    metadata = response.json()

    # Extract the total row count from the first column
    total_rows = int(metadata['columns'][0]['cachedContents']['count'])

    return total_rows


@test
def test_output(output, *args) -> None:
    """
    Test if there is an output and verifies its type.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, int), 'The output is not an int'
