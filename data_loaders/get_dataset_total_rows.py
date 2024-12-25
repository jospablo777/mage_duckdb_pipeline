import io
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def get_total_of_rows(*args, **kwargs):
    """
    Download the total of rows of the data set
    """
    url = f'https://{kwargs['DOMAIN']}/api/views/{kwargs['DATASET_ID']}'
    response = requests.get(url)
    metadata = response.json()

    # Extract the total row count from the first column
    total_rows = int(metadata['columns'][0]['cachedContents']['count'])

    return total_rows


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, int), 'The output is not an int'
