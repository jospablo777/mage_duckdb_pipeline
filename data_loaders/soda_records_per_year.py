import io
import polars as pl
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Rethrieves the number of records per year in the SODA database. 

    Returns:
        data (pl.DataDrame): data frame with the years present in the SODA database, and the number of records in each year.
    """
    # SoQL to get the the number of invoices per year
    data_url = "https://data.iowa.gov/resource/m3tr-qhgy.csv?$select=date_extract_y(date) AS year, count(invoice_line_no) AS rows&$group=date_extract_y(date)"
    print("Fetching the records-per-year metadata. This might take a couple minutes..")
    response = requests.get(data_url)
    print("Done! We have our year record metadata.\n")
    data = pl.read_csv(io.StringIO(response.text))
    
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, pl.DataFrame), 'The output is not a Polars DataFrame'
